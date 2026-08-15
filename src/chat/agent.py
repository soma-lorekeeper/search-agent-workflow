"""작가와 대화하는 단일 에이전트.

설정 오류 검사(src/contradiction/agent.py)와 루프 구조는 같지만 목적이 다르다 — 거기서는
정해진 claim 하나를 구조화된 verdict로 판정하고, 여기서는 작가가 무엇을 물을지 알 수 없는
자유 대화를 근거 있는 자연어로 답한다. 그래서 응답을 JSON으로 강제하지 않고, 도구 호출을
tool_choice로 강제하지도 않는다(작품과 무관한 잡담에까지 KG를 뒤지게 만들면 느리고 이상하다).

대신 UI에 "무엇을 찾아봤는지"를 보여줘야 해서, 호출된 도구를 성공/실패까지 포함해 기록한다.

[루프를 LangGraph 그래프로 옮긴 이유]
예전에는 이 파일이 `for turn in range(MAX_TURNS)` 손수 루프로 "모델 호출 → 도구 실행 → 다시
모델 호출"을 돌렸다. 지금은 같은 흐름을 LangGraph의 StateGraph(model 노드 · tools 노드 ·
조건부 엣지)로 표현한다. 앞으로 붙일 기능이 전부 **분기**이기 때문이다 — 원고 수정 도구는
"모델이 고른 수정안을 작가가 승인해야 실행되는" 중단 지점이 필요하고(human-in-the-loop),
집필 보조 흐름은 조회와 다른 노드를 타야 한다. 손수 루프에서는 그런 분기가 if문 더미로
쌓이지만, 그래프에서는 노드와 엣지가 하나 늘어날 뿐이다.

**모델 호출은 그대로 OpenAI SDK로 한다(langchain-openai의 ChatOpenAI를 쓰지 않는다).**
이 에이전트의 핵심 통제 셋 — 예산이 떨어지면 tools 파라미터 자체를 빼는 것, prompt_cache_key로
프롬프트 캐시를 묶는 것, parallel_tool_calls를 끄는 것 — 은 전부 Chat Completions 요청 본문을
우리가 직접 조립해야 확실하다. 추상화 한 겹을 더 끼우면 "정말 tools가 빠졌는가"를 코드로
증명하기 어려워진다. LangGraph는 오케스트레이션(무엇 다음에 무엇)을 맡고, 모델 호출의 세부는
계속 우리가 쥔다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import operator
import random
from dataclasses import dataclass
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime
from openai import AsyncOpenAI, RateLimitError

from src.chat.indexed import fetch_indexed_episodes
from src.chat.prompts import (
    CHAT_CACHE_KEY,
    CHAT_SYSTEM_PROMPT,
    TITLE_CACHE_KEY,
    TITLE_SYSTEM_PROMPT,
)
from src.chat.tools import TOOL_GUIDE, ChatTool, build_chat_tools
from src.config import OPENAI_API_KEY, OPENAI_MODEL

logger = logging.getLogger("chat.agent")

MAX_TOOL_CALLS = 5
MAX_TURNS = MAX_TOOL_CALLS + 2  # 도구 호출 예산 + 최종 답변 여유 턴. 무한루프 방지용 안전판.
RATE_LIMIT_MAX_RETRIES = 5  # 조직 TPM 한도(429)에 걸렸을 때 재시도 횟수

# 이 길이를 넘으면 "첫 턴"이 아니다 — 제목은 대화가 시작될 때 한 번만 지으면 되고,
# 매 턴 다시 지으면 사이드바 제목이 계속 바뀌어 작가가 대화를 못 찾는다.
FIRST_TURN_MESSAGE_COUNT = 2
TITLE_MAX_CHARS = 20

# 예산을 다 썼거나 없는 도구를 부른 호출에 돌려주는 도구 결과. 호출을 그냥 무시하면 모델은
# tool_call_id에 대한 답이 없다며 다음 요청에서 400을 맞는다 — 반드시 뭐라도 돌려줘야 한다.
BUDGET_EXHAUSTED_TOOL_RESULT = (
    "(사용할 수 없는 도구이거나 호출 예산을 모두 사용했습니다. 지금까지 확인한 내용으로 답하세요.)"
)

_client = AsyncOpenAI(api_key=OPENAI_API_KEY)


async def _create_with_retry(**kwargs: Any) -> Any:
    """RateLimitError(429)면 지수 백오프로 재시도한다.

    채팅은 사람이 기다리는 요청이라 그냥 실패시키면 그대로 에러 말풍선이 뜬다. 몇 초 늦더라도
    답이 오는 편이 낫다(같은 서버에서 설정 오류 검사가 claim별로 병렬로 돌고 있으면 조직
    TPM 한도에 순간적으로 몰리기 쉽다).
    """
    for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
        try:
            return await _client.chat.completions.create(**kwargs)
        except RateLimitError:
            if attempt == RATE_LIMIT_MAX_RETRIES:
                raise
            delay = min(2**attempt, 30) + random.uniform(0, 1)
            logger.warning(
                "rate limit(429) — %.1f초 후 재시도 (%d/%d)",
                delay,
                attempt + 1,
                RATE_LIMIT_MAX_RETRIES,
            )
            await asyncio.sleep(delay)


def _format_indexed(indexed: list[int]) -> str:
    """KG 조회가 가능한 회차 범위를 모델이 오해할 수 없게 적는다.

    번호를 그냥 나열하는 대신 "이 밖은 조회 불가"를 매번 못 박는다 — 모델은 목록만 주면
    그걸 상한이 아니라 예시로 읽고 목록 밖 회차를 자신 있게 지어낸다.
    """
    if not indexed:
        return (
            "인덱싱이 끝난 회차가 **하나도 없다**. 작품 내용에 대한 KG 조회는 지금 전부 빈 "
            "결과가 나온다. 회차 내용을 물으면 '아직 인덱싱된 회차가 없어 확인할 수 없다'고 "
            "말하고, 도구로 억지로 찾으려 하지 마라."
        )

    listed = ", ".join(f"{n}화" for n in indexed)
    missing = [n for n in range(indexed[0], indexed[-1] + 1) if n not in set(indexed)]
    gap = (
        f" 중간의 {', '.join(f'{n}화' for n in missing)}는 인덱싱되지 않아 조회할 수 없다."
        if missing
        else ""
    )
    return (
        f"KG에서 조회할 수 있는 회차는 {listed}뿐이다(총 {len(indexed)}개 회차)."
        f"{gap} 이 목록 밖의 회차는 **어떤 도구로도 조회할 수 없고**, 조회하면 빈 결과가 "
        f"돌아온다. 목록 밖 회차를 물으면 아직 인덱싱되지 않았다고 분명히 답해라."
    )


def _format_editing(editing: dict | None) -> str:
    """집필 중인 회차 — 번호·제목과 함께 원고 **전문**을 그대로 싣는다.

    전문을 넣는 건 작가의 명시적 요구다(발췌가 아니라 전체). 그 대가로 매 턴 큰 프롬프트가
    나가지만, 지금 고쳐 쓰는 회차만큼은 모델이 도구를 거치지 않고 통째로 읽는 편이
    대화 품질이 확실히 낫다. 비용이 문제가 되면 여기서 발췌로 줄이면 된다 — 그때
    프롬프트의 "이미 들어 있다" 규칙도 함께 고쳐야 한다.
    """
    if not editing:
        return (
            "지금 집필 중인 회차를 알 수 없다. '이번 화', '직전 화'처럼 기준이 필요한 표현이 "
            "나오면 지어내지 말고 어느 회차인지 작가에게 되물어라."
        )

    number = editing.get("number")
    title = (editing.get("title") or "").strip() or "(제목 없음)"
    text = editing.get("text") or ""

    if isinstance(number, int):
        head = (
            f"작가는 지금 {number}화 「{title}」를 집필 중이다. '이번 화'는 {number}화, "
            f"'직전 화'는 {number - 1}화를 뜻한다."
        )
    else:
        # DRAFT는 화수가 아직 배정되지 않는다(Episode 도메인 규칙). 번호가 없는 걸 모델이
        # "0화"나 "1화"로 메우지 않게 상태를 그대로 설명한다.
        head = (
            f"작가는 지금 화수가 아직 배정되지 않은 새 회차 「{title}」를 집필 중이다. "
            f"'이번 화'는 이 회차를 뜻하고, 확정된 화수는 아직 없다."
        )

    if not text.strip():
        return f"{head}\n아직 본문이 비어 있다(작성 전)."

    return (
        f"{head}\n이 회차의 원고 **전문**이 아래에 그대로 들어 있다. 이 회차에 대해서는 "
        f"episode_manuscript를 부르지 말고 아래 본문을 읽어라.\n"
        f"--- 집필 중인 원고 시작 ({len(text)}자) ---\n{text}\n--- 집필 중인 원고 끝 ---"
    )


def _format_viewing(number: int | None, editing: dict | None) -> str:
    """보고 있는 회차 — 질문의 주어가 아니라 "무엇을 묻는 중인지"의 힌트다."""
    if not isinstance(number, int):
        return "작가가 지금 어느 회차를 열어 두고 있는지는 알 수 없다."

    editing_number = (editing or {}).get("number")
    same = " (집필 중인 회차와 같다.)" if editing_number == number else ""
    return (
        f"작가는 지금 {number}화를 화면에 열어 두고 있다.{same} 이건 질문의 배경일 뿐이니 "
        f"모든 질문을 {number}화 이야기로 넘겨짚지 마라. 회차를 말하지 않고 '여기', '이 장면'"
        f"처럼 가리킬 때만 {number}화로 해석해라."
    )


def _system_prompt(context: dict | None, indexed_episodes: list[int]) -> str:
    """세 가지 회차 개념을 각각 다른 자리에 적어 넣는다.

    셋은 실제로 다르다 — 인덱싱된 회차는 "조회 가능한 범위", 집필 중인 회차는 "지금 쓰는 글",
    보고 있는 회차는 "지금 화면"이다. 예전처럼 회차 번호 하나(current_episode_number)로
    뭉뚱그리면 모델이 이 셋을 섞어서, 인덱싱도 안 된 회차를 조회했다고 하거나 보고 있는
    회차를 집필 중인 회차로 착각한다.
    """
    ctx = context or {}
    editing = ctx.get("editing_episode")
    # 원고 전문은 작가가 쓴 임의의 텍스트라 "{tool_guide}" 같은 문자열이 들어 있을 수 있다.
    # 그래서 다른 플레이스홀더를 전부 채운 **뒤 마지막에** 끼워 넣는다 — 순서를 바꾸면
    # 원고 속 중괄호 문자열이 다음 replace의 대상이 된다.
    return (
        CHAT_SYSTEM_PROMPT.replace("{indexed_episodes}", _format_indexed(indexed_episodes))
        .replace("{viewing_episode}", _format_viewing(ctx.get("viewing_episode_number"), editing))
        .replace("{tool_guide}", TOOL_GUIDE)
        .replace("{max_tool_calls}", str(MAX_TOOL_CALLS))
        .replace("{editing_episode}", _format_editing(editing))
    )


def _to_openai_messages(messages: list[dict]) -> list[dict]:
    """클라이언트가 보낸 대화 기록을 그대로 신뢰하지 않고 role/content만 추려 넘긴다."""
    return [
        {"role": m["role"] if m.get("role") in ("user", "assistant") else "user",
         "content": str(m.get("content") or "")}
        for m in messages
    ]


async def _suggest_title(user_text: str, answer: str) -> str | None:
    """대화 첫 턴에만 세션 제목을 짓는다. 실패해도 대화 자체는 성공이므로 None으로 삼킨다."""
    try:
        response = await _create_with_retry(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": TITLE_SYSTEM_PROMPT},
                {"role": "user", "content": f"[작가]\n{user_text}\n\n[AI]\n{answer}"},
            ],
            prompt_cache_key=TITLE_CACHE_KEY,
        )
    except Exception as exc:  # noqa: BLE001 — 제목은 부가 기능이라 실패가 답변을 막으면 안 된다
        logger.warning("세션 제목 생성 실패 | %s", exc)
        return None

    title = (response.choices[0].message.content or "").strip().strip("\"'“”「」 .")
    if not title:
        return None
    # 모델이 20자 규칙을 어길 때가 있어 마지막에 우리가 한 번 더 자른다(사이드바가 깨지는 것보다 낫다).
    return title[:TITLE_MAX_CHARS]


# ---------- LangGraph 그래프 ----------
#
# 모양(노드 둘 + 조건부 엣지 둘):
#
#     START ──▶ model ──(도구 호출 없음)──▶ END
#                 │  ▲
#      (도구 호출) │  │ (턴 여유 있음)
#                 ▼  │
#                tools ──(턴 상한 도달)──▶ END
#
# 나중에 붙일 것들의 자리도 이 그림 안에 있다:
#   - human-in-the-loop: model과 tools 사이에 승인 노드를 끼우고 거기서 interrupt()를 건다
#     (원고를 고치는 도구가 생기면 작가 승인 없이 실행되면 안 된다).
#   - 원고 수정(edit-manuscript): model의 조건부 엣지에 "tools" 말고 다른 목적지를 하나 더
#     추가하면 된다. 지금 분기 함수(_route_after_model)가 그 자리다.


class ChatState(TypedDict):
    """그래프 한 번 실행(= 대화 한 턴) 동안의 상태.

    messages는 OpenAI Chat Completions 와이어 포맷 그대로다(dict 목록). LangChain 메시지
    객체로 바꾸지 않는 이유는 위 모듈 docstring과 같다 — 요청 본문을 우리가 직접 조립해야
    "예산이 떨어지면 tools를 아예 안 보낸다" 같은 통제가 코드에 그대로 드러난다.
    """

    # 두 리스트만 누적(append)이고 나머지는 마지막에 쓴 값이 이긴다.
    messages: Annotated[list[dict], operator.add]
    tool_records: Annotated[list[dict], operator.add]
    tool_calls_used: int  # 실제로 실행된 도구 호출 수. MAX_TOOL_CALLS와 비교하는 예산.
    turns: int  # 모델을 부른 횟수. MAX_TURNS와 비교하는 무한루프 안전판(예산과 별개다).
    answer: str | None  # 모델이 도구 없이 답한 최종 텍스트. None이면 아직 안 끝났다.


@dataclass(frozen=True)
class ChatRuntime:
    """그래프가 도는 동안 **서버만 아는** 값. state가 아니라 LangGraph의 런타임 컨텍스트다.

    work_id를 여기 두는 게 핵심이다. 도구 JSON 스키마에 없으니 모델이 채울 수 없고, state의
    messages에도 실리지 않으니 모델이 읽을 수도 없다. 조회 대상 작품은 요청이 정하는 값이지
    모델이 고를 값이 아니다 — 모델이 고를 수 있으면 남의 작품 데이터를 읽는 경로가 열린다.
    서버가 채워 넣는 인자가 앞으로 더 생기면(user_id 등) 전부 여기에 넣는다.
    """

    work_id: int
    tool_schemas: list[dict[str, Any]]
    tools_by_name: dict[str, ChatTool]


async def _call_model(state: ChatState, runtime: Runtime[ChatRuntime]) -> dict:
    """모델을 한 번 부른다. 예산이 남았을 때만 도구 목록을 함께 보낸다."""
    kwargs: dict[str, Any] = {
        "model": OPENAI_MODEL,
        "messages": state["messages"],
        "prompt_cache_key": CHAT_CACHE_KEY,
    }
    if state["tool_calls_used"] < MAX_TOOL_CALLS:
        kwargs["tools"] = runtime.context.tool_schemas
        kwargs["tool_choice"] = "auto"
        # 예산 추적을 단순하게 유지한다. tools 노드가 호출을 하나씩 세면서 실행하므로 병렬로
        # 와도 회계 자체는 정확하지만, 한 턴에 여러 개가 몰리면 "예산 5"가 실제로는 5를 넘겨
        # 실행되는 게 아니라 남는 호출이 통째로 버려진다 — 모델 입장에서 부른 도구가 조용히
        # 사라지는 편이 더 나쁘다. 그래서 애초에 한 번에 하나만 부르게 한다.
        kwargs["parallel_tool_calls"] = False
    # 예산이 0이면 tools/tool_choice를 아예 넣지 않는다. "부르지 마라"는 프롬프트 지시가 아니라
    # **부를 수단 자체를 없애는 것**이다 — 지시는 무시될 수 있지만 없는 도구는 부를 수 없다.

    response = await _create_with_retry(**kwargs)
    message = response.choices[0].message

    if not message.tool_calls:
        return {"turns": state["turns"] + 1, "answer": message.content or ""}

    return {
        "turns": state["turns"] + 1,
        "messages": [
            {
                "role": "assistant",
                "content": message.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in message.tool_calls
                ],
            }
        ],
    }


async def _run_tools(state: ChatState, runtime: Runtime[ChatRuntime]) -> dict:
    """직전 assistant 메시지가 요청한 도구를 순서대로 실행한다.

    LangGraph의 prebuilt ToolNode를 쓰지 않는다. 여기서 하는 세 가지가 prebuilt에 없기 때문이다:
      1. work_id를 실행 시점에 주입한다(모델이 준 인자에는 없다).
      2. 호출 하나하나 예산을 확인하고, 넘으면 실행하지 않고 결과만 돌려준다.
      3. 도구가 반환하는 (결과 텍스트, 화면용 한 줄 요약) 쌍에서 요약만 따로 모은다 —
         UI가 답변 위에 "무엇을 찾아봤는지"를 보여주는 근거다.
    """
    calls = state["messages"][-1]["tool_calls"]
    used = state["tool_calls_used"]
    new_messages: list[dict] = []
    records: list[dict] = []

    for tool_call in calls:
        name = tool_call["function"]["name"]
        arguments = tool_call["function"]["arguments"]
        tool = runtime.context.tools_by_name.get(name)
        if tool is None or used >= MAX_TOOL_CALLS:
            new_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": BUDGET_EXHAUSTED_TOOL_RESULT,
                }
            )
            continue

        try:
            args = json.loads(arguments or "{}")
            # work_id는 스키마에 없어 모델이 채우지 않는다 — 런타임 컨텍스트의 값을 여기서 주입한다.
            # 도구는 Neo4j/PostgreSQL에 동기로 붙으므로 스레드로 뺀다(이벤트 루프를 막지 않는다).
            result_text, summary = await asyncio.to_thread(
                tool.run, runtime.context.work_id, **args
            )
            records.append({"name": name, "summary": summary, "status": "DONE"})
        except Exception as exc:  # noqa: BLE001 — 도구 실패는 "근거 부족"으로 두고 대화를 계속한다
            logger.warning("도구 실행 실패 | tool=%s args=%s | %s", name, arguments, exc)
            # 오류 문자열을 도구 결과로 그대로 돌려준다. 그래야 모델이 "확인하지 못했다"고
            # 말할 수 있다 — 여기서 예외를 올리면 대화 전체가 에러 말풍선이 된다.
            result_text = f"도구 실행 오류: {exc}"
            records.append({"name": name, "summary": f"{name} 조회 실패", "status": "FAILED"})

        used += 1
        new_messages.append(
            {"role": "tool", "tool_call_id": tool_call["id"], "content": result_text}
        )

    return {"messages": new_messages, "tool_records": records, "tool_calls_used": used}


def _route_after_model(state: ChatState) -> str:
    """모델이 도구를 불렀으면 tools로, 아니면 그 답변이 최종이다.

    원고 수정처럼 승인이 필요한 흐름이 생기면 여기서 목적지를 하나 더 나눈다.
    """
    return END if state["answer"] is not None else "tools"


def _route_after_tools(state: ChatState) -> str:
    """도구 실행 뒤 모델에게 돌아간다 — 단, 턴 상한에 닿았으면 멈춘다.

    이 상한은 도구 예산(MAX_TOOL_CALLS)과 다른 목적이다. 예산이 0이 돼도 모델은 계속
    "도구를 부르겠다"는 응답을 낼 수 있어서(그러면 model↔tools를 영원히 왕복한다), 대화가
    몇 바퀴를 돌든 무조건 끊는 별도의 안전판이 필요하다.
    """
    return END if state["turns"] >= MAX_TURNS else "model"


def _build_graph():
    builder = StateGraph(ChatState, context_schema=ChatRuntime)
    builder.add_node("model", _call_model)
    builder.add_node("tools", _run_tools)
    builder.add_edge(START, "model")
    builder.add_conditional_edges("model", _route_after_model, {"tools": "tools", END: END})
    builder.add_conditional_edges("tools", _route_after_tools, {"model": "model", END: END})
    # **체크포인터를 붙이지 않는다.** 대화 기록의 진실의 원천은 API 서버(Spring)의
    # chat_messages 테이블이고, 매 요청에 대화 전체가 실려 온다. 여기에 체크포인터를 두면
    # 같은 대화의 저장소가 둘이 되어 반드시 어긋난다(어느 쪽이 맞는지 아무도 모르게 된다).
    # 다만 나중에 human-in-the-loop interrupt를 붙일 때는 체크포인터가 필요하다 — 중단된
    # 그래프를 재개하려면 상태를 어딘가에 남겨야 하기 때문이다. 그때도 "대화 기록"이 아니라
    # "중단된 실행"만 담는 저장소여야 진실의 원천이 둘로 갈라지지 않는다.
    return builder.compile()


# 그래프 구조는 요청마다 달라지지 않으므로 프로세스당 한 번만 만든다(요청별 값은 전부
# state/context로 넘어간다). 컴파일된 그래프는 상태를 들고 있지 않아 동시 실행해도 안전하다.
_GRAPH = _build_graph()


async def run_chat(
    work_id: int,
    session_id: int,
    messages: list[dict],
    context: dict | None = None,
) -> dict:
    """대화 한 턴을 처리한다.

    실제 진행(모델 호출 ↔ 도구 실행)은 위의 LangGraph 그래프가 맡고, 이 함수는 그 앞뒤 —
    턴마다 새로 읽어야 하는 값(인덱싱된 회차) 준비와, HTTP 응답 계약으로의 변환 — 만 한다.
    호출부(src/webapp.py)가 보는 시그니처와 반환 모양은 그래프 도입 전과 완전히 같다.

    messages는 지금까지의 대화 전체({"role": "user"|"assistant", "content": str} 목록)이며
    마지막 항목이 이번에 답할 사용자 발화다. 세션 상태를 서버에 두지 않고 매번 통째로 받는 이유는,
    대화 기록의 진실의 원천이 API 서버(Spring)의 chat_messages 테이블이기 때문이다.

    context는 회차에 관한 세 개념 중 요청이 알려줄 수 있는 둘을 담는다:
      - editing_episode: {"number": int|None, "title": str, "text": str} — 집필 중인 회차 전문
      - viewing_episode_number: int|None — 화면에 열어 둔 회차
    셋째(인덱싱된 회차)는 요청이 아니라 Neo4j에게 매 턴 직접 묻는다 — 인덱싱은 대화와 무관하게
    진행되므로 요청이 들고 온 값은 이미 낡았을 수 있다.

    반환: {"content": str, "tool_calls": [{"name", "summary", "status"}], "suggested_title": str|None}
    """
    tool_schemas, tools_by_name = build_chat_tools()

    # 요청마다(프로세스마다가 아니라) 새로 읽는다. 인덱싱이 방금 끝난 회차를 "없다"고
    # 답하지 않으려면 캐시하면 안 된다. 드라이버 왕복 한 번이라 한 턴에 한 번은 감당된다.
    indexed_episodes = await asyncio.to_thread(fetch_indexed_episodes, work_id)

    initial_state: ChatState = {
        # 시스템 프롬프트 + 지금까지의 대화. 도구 호출/결과는 그래프가 여기에 이어 붙인다.
        "messages": [
            {"role": "system", "content": _system_prompt(context, indexed_episodes)},
            *_to_openai_messages(messages),
        ],
        "tool_records": [],
        "tool_calls_used": 0,
        "turns": 0,
        "answer": None,
    }

    final_state = await _GRAPH.ainvoke(
        initial_state,
        context=ChatRuntime(
            work_id=work_id, tool_schemas=tool_schemas, tools_by_name=tools_by_name
        ),
        # 턴 상한(_route_after_tools)이 먼저 걸리므로 여기 닿을 일은 없다. 엣지를 잘못 이어
        # 놓았을 때 무한히 도는 대신 예외로 터지게 하는 그래프 차원의 마지막 보호선이다.
        config={"recursion_limit": MAX_TURNS * 2 + 1},
    )

    answer: str | None = final_state["answer"]
    tool_records: list[dict] = final_state["tool_records"]
    tool_calls_used: int = final_state["tool_calls_used"]

    if answer is None:
        logger.warning("채팅 턴 상한 도달 | work_id=%s session_id=%s", work_id, session_id)
        answer = "자료를 찾는 데 시간이 너무 오래 걸렸습니다. 질문을 조금 좁혀서 다시 물어봐 주세요."

    logger.info(
        "채팅 응답 완료 | work_id=%s session_id=%s tool_calls=%d",
        work_id,
        session_id,
        tool_calls_used,
    )

    suggested_title = None
    if len(messages) <= FIRST_TURN_MESSAGE_COUNT:
        first_user = next((m.get("content", "") for m in messages if m.get("role") == "user"), "")
        suggested_title = await _suggest_title(first_user, answer)

    return {
        "content": answer,
        "tool_calls": tool_records,
        "suggested_title": suggested_title,
    }
