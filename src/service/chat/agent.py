"""작가와 대화하는 단일 에이전트.

설정 오류 검사(src/service/detect)와 목적이 다르다 — 거기서는
정해진 claim 하나를 구조화된 verdict로 판정하고, 여기서는 작가가 무엇을 물을지 알 수 없는
자유 대화를 근거 있는 자연어로 답한다. 그래서 응답을 JSON으로 강제하지 않고, 도구 호출을
tool_choice로 강제하지도 않는다(작품과 무관한 잡담에까지 KG를 뒤지게 만들면 느리고 이상하다).

대신 UI에 "무엇을 찾아봤는지"를 보여줘야 해서, 호출된 도구를 성공/실패까지 포함해 기록한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

# 이름을 이 모듈에 바인딩해 부른다(`openai_client.create_completion(...)`처럼 정규화해
# 부르지 않는다). 이 레포의 테스트는 정의처가 아니라 **소비 모듈**의 속성을 스텁으로
# 갈아끼우는 방식이라, 그래야 다른 서비스와 같은 방법으로 LLM을 막을 수 있다.
from src.common.openai_client import create_completion
from src.service.chat.indexed import fetch_indexed_episodes
from src.service.chat.prompts import (
    CHAT_CACHE_KEY,
    CHAT_SYSTEM_PROMPT,
    TITLE_CACHE_KEY,
    TITLE_SYSTEM_PROMPT,
)
from src.service.chat.tools import TOOL_GUIDE, build_chat_tools
from src.config import OPENAI_MODEL

logger = logging.getLogger("chat.agent")

MAX_TOOL_CALLS = 5
MAX_TURNS = MAX_TOOL_CALLS + 2  # 도구 호출 예산 + 최종 답변 여유 턴. 무한루프 방지용 안전판.

# 이 길이를 넘으면 "첫 턴"이 아니다 — 제목은 대화가 시작될 때 한 번만 지으면 되고,
# 매 턴 다시 지으면 사이드바 제목이 계속 바뀌어 작가가 대화를 못 찾는다.
FIRST_TURN_MESSAGE_COUNT = 2
TITLE_MAX_CHARS = 20

# 429 재시도는 관문(src/common/openai_client.py)이 한다. 예전에는 여기에 같은 로직이
# 한 벌 더 있었는데, 두 벌이면 정책이 조용히 갈라진다(한쪽만 고치면 다른 쪽은 그대로).
# 흡수하면서 동작이 두 가지 바뀐다:
#   - 재시도 횟수 6회 → 5회(관문의 _MAX_ATTEMPTS)
#   - 크레딧 소진(insufficient_quota)이면 즉시 실패한다. 예전에는 기다려도 안 풀리는
#     429를 붙들고 6번 헛돌았다 — 그동안 작가는 빈 화면을 본다.


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
        response = await create_completion(
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


async def run_chat(
    user_id: int,
    work_id: int,
    session_id: int,
    messages: list[dict],
    context: dict | None = None,
) -> dict:
    """대화 한 턴을 처리한다.

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
    indexed_episodes = await asyncio.to_thread(fetch_indexed_episodes, user_id, work_id)

    convo: list[dict] = [
        {"role": "system", "content": _system_prompt(context, indexed_episodes)},
        *_to_openai_messages(messages),
    ]

    tool_records: list[dict] = []
    tool_calls_used = 0
    answer: str | None = None

    for _turn in range(MAX_TURNS):
        kwargs: dict[str, Any] = {
            "model": OPENAI_MODEL,
            "messages": convo,
            "prompt_cache_key": CHAT_CACHE_KEY,
        }
        if tool_calls_used < MAX_TOOL_CALLS:
            kwargs["tools"] = tool_schemas
            kwargs["tool_choice"] = "auto"
            kwargs["parallel_tool_calls"] = False  # 예산 추적을 단순하게 유지

        response = await create_completion(**kwargs)
        message = response.choices[0].message

        if not message.tool_calls:
            answer = message.content or ""
            break

        convo.append(
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
        )

        for tool_call in message.tool_calls:
            name = tool_call.function.name
            tool = tools_by_name.get(name)
            if tool is None or tool_calls_used >= MAX_TOOL_CALLS:
                convo.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": (
                            "(사용할 수 없는 도구이거나 호출 예산을 모두 사용했습니다. "
                            "지금까지 확인한 내용으로 답하세요.)"
                        ),
                    }
                )
                continue

            try:
                args = json.loads(tool_call.function.arguments or "{}")
                # user_id·work_id는 스키마에 없어 모델이 채우지 않는다 — 요청에서 받은
                # 값을 여기서 주입한다. 모델이 남의 작품을 조회하도록 값을 지어낼 여지를
                # 없애는 것이 이 설계의 요점이다.
                result_text, summary = await asyncio.to_thread(
                    tool.run, user_id, work_id, **args
                )
                tool_records.append({"name": name, "summary": summary, "status": "DONE"})
            except Exception as exc:  # noqa: BLE001 — 도구 실패는 "근거 부족"으로 두고 대화를 계속한다
                logger.warning(
                    "도구 실행 실패 | tool=%s args=%s | %s", name, tool_call.function.arguments, exc
                )
                result_text = f"도구 실행 오류: {exc}"
                tool_records.append(
                    {"name": name, "summary": f"{name} 조회 실패", "status": "FAILED"}
                )
            tool_calls_used += 1

            convo.append(
                {"role": "tool", "tool_call_id": tool_call.id, "content": result_text}
            )

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
