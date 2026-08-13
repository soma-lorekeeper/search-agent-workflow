"""작가와 대화하는 단일 에이전트.

설정 오류 검사(src/contradiction/agent.py)와 루프 구조는 같지만 목적이 다르다 — 거기서는
정해진 claim 하나를 구조화된 verdict로 판정하고, 여기서는 작가가 무엇을 물을지 알 수 없는
자유 대화를 근거 있는 자연어로 답한다. 그래서 응답을 JSON으로 강제하지 않고, 도구 호출을
tool_choice로 강제하지도 않는다(작품과 무관한 잡담에까지 KG를 뒤지게 만들면 느리고 이상하다).

대신 UI에 "무엇을 찾아봤는지"를 보여줘야 해서, 호출된 도구를 성공/실패까지 포함해 기록한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any

from openai import AsyncOpenAI, RateLimitError

from src.chat.prompts import (
    CHAT_CACHE_KEY,
    CHAT_SYSTEM_PROMPT,
    TITLE_CACHE_KEY,
    TITLE_SYSTEM_PROMPT,
)
from src.chat.tools import TOOL_GUIDE, build_chat_tools
from src.config import OPENAI_API_KEY, OPENAI_MODEL

logger = logging.getLogger("chat.agent")

MAX_TOOL_CALLS = 5
MAX_TURNS = MAX_TOOL_CALLS + 2  # 도구 호출 예산 + 최종 답변 여유 턴. 무한루프 방지용 안전판.
RATE_LIMIT_MAX_RETRIES = 5  # 조직 TPM 한도(429)에 걸렸을 때 재시도 횟수

# 이 길이를 넘으면 "첫 턴"이 아니다 — 제목은 대화가 시작될 때 한 번만 지으면 되고,
# 매 턴 다시 지으면 사이드바 제목이 계속 바뀌어 작가가 대화를 못 찾는다.
FIRST_TURN_MESSAGE_COUNT = 2
TITLE_MAX_CHARS = 20

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


def _system_prompt(context: dict | None) -> str:
    episode = (context or {}).get("current_episode_number")
    # 작가는 "이번 화", "직전 화"처럼 상대적으로 말한다. 기준점을 안 주면 모델이 회차를
    # 지어내므로, 모를 때는 모른다고 명시해 되묻게 만든다.
    current = (
        f"작가는 지금 {episode}화를 집필 중이다. 작가가 '이번 화'라고 하면 {episode}화, "
        f"'직전 화'라고 하면 {episode - 1}화를 뜻한다."
        if isinstance(episode, int)
        else "지금 어느 회차를 집필 중인지 알 수 없다. 회차를 특정해야 하면 작가에게 되물어라."
    )
    return (
        CHAT_SYSTEM_PROMPT.replace("{current_episode}", current)
        .replace("{tool_guide}", TOOL_GUIDE)
        .replace("{max_tool_calls}", str(MAX_TOOL_CALLS))
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


async def run_chat(
    work_id: int,
    session_id: int,
    messages: list[dict],
    context: dict | None = None,
) -> dict:
    """대화 한 턴을 처리한다.

    messages는 지금까지의 대화 전체({"role": "user"|"assistant", "content": str} 목록)이며
    마지막 항목이 이번에 답할 사용자 발화다. 세션 상태를 서버에 두지 않고 매번 통째로 받는 이유는,
    대화 기록의 진실의 원천이 API 서버(Spring)의 chat_messages 테이블이기 때문이다.

    반환: {"content": str, "tool_calls": [{"name", "summary", "status"}], "suggested_title": str|None}
    """
    tool_schemas, tools_by_name = build_chat_tools()

    convo: list[dict] = [
        {"role": "system", "content": _system_prompt(context)},
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

        response = await _create_with_retry(**kwargs)
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
                # work_id는 스키마에 없어 모델이 채우지 않는다 — 요청에서 받은 값을 여기서 주입한다.
                result_text, summary = await asyncio.to_thread(tool.run, work_id, **args)
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
