"""claim 하나를 검증하는 단일 에이전트.

부모 파이프라인(pipeline.py의 0/1/3/4단계)은 고정 순서를 따르는 결정론적 스크립트인 반면,
이 안쪽만 "어떤 근거가 필요한지 claim마다 다르고 미리 알 수 없다"는 불확실성이 있어
에이전틱하게 짰다 — LLM이 스스로 도구(tools.py의 lorekeeper 4종)를 골라 최대
MAX_TOOL_CALLS번 호출하고, 근거가 쌓이면 구조화된 verdict로 마무리한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any

from openai import AsyncOpenAI, RateLimitError

from src.config import OPENAI_API_KEY, OPENAI_MODEL
from src.contradiction.prompts import VERIFIER_SYSTEM_PROMPT, VERIFY_CACHE_KEY
from src.contradiction.tools import TOOL_GUIDE, format_tool_result

logger = logging.getLogger("contradiction.agent")

MAX_TOOL_CALLS = 4
MIN_TOOL_CALLS = 2  # 최소 이만큼은 도구를 호출한 뒤에야 근거 없이 바로 판정을 내릴 수 있게 허용
MAX_TURNS = MAX_TOOL_CALLS + 2  # 도구 호출 예산 + 최종 답변 여유 턴. 무한루프 방지용 안전판.
RATE_LIMIT_MAX_RETRIES = 5  # 조직 TPM 한도(429)에 걸렸을 때 재시도 횟수

_client = AsyncOpenAI(api_key=OPENAI_API_KEY)


async def _create_with_retry(**kwargs: Any) -> Any:
    """RateLimitError(429)면 지수 백오프로 재시도한다.

    claim마다 병렬로 검증 에이전트가 뜨는데, MIN_TOOL_CALLS 도입으로 claim 하나당 호출
    수가 늘면서 조직 TPM(분당 토큰) 한도에 순간적으로 몰리기 쉬워졌다 — 그대로 실패시키지
    않고 잠깐 쉬었다 다시 보낸다.
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


def _claim_user_message(claim: dict) -> str:
    entities = ", ".join(claim.get("entities") or [])
    return (
        f"[신규 회차 서술 (claim, 원문 그대로)]\n{claim.get('quote', '')}\n\n"
        f"[카테고리] {claim.get('category', '기타')}\n"
        f"[관련 대상] {entities or '(명시 안 됨)'}"
    )


def _parse_verdict(content: str | None) -> dict[str, Any]:
    try:
        data = json.loads(content or "{}")
    except json.JSONDecodeError:
        logger.warning("verdict JSON 파싱 실패 | content=%r", content)
        return {
            "label": "unknown",
            "established_fact": "",
            "source_episode": None,
            "explanation": "판정 응답 파싱 실패(모델이 JSON 형식을 지키지 않음).",
        }
    return {
        "label": data.get("label", "unknown"),
        "established_fact": data.get("established_fact", ""),
        "source_episode": data.get("source_episode"),
        "explanation": data.get("explanation", ""),
    }


async def verify_claim(
    claim: dict,
    background_context: str,
    tool_schemas: list[dict],
    tools_by_name: dict,
) -> dict:
    """claim 하나를 검증한다.

    반환: {**claim, label, established_fact, source_episode, explanation, tool_calls_used}
    """
    system_prompt = (
        VERIFIER_SYSTEM_PROMPT.replace("{background_context}", background_context)
        .replace("{tool_guide}", TOOL_GUIDE)
        .replace("{max_tool_calls}", str(MAX_TOOL_CALLS))
    )

    messages: list[dict] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": _claim_user_message(claim)},
    ]

    tool_calls_used = 0
    for _turn in range(MAX_TURNS):
        offer_tools = tool_calls_used < MAX_TOOL_CALLS
        kwargs: dict[str, Any] = {
            "model": OPENAI_MODEL,
            "messages": messages,
            "response_format": {"type": "json_object"},
            "prompt_cache_key": VERIFY_CACHE_KEY,
        }
        if offer_tools:
            kwargs["tools"] = tool_schemas
            # 최소 호출 수(MIN_TOOL_CALLS)를 채우기 전에는 근거 없이 바로 판정하지 못하게
            # tool_choice를 강제한다 — 그 이후에는 모델이 충분하다고 판단하면 스스로 끝낼 수 있다.
            kwargs["tool_choice"] = "required" if tool_calls_used < MIN_TOOL_CALLS else "auto"
            kwargs["parallel_tool_calls"] = False  # 예산(4회) 추적을 단순하게 유지

        response = await _create_with_retry(**kwargs)
        message = response.choices[0].message

        if not message.tool_calls:
            verdict = _parse_verdict(message.content)
            logger.info(
                "claim 검증 완료 | quote=%r label=%s tool_calls=%d",
                claim.get("quote", "")[:40],
                verdict["label"],
                tool_calls_used,
            )
            return {**claim, **verdict, "tool_calls_used": tool_calls_used}

        messages.append(
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
            tool = tools_by_name.get(tool_call.function.name)
            if tool is None or tool_calls_used >= MAX_TOOL_CALLS:
                result_text = (
                    "(사용할 수 없는 도구이거나 호출 예산을 모두 사용했습니다. "
                    "지금까지의 근거로 최종 판정하세요.)"
                )
            else:
                try:
                    args = json.loads(tool_call.function.arguments or "{}")
                    result = await asyncio.to_thread(tool.execute, **args)
                    result_text = format_tool_result(result)
                except Exception as exc:  # noqa: BLE001 — 도구 실행 실패도 "근거 부족"으로 취급해 계속 진행
                    logger.warning(
                        "도구 실행 실패 | tool=%s args=%s | %s",
                        tool_call.function.name,
                        tool_call.function.arguments,
                        exc,
                    )
                    result_text = f"도구 실행 오류: {exc}"
                tool_calls_used += 1

            messages.append(
                {"role": "tool", "tool_call_id": tool_call.id, "content": result_text}
            )

    logger.warning("claim 검증 턴 상한 도달 | quote=%r", claim.get("quote", "")[:40])
    return {
        **claim,
        "label": "unknown",
        "established_fact": "",
        "source_episode": None,
        "explanation": "턴 상한에 도달해 판정을 완료하지 못했습니다.",
        "tool_calls_used": tool_calls_used,
    }
