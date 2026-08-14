"""OpenAI 호출의 단일 관문.

추출·판정·채팅이 전부 이 함수를 지난다. 예전에는 세 곳이 각자 클라이언트를 만들고 각자
재시도를 구현했는데, 그러면 재시도 정책이 조용히 갈라지고(한쪽만 고치면 다른 쪽은 그대로)
테스트에서 LLM을 막으려면 세 군데를 각각 가짜로 바꿔야 한다.

관문이 하나면 테스트가 여기 한 곳만 대체해도 서비스 전체가 LLM 없이 돈다.
"""

from __future__ import annotations

import asyncio
import logging
import random

from openai import AsyncOpenAI, RateLimitError

from src.config import OPENAI_API_KEY

logger = logging.getLogger("openai")

_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# 429에 대한 지수 백오프. 판정은 한 호출에 27k 토큰을 쓰므로 몇 편만 겹쳐도 조직 TPM에
# 순간적으로 몰린다 — 대개 몇 초 쉬면 풀린다.
_MAX_ATTEMPTS = 5


async def create_completion(**kwargs):
    """chat.completions.create를 부르되 rate limit만 백오프 재시도한다.

    **잔액 소진은 재시도하지 않는다.** OpenAI는 크레딧이 바닥나도 429를 주는데, 그건
    기다린다고 풀리지 않는다. 구분하지 않으면 이미 실패가 확정된 요청을 붙들고 5회를
    헛돌며 최대 1분을 버린다(실제로 겪었다). 호출자가 즉시 실패로 다룰 수 있게 그대로
    올려보낸다.
    """
    for attempt in range(_MAX_ATTEMPTS):
        try:
            return await _client.chat.completions.create(**kwargs)
        except RateLimitError as exc:
            if _is_quota_exhausted(exc):
                logger.error("OpenAI 크레딧 소진 — 재시도하지 않는다")
                raise
            if attempt == _MAX_ATTEMPTS - 1:
                raise
            # 지터를 더해 동시에 깨어난 호출들이 다시 몰리는 것을 막는다.
            await asyncio.sleep(min(2**attempt, 30) + random.uniform(0, 1))
    raise AssertionError("unreachable")


def _is_quota_exhausted(exc: RateLimitError) -> bool:
    """이 429가 '기다리면 풀리는 혼잡'이 아니라 '잔액 없음'인지 본다.

    SDK가 응답 본문을 어떤 모양으로 들고 있는지는 버전마다 다르므로, 구조를 뒤지다
    실패하면 문자열로 확인한다 — 여기서 예외가 나면 원래의 실패 원인이 가려진다.
    """
    try:
        code = (exc.body or {}).get("error", {}).get("type") or ""
        if code == "insufficient_quota":
            return True
    except Exception:  # noqa: BLE001 — 판별 실패가 호출 실패를 가리면 안 된다
        pass
    return "insufficient_quota" in str(exc)
