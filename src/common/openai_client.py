"""OpenAI 호출의 단일 관문.

추출·판정·채팅이 전부 이 함수를 지난다. 예전에는 세 곳이 각자 클라이언트를 만들고 각자
재시도를 구현했는데, 그러면 재시도 정책이 조용히 갈라지고(한쪽만 고치면 다른 쪽은 그대로)
테스트에서 LLM을 막으려면 세 군데를 각각 가짜로 바꿔야 한다.

관문이 하나면 테스트가 여기 한 곳만 대체해도 서비스 전체가 LLM 없이 돈다. 그리고 관문이
하나이기 때문에 **여기서만** 두 가지를 할 수 있다:

  1. 계량 — 응답 헤더의 `x-ratelimit-*`로 모델별 남은 양을 실측한다. 이 값은 조직 전체를
     세므로 우리 프로세스 밖의 소비자까지 반영돼 있다(src/common/llm_limit.py 참고).
  2. 통제 — 모델별 세마포어로 동시에 나가는 호출 수를 묶는다. 여기를 지나지 않는 경로가
     없으므로, 새 호출자가 늘어도 자동으로 통제 안에 들어온다.

**헤더를 얻으려면 raw 응답을 거쳐야 한다.** 평범한 `create()`는 파싱된 객체만 돌려주고
원본 HTTP 응답을 그 자리에서 버려서, 헤더에 닿을 방법이 없다.
"""

from __future__ import annotations

import asyncio
import logging
import random

from openai import AsyncOpenAI, RateLimitError

from src.common import llm_limit
from src.config import OPENAI_API_KEY

logger = logging.getLogger("openai")

_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# 429에 대한 지수 백오프. 판정은 한 호출에 27k 토큰을 쓰므로 몇 편만 겹쳐도 조직 TPM에
# 순간적으로 몰린다 — 대개 몇 초 쉬면 풀린다.
_MAX_ATTEMPTS = 5


def _headers_of(exc: Exception) -> dict:
    """실패 응답에 실린 헤더. 못 찾으면 빈 dict.

    한도에 걸린 순간이 오히려 가장 정확한 값이라 실패 경로에서도 반드시 읽는다 —
    성공만 반영하면 미터가 "바닥난 상태"를 영영 못 본다.
    """
    response = getattr(exc, "response", None)
    return getattr(response, "headers", None) or {}


async def create_completion(**kwargs):
    """chat.completions.create를 부르되, 계량·통제·재시도를 함께 한다.

    **잔액 소진은 재시도하지 않는다.** OpenAI는 크레딧이 바닥나도 429를 주는데, 그건
    기다린다고 풀리지 않는다. 구분하지 않으면 이미 실패가 확정된 요청을 붙들고 5회를
    헛돌며 최대 1분을 버린다(실제로 겪었다). 호출자가 즉시 실패로 다룰 수 있게 그대로
    올려보낸다.

    세마포어는 **실제 호출 구간만** 감싼다. 백오프로 쉬는 동안까지 슬롯을 붙들면
    대기 중인 호출이 슬롯을 다 먹어 아무도 못 나가는 상태가 된다 — 세마포어가 세려는
    것은 "지금 날아가 있는 호출 수"이지 "재시도를 기다리는 수"가 아니다.
    """
    model = kwargs.get("model") or ""

    for attempt in range(_MAX_ATTEMPTS):
        try:
            async with llm_limit.async_slot(model):
                # with_raw_response 를 거쳐야 헤더가 남는다. 평범한 create()는 파싱된
                # 객체만 돌려주고 원본 응답을 버린다.
                raw = await _client.chat.completions.with_raw_response.create(**kwargs)
        except RateLimitError as exc:
            llm_limit.observe(model, _headers_of(exc))
            if _is_quota_exhausted(exc):
                logger.error("OpenAI 크레딧 소진 — 재시도하지 않는다")
                raise
            if attempt == _MAX_ATTEMPTS - 1:
                raise
            # 지터를 더해 동시에 깨어난 호출들이 다시 몰리는 것을 막는다.
            await asyncio.sleep(min(2**attempt, 30) + random.uniform(0, 1))
            continue

        llm_limit.observe(model, raw.headers)
        # parse()는 **동기**다. with_raw_response는 LegacyAPIResponse를 돌려주는데
        # 그쪽 parse()는 async 클라이언트에서도 동기다(_legacy_response.py:100).
        # `_response.py`의 AsyncAPIResponse.parse()는 async지만 그건
        # with_streaming_response 경로라 여기와 무관하다 — await를 붙이면
        # "ChatCompletion can't be used in 'await' expression"으로 죽는다.
        return raw.parse()

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
