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

from openai import APIConnectionError, APIStatusError, AsyncOpenAI, RateLimitError

from src.common import llm_limit
from src.config import OPENAI_API_KEY

logger = logging.getLogger("openai")

_client = AsyncOpenAI(
    api_key=OPENAI_API_KEY,
    # SDK 자체 재시도를 끈다. 기본값(2회)을 켜두면 아래 루프와 **곱으로** 겹쳐 429 한 번의
    # 장애에 HTTP 요청이 최대 5×3=15회 나가고, insufficient_quota 판별과 헤더 계량이 전부
    # SDK가 재시도를 소진한 뒤에야 도달한다. 재시도 정책은 이 관문 한 층에만 둔다.
    # 대신 SDK가 커버하던 연결 오류·5xx 재시도는 아래 루프가 인수한다.
    max_retries=0,
    # 기본값 600초는 죽은 요청이 세마포어 슬롯을 10분 붙드는 시간이다. 모든 호출이
    # non-streaming이라 완결 응답 기준의 상한이면 충분하다. 넘기면 APITimeoutError
    # (APIConnectionError의 하위 클래스)가 나서 아래 루프의 재시도 대상이 된다.
    timeout=180.0,
)

# 재시도 상한(429·연결 오류·5xx 공통). 판정은 한 호출에 27k 토큰을 쓰므로 몇 편만 겹쳐도
# 조직 TPM에 순간적으로 몰린다 — 대개 몇 초 쉬면 풀린다.
_MAX_ATTEMPTS = 5


def _headers_of(exc: Exception) -> dict:
    """실패 응답에 실린 헤더. 못 찾으면 빈 dict.

    한도에 걸린 순간이 오히려 가장 정확한 값이라 실패 경로에서도 반드시 읽는다 —
    성공만 반영하면 미터가 "바닥난 상태"를 영영 못 본다.
    """
    response = getattr(exc, "response", None)
    return getattr(response, "headers", None) or {}


async def create_completion(**kwargs):
    """chat.completions.create — 추출·판정·인덱싱이 쓴다. 몸통은 _request 공용."""
    return await _request(_client.chat.completions.with_raw_response.create, kwargs)


async def create_response(**kwargs):
    """responses.create — 채팅이 쓴다(도구 + 추론 조합이 chat/completions에서는
    추론 기본 모델(gpt-5.6-luna 등)에 대해 400으로 거부되기 때문).

    responses의 with_raw_response도 chat.completions와 같은 LegacyAPIResponse 래퍼라
    헤더 계량·동기 parse() 계약이 그대로 성립한다 — 그래서 몸통을 공유할 수 있다.
    """
    return await _request(_client.responses.with_raw_response.create, kwargs)


async def _request(create_raw, kwargs: dict):
    """OpenAI 호출 한 번의 계량·통제·재시도 공통 몸통.

    재시도 대상은 세 가지다: 혼잡성 429, 연결 오류·타임아웃, 5xx. SDK 자체 재시도는
    꺼두었으므로(클라이언트 생성부 주석 참고) 이 목록이 재시도 정책의 전부다.
    4xx는 다시 보내도 같은 결과라 즉시 올린다.

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
                raw = await create_raw(**kwargs)
        except RateLimitError as exc:
            llm_limit.observe(model, _headers_of(exc))
            if _is_quota_exhausted(exc):
                logger.error("OpenAI 크레딧 소진 — 재시도하지 않는다")
                raise
            if attempt == _MAX_ATTEMPTS - 1:
                raise
            await _backoff(attempt)
            continue
        except APIConnectionError:
            # 연결 실패와 타임아웃(APITimeoutError는 APIConnectionError의 하위 클래스).
            # SDK 재시도를 껐으므로 일시적 네트워크 문제는 여기서 다시 시도해야 한다 —
            # 인덱싱은 한 화의 실패가 뒤 화 전부를 쓰러뜨리는 구조라 즉시 실패가 특히 아프다.
            if attempt == _MAX_ATTEMPTS - 1:
                raise
            logger.warning("OpenAI 연결 실패 — 재시도한다 | attempt=%d", attempt + 1)
            await _backoff(attempt)
            continue
        except APIStatusError as exc:
            # 5xx만 재시도한다. 4xx(잘못된 요청, 인증 실패 등)는 다시 보내도 같은 결과라
            # 그대로 올린다. 429는 위의 RateLimitError 분기가 먼저 잡으므로 여기 오지 않는다.
            if exc.status_code < 500:
                raise
            if attempt == _MAX_ATTEMPTS - 1:
                raise
            logger.warning(
                "OpenAI 서버 오류 — 재시도한다 | status=%d attempt=%d", exc.status_code, attempt + 1
            )
            await _backoff(attempt)
            continue

        llm_limit.observe(model, raw.headers)
        # parse()는 **동기**다. with_raw_response는 LegacyAPIResponse를 돌려주는데
        # 그쪽 parse()는 async 클라이언트에서도 동기다(_legacy_response.py:100).
        # `_response.py`의 AsyncAPIResponse.parse()는 async지만 그건
        # with_streaming_response 경로라 여기와 무관하다 — await를 붙이면
        # "ChatCompletion can't be used in 'await' expression"으로 죽는다.
        return raw.parse()

    raise AssertionError("unreachable")


async def _backoff(attempt: int) -> None:
    """지수 백오프 + 지터. 지터는 동시에 깨어난 호출들이 다시 몰리는 것을 막는다."""
    await asyncio.sleep(min(2**attempt, 30) + random.uniform(0, 1))


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
