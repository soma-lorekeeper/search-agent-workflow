"""LLM 관문의 계약을 고정한다.

**기존 탐지 테스트로는 이 코드가 검증되지 않는다.** 그쪽은 소비 모듈의 이름
(`extract_service.create_completion`)을 통째로 스텁으로 갈아끼우므로 관문 안쪽이 아예
실행되지 않는다. 그래서 여기서는 `openai_client._client`를 가짜로 바꿔 **raw 응답 경로가
실제로 돌게** 한다.

여기서 틀리면 조용하다 — 헤더를 못 읽으면 미터가 영원히 가정값을 가리키고, 세마포어가
안 걸리면 호출이 한꺼번에 나간다. 둘 다 예외가 아니라 "한도에 걸려서야" 드러난다.
"""

from __future__ import annotations

import asyncio
import weakref
from types import SimpleNamespace

import httpx
import pytest
from openai import APIConnectionError, APIStatusError, RateLimitError

from src.common import llm_limit, openai_client

MODEL = "gpt-test"

_HEADERS = {
    "x-ratelimit-limit-tokens": "200000",
    "x-ratelimit-remaining-tokens": "123456",
    "x-ratelimit-reset-tokens": "6m0s",
    "x-ratelimit-limit-requests": "500",
    "x-ratelimit-remaining-requests": "321",
    "x-ratelimit-reset-requests": "1s",
}


@pytest.fixture(autouse=True)
def 상태_초기화(monkeypatch):
    """미터·세마포어 전역을 새 것으로 교체한다(test_llm_limit.py와 같은 이유)."""
    monkeypatch.setattr(llm_limit, "_buckets", {})
    monkeypatch.setattr(llm_limit, "_async_slots", weakref.WeakKeyDictionary())
    monkeypatch.setattr(llm_limit, "_thread_slots", {})


class _FakeRaw:
    """`with_raw_response.create()`가 돌려주는 것의 최소 흉내.

    ⚠️ `parse()`는 **동기**다. `with_raw_response`는 `LegacyAPIResponse`를 돌려주고
    그쪽 parse()는 async 클라이언트에서도 동기다(`openai/_legacy_response.py:100`).
    `_response.py`의 `AsyncAPIResponse.parse()`가 `async def`라 헷갈리기 쉬운데, 그건
    `with_streaming_response` 경로다.

    처음에 이 가짜를 `async def parse`로 만들었더니 **테스트는 전부 통과하는데 실제
    호출은 죽었다**(TypeError: ChatCompletion can't be used in 'await' expression).
    가짜가 틀리면 테스트가 틀린 계약을 굳힌다 — 실호출 스모크가 잡아낸 것이다.
    """

    def __init__(self, content: str, headers: dict):
        self.headers = headers
        self._content = content

    def parse(self):
        message = type("_M", (), {"content": self._content})()
        return type("_R", (), {"choices": [type("_C", (), {"message": message})()]})()


def _fake_client(create):
    """관문이 쓰는 두 경로(chat.completions / responses)의 with_raw_response.create만 갖춘
    가짜 클라이언트. 두 관문 함수가 같은 몸통(_request)을 쓰므로 같은 create를 물린다."""
    return SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(with_raw_response=SimpleNamespace(create=create))
        ),
        responses=SimpleNamespace(with_raw_response=SimpleNamespace(create=create)),
    )


def _rate_limit_error(headers: dict, body: dict) -> RateLimitError:
    """실제 SDK가 던지는 것과 같은 모양의 429. 헤더가 실려 있어야 미터가 읽을 수 있다."""
    response = httpx.Response(
        429, headers=headers, request=httpx.Request("POST", "https://api.openai.com/v1")
    )
    return RateLimitError("rate limited", response=response, body=body)


# ---------- 계량 ----------


def test_응답_헤더가_미터에_반영된다(monkeypatch):
    """관문이 헤더를 안 읽으면 미터는 영원히 콜드 스타트 가정값을 가리킨다."""

    async def create(**kwargs):
        return _FakeRaw("답", _HEADERS)

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))

    assert llm_limit.remaining(MODEL).remaining == 123456
    assert llm_limit.remaining_requests(MODEL).remaining == 321


def test_파싱된_응답을_돌려준다_코루틴이_아니라(monkeypatch):
    """AsyncAPIResponse.parse()는 async def다. await를 빼면 호출자가 코루틴을 받고,
    `.choices[0]`에서 AttributeError가 난다 — 관문 밖에서야 터지는 종류다."""

    async def create(**kwargs):
        return _FakeRaw("정상 응답", _HEADERS)

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    response = asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))

    assert not asyncio.iscoroutine(response)
    assert response.choices[0].message.content == "정상 응답"


def test_responses_관문도_헤더를_미터에_반영한다(monkeypatch):
    """채팅이 쓰는 responses 경로도 같은 몸통(_request)을 지나야 한다 — 여기서 계량이
    빠지면 채팅 호출만 미터 밖에서 돌아 admission 판단이 어긋난다."""

    async def create(**kwargs):
        return _FakeRaw("답", _HEADERS)

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    response = asyncio.run(openai_client.create_response(model=MODEL, input=[]))

    # 동기 parse 계약(코루틴 아님)도 responses 경로에서 함께 성립해야 한다.
    assert not asyncio.iscoroutine(response)
    assert llm_limit.remaining(MODEL).remaining == 123456


def test_responses_관문도_혼잡_429를_재시도한다(monkeypatch):
    """재시도 정책이 경로별로 갈라지면 한쪽만 고쳐지는 사고가 난다 — 몸통 공유의 계약."""
    calls = {"n": 0}

    async def create(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _rate_limit_error(_HEADERS, {"error": {"type": "rate_limit_exceeded"}})
        return _FakeRaw("성공", _HEADERS)

    async def _no_backoff(attempt):
        return None

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    monkeypatch.setattr(openai_client, "_backoff", _no_backoff)
    response = asyncio.run(openai_client.create_response(model=MODEL, input=[]))

    assert calls["n"] == 2
    assert response.choices[0].message.content == "성공"


def test_429의_헤더도_미터에_반영된다(monkeypatch):
    """한도에 걸린 순간이 가장 정확한 값이다. 성공만 반영하면 바닥난 상태를 못 본다."""
    바닥 = {**_HEADERS, "x-ratelimit-remaining-tokens": "0"}

    async def create(**kwargs):
        raise _rate_limit_error(바닥, {"error": {"type": "insufficient_quota"}})

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    with pytest.raises(RateLimitError):
        asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))

    assert llm_limit.remaining(MODEL).remaining == 0


# ---------- 재시도 ----------


def test_잔액_소진은_재시도하지_않는다(monkeypatch):
    """기다린다고 풀리지 않는다. 구분하지 않으면 실패가 확정된 요청으로 1분을 버린다."""
    calls = []

    async def create(**kwargs):
        calls.append(1)
        raise _rate_limit_error(_HEADERS, {"error": {"type": "insufficient_quota"}})

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    with pytest.raises(RateLimitError):
        asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))

    assert len(calls) == 1


def test_혼잡성_429는_상한까지_재시도한다(monkeypatch):
    """총 시도 횟수를 못박아 둔다 — 커밋 9에서 재시도 층을 정리할 때 기준이 된다."""
    calls = []

    async def create(**kwargs):
        calls.append(1)
        raise _rate_limit_error(_HEADERS, {"error": {"type": "rate_limit_exceeded"}})

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    monkeypatch.setattr(openai_client.asyncio, "sleep", _즉시)

    with pytest.raises(RateLimitError):
        asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))

    assert len(calls) == openai_client._MAX_ATTEMPTS


def test_재시도_뒤_성공하면_그_응답을_돌려준다(monkeypatch):
    calls = []

    async def create(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise _rate_limit_error(_HEADERS, {"error": {"type": "rate_limit_exceeded"}})
        return _FakeRaw("두 번째에 성공", _HEADERS)

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    monkeypatch.setattr(openai_client.asyncio, "sleep", _즉시)

    response = asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))
    assert response.choices[0].message.content == "두 번째에 성공"


def _status_error(status: int) -> APIStatusError:
    """SDK가 4xx/5xx에서 던지는 것과 같은 모양의 예외. status_code는 응답에서 읽힌다."""
    response = httpx.Response(
        status, request=httpx.Request("POST", "https://api.openai.com/v1")
    )
    return APIStatusError("server error", response=response, body=None)


def test_5xx는_상한까지_재시도한다(monkeypatch):
    """SDK 자체 재시도를 껐으므로(max_retries=0) 일시적 서버 오류는 관문이 다시 보내야
    한다 — 안 그러면 인덱싱에서 502 한 번이 잡 전체를 쓰러뜨린다."""
    calls = []

    async def create(**kwargs):
        calls.append(1)
        raise _status_error(502)

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    monkeypatch.setattr(openai_client.asyncio, "sleep", _즉시)

    with pytest.raises(APIStatusError):
        asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))

    assert len(calls) == openai_client._MAX_ATTEMPTS


def test_4xx는_재시도하지_않는다(monkeypatch):
    """잘못된 요청은 다시 보내도 같은 결과다. 재시도하면 확정 실패로 백오프 시간만 태운다."""
    calls = []

    async def create(**kwargs):
        calls.append(1)
        raise _status_error(400)

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    with pytest.raises(APIStatusError):
        asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))

    assert len(calls) == 1


def test_연결_오류는_재시도한다(monkeypatch):
    """타임아웃(APITimeoutError)도 APIConnectionError의 하위 클래스라 같은 분기를 탄다."""
    calls = []

    async def create(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise APIConnectionError(request=httpx.Request("POST", "https://api.openai.com/v1"))
        return _FakeRaw("재연결 성공", _HEADERS)

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))
    monkeypatch.setattr(openai_client.asyncio, "sleep", _즉시)

    response = asyncio.run(openai_client.create_completion(model=MODEL, messages=[]))
    assert response.choices[0].message.content == "재연결 성공"


async def _즉시(_seconds):
    """백오프를 건너뛴다. 재시도 횟수만 보는 테스트에서 실제로 기다릴 이유가 없다."""
    return None


# ---------- 통제 ----------


def test_동시_호출이_모델별_상한에_묶인다(monkeypatch):
    """관문을 지나는 모든 호출이 대상이다 — 탐지의 조각 병렬 발사도 여기서 묶인다."""
    monkeypatch.setattr(llm_limit, "_MAX_CONCURRENCY", 2)

    현재 = 0
    최대 = 0

    async def create(**kwargs):
        nonlocal 현재, 최대
        현재 += 1
        최대 = max(최대, 현재)
        await asyncio.sleep(0.01)
        현재 -= 1
        return _FakeRaw("답", _HEADERS)

    monkeypatch.setattr(openai_client, "_client", _fake_client(create))

    async def 여러_번():
        await asyncio.gather(
            *[openai_client.create_completion(model=MODEL, messages=[]) for _ in range(8)]
        )

    asyncio.run(여러_번())
    assert 최대 == 2
