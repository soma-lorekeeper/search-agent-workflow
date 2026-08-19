"""모델별 미터·세마포어의 계약을 고정한다.

여기 모인 것은 전부 **틀려도 예외가 안 나는** 종류다. 헤더를 잘못 읽어도, 보간이 어긋나도,
세마포어가 상한을 안 지켜도 서버는 멀쩡히 돌고 로그도 조용하다 — 대신 한도에 걸려서야
드러난다. 그래서 값 자체를 못박는다.

LLM을 부르지 않는다. 헤더와 usage를 직접 만들어 넣는다.
"""

from __future__ import annotations

import asyncio
import threading
import weakref

import pytest

from src.common import llm_limit

# **실측값이다** — gpt-5.6-luna 응답에서 그대로 옮겼다(2026-08-16,
# tests/test_llm_smoke.py). 지어낸 값을 쓰면 테스트가 틀린 계약을 굳힌다.
#
# reset이 밀리초 단위인 데 주목: OpenAI의 버킷은 60초 창이 아니라 **연속 충전**이다.
# reset은 "만수위 복귀까지 남은 시간"이라, 조금만 썼으면 몇 ms 만에 다 찬다.
# 만료 테스트가 6m0s를 쓰는 건 시간 흐름을 크게 잡아 검증하기 쉬워서다.
_HEADERS = {
    "x-ratelimit-limit-tokens": "200000",
    "x-ratelimit-remaining-tokens": "199989",
    "x-ratelimit-reset-tokens": "3ms",
    "x-ratelimit-limit-requests": "500",
    "x-ratelimit-remaining-requests": "499",
    "x-ratelimit-reset-requests": "120ms",
}

MODEL = "test-model"


@pytest.fixture(autouse=True)
def 상태_초기화(monkeypatch):
    """모듈 전역 상태를 매 테스트마다 새 것으로 바꾼다.

    `.clear()`가 아니라 **교체**하는 이유: 세마포어 dict는 이벤트 루프에 묶인 객체를
    담고 있어서, 앞 테스트의 루프에 묶인 것이 남으면 다음 테스트가 그걸 물려받는다
    (test_index_api.py가 `_index_queue`를 같은 이유로 교체한다).
    """
    monkeypatch.setattr(llm_limit, "_buckets", {})
    monkeypatch.setattr(llm_limit, "_async_slots", weakref.WeakKeyDictionary())
    monkeypatch.setattr(llm_limit, "_thread_slots", {})


# ---------- 헤더 읽기 ----------


def test_응답_헤더가_양축에_반영된다():
    """토큰만 세면 임베딩처럼 '토큰은 작은데 요청이 많은' 경로를 못 본다."""
    llm_limit.observe(MODEL, _HEADERS)

    tokens = llm_limit.remaining(MODEL)
    assert tokens.limit == 200000
    assert tokens.remaining == 199989

    requests = llm_limit.remaining_requests(MODEL)
    assert requests.limit == 500
    assert requests.remaining == 499


def test_헤더_대소문자와_httpx_형식을_둘_다_받는다():
    """운영에서는 httpx.Headers(대소문자 무시)가 오고 테스트는 평범한 dict를 넘긴다.

    한쪽만 지원하면 테스트는 통과하는데 운영에서 헤더를 통째로 못 읽는 상태가 된다 —
    예외 없이 미터만 영원히 비어 있게 되므로 눈에 띄지 않는다.
    """
    llm_limit.observe(MODEL, {"X-RateLimit-Remaining-Tokens": "12345"})
    assert llm_limit.remaining(MODEL).remaining == 12345


@pytest.mark.parametrize(
    "value, expected",
    [
        ("6m0s", 360.0),
        ("1s", 1.0),
        ("500ms", 0.5),
        ("1h2m3s", 3723.0),
        ("120ms", 0.12),
    ],
)
def test_reset_기간_문자열을_초로_읽는다(value, expected):
    """reset 헤더는 숫자가 아니라 "6m0s" 같은 기간 문자열이다.

    ms를 m보다 먼저 시도하지 않으면 "500ms"가 500분으로 읽혀, 복구 시각이 8시간 뒤가
    된다 — 그 사이 내내 "한도 소진"으로 보인다.
    """
    assert llm_limit.parse_duration(value) == pytest.approx(expected)


@pytest.mark.parametrize("value", [None, "", "garbage", "60"])
def test_읽을_수_없는_reset은_축을_건드리지_않는다(value):
    """형식이 바뀌었다고 호출을 죽이지 않는다 — reset을 모르는 것과 못 부르는 것은 다르다."""
    assert llm_limit.parse_duration(value) is None


# ---------- 만료와 복구 ----------


def test_reset_시각이_지나면_만수위로_돌아온다(monkeypatch):
    """복구를 안 하면 한 번 바닥난 모델이 영원히 바닥으로 남는다."""
    clock = [1000.0]
    monkeypatch.setattr(llm_limit.time, "monotonic", lambda: clock[0])

    # 실측 헤더의 reset은 3ms라 시간 흐름을 재기 어렵다. 여기서는 크게 잡는다 —
    # 실제로도 많이 쓴 상태면 reset이 분 단위로 길어진다("만수위 복귀까지"의 뜻이라
    # 잔량이 적을수록 커진다).
    llm_limit.observe(
        MODEL,
        {**_HEADERS, "x-ratelimit-remaining-tokens": "10", "x-ratelimit-reset-tokens": "6m0s"},
    )
    assert llm_limit.remaining(MODEL).remaining == 10

    clock[0] += 359  # reset은 6m0s = 360초
    assert llm_limit.remaining(MODEL).remaining == 10, "아직 복구되면 안 된다"

    clock[0] += 2
    복구 = llm_limit.remaining(MODEL)
    assert 복구.remaining == 200000
    assert 복구.reset_at is None, "복구했으면 예약도 지워야 다음 헤더가 새로 정한다"


# ---------- 보간 ----------


def test_헤더_없는_경로는_차감으로_메운다():
    """라이브러리 내부를 지나는 호출은 헤더에 닿을 수 없다.

    그쪽을 아예 안 세면 인덱싱이 쓴 토큰이 미터에 안 잡혀, 게이트가 '여유롭다'고
    착각한다.
    """
    llm_limit.observe(MODEL, _HEADERS)
    llm_limit.spend(MODEL, tokens=1000)

    assert llm_limit.remaining(MODEL).remaining == 198989
    assert llm_limit.remaining_requests(MODEL).remaining == 498, "요청 축도 함께 줄어야 한다"


def test_다음_헤더가_보간분을_덮어써_오차가_쌓이지_않는다():
    """보간은 증분이라 틀릴 수 있다. 절대값이 오면 갈아치워야 오차가 누적되지 않는다."""
    llm_limit.observe(MODEL, _HEADERS)
    llm_limit.spend(MODEL, tokens=50000)  # 일부러 크게 틀린 차감
    assert llm_limit.remaining(MODEL).remaining == 149989

    llm_limit.observe(MODEL, {**_HEADERS, "x-ratelimit-remaining-tokens": "180000"})
    assert llm_limit.remaining(MODEL).remaining == 180000, "보간분이 남아 있으면 안 된다"


def test_차감은_0_아래로_내려가지_않는다():
    """음수 잔량은 호출자에게 의미가 없고, 비교식을 조용히 뒤집는다."""
    llm_limit.observe(MODEL, _HEADERS)
    llm_limit.spend(MODEL, tokens=999_999_999)
    assert llm_limit.remaining(MODEL).remaining == 0


def test_헤더를_한_번도_못_봤어도_차감이_동작한다():
    """콜드 스타트. 시작점을 모르면 뺄 수가 없으므로 가정값을 깔아둔다."""
    llm_limit.spend(MODEL, tokens=100)
    남음 = llm_limit.remaining(MODEL)
    assert 남음.limit == llm_limit._ASSUMED_TPM
    assert 남음.remaining == llm_limit._ASSUMED_TPM - 100


def test_한도에_걸린_응답의_헤더도_반영된다():
    """429일 때가 가장 정확한 값이다. 성공만 반영하면 바닥난 순간을 영영 못 본다."""
    llm_limit.observe(MODEL, {**_HEADERS, "x-ratelimit-remaining-tokens": "0"})
    assert llm_limit.remaining(MODEL).remaining == 0


def test_호출이_없던_모델은_updated_at이_비어_있다():
    """'이 경로가 게이트웨이를 지났는가'를 판별하는 수단이다.

    전체 검증에서 세 버킷이 모두 찍혔는지 보는 근거가 이 필드다.
    """
    assert llm_limit.snapshot() == {}
    llm_limit.observe(MODEL, _HEADERS)
    assert llm_limit.snapshot()[MODEL].updated_at is not None


# ---------- 세마포어 ----------


async def _최대_동시_진입(model: str, 동시_요청: int) -> int:
    """세마포어를 통과하는 코루틴을 동시에 띄우고, 한 순간 최대 몇 개가 안에 있었는지 센다."""
    semaphore = llm_limit.async_slot(model)
    현재 = 0
    최대 = 0

    async def 한_번():
        nonlocal 현재, 최대
        async with semaphore:
            현재 += 1
            최대 = max(최대, 현재)
            await asyncio.sleep(0.01)  # 겹칠 시간을 준다
            현재 -= 1

    await asyncio.gather(*[한_번() for _ in range(동시_요청)])
    return 최대


def test_동시_호출이_모델별_상한을_넘지_않는다(monkeypatch):
    """상한이 없으면 탐지 한 건이 조각 수만큼 한꺼번에 발사한다."""
    monkeypatch.setattr(llm_limit, "_MAX_CONCURRENCY", 3)
    assert asyncio.run(_최대_동시_진입(MODEL, 동시_요청=10)) == 3


def test_세마포어가_루프가_바뀌어도_죽지_않는다(monkeypatch):
    """루프 바인딩 회귀 방지 — 이 스위트에서 가장 중요한 한 건이다.

    asyncio.Semaphore는 **대기자가 생길 때만** 이벤트 루프를 기억한다(자리가 있으면
    숫자만 줄이고 끝난다). 그래서 모듈 전역 하나로 두면 경합이 없는 동안은 전부
    통과하다가, 한 테스트가 포화시킨 뒤 **다음 루프를 쓰는 무관한 테스트**가
    "bound to a different event loop"로 죽는다. 실패가 원인이 아니라 피해자를 가리키고,
    테스트 순서를 바꾸면 죽는 대상도 바뀐다.

    그래서 두 루프 모두 **일부러 상한을 초과시켜** 대기자 경로를 강제한다. gather로
    상한보다 많이 넣지 않으면 대기자가 안 생겨 이 테스트가 아무것도 검증하지 못한다.
    """
    monkeypatch.setattr(llm_limit, "_MAX_CONCURRENCY", 2)

    assert asyncio.run(_최대_동시_진입(MODEL, 동시_요청=6)) == 2
    # 새 이벤트 루프. TestClient를 두 번 만드는 것과 같은 조건이다.
    assert asyncio.run(_최대_동시_진입(MODEL, 동시_요청=6)) == 2


def test_임베딩은_다른_상한을_쓴다(monkeypatch):
    """임베딩은 호출당 토큰이 작고 요청 수가 많아 병목 축이 다르다."""
    monkeypatch.setattr(llm_limit, "_MAX_CONCURRENCY", 3)
    monkeypatch.setattr(llm_limit, "_EMBEDDING_CONCURRENCY", 7)
    assert llm_limit.limit_for(llm_limit.EMBEDDING_MODEL) == 7
    assert llm_limit.limit_for(MODEL) == 3


def test_스레드_경로도_상한을_지킨다(monkeypatch):
    """임베딩은 동기 인터페이스라 스레드에서 온다 — asyncio 세마포어를 쓸 수 없다."""
    monkeypatch.setattr(llm_limit, "_MAX_CONCURRENCY", 2)
    semaphore = llm_limit.thread_slot(MODEL)

    상태_잠금 = threading.Lock()
    현재 = 0
    최대 = 0

    def 한_번():
        nonlocal 현재, 최대
        with semaphore:
            with 상태_잠금:
                현재 += 1
                최대 = max(최대, 현재)
            threading.Event().wait(0.01)
            with 상태_잠금:
                현재 -= 1

    스레드들 = [threading.Thread(target=한_번) for _ in range(8)]
    for t in 스레드들:
        t.start()
    for t in 스레드들:
        t.join()

    assert 최대 == 2


def test_같은_모델은_같은_세마포어를_돌려준다():
    """매번 새로 만들면 상한이 호출 수만큼 곱해져 통제가 무의미해진다."""

    async def 두_번_꺼낸다():
        return llm_limit.async_slot(MODEL) is llm_limit.async_slot(MODEL)

    assert asyncio.run(두_번_꺼낸다())


def test_루프_밖에서_async_slot을_부르면_즉시_실패한다():
    """조용히 다른 걸 돌려주면 통제가 걸린 줄 알고 넘어간다 — 드러나는 편이 낫다."""
    with pytest.raises(RuntimeError):
        llm_limit.async_slot(MODEL)
