"""임베딩 계량의 계약을 고정한다.

임베딩은 이 서버에서 **호출 수가 가장 많은** 경로다 — 탐지 검색이 claim 하나당 최대
4채널을 던지고 채널마다 질의 임베딩이 한 번씩 나가므로, claim 100개면 400 요청이다.
그런데 토큰은 작아서 TPM으로는 안 잡히고 RPM으로 걸린다.

라이브러리 기본 embedder는 벡터만 돌려주고 원본 응답을 버려 헤더에도 usage에도 닿을 수
없다. 그래서 대체했고, 여기서 그 대체가 실제로 계량·통제를 하는지 확인한다.
"""

from __future__ import annotations

import threading
import weakref
from types import SimpleNamespace

import pytest

from src.common import graphrag, llm_limit

MODEL = "text-embedding-test"

_HEADERS = {
    "x-ratelimit-limit-tokens": "1000000",
    "x-ratelimit-remaining-tokens": "999000",
    "x-ratelimit-limit-requests": "3000",
    "x-ratelimit-remaining-requests": "2997",
    "x-ratelimit-reset-requests": "1s",
}


@pytest.fixture(autouse=True)
def 상태_초기화(monkeypatch):
    monkeypatch.setattr(llm_limit, "_buckets", {})
    monkeypatch.setattr(llm_limit, "_async_slots", weakref.WeakKeyDictionary())
    monkeypatch.setattr(llm_limit, "_thread_slots", {})


class _FakeRaw:
    """`embeddings.with_raw_response.create()`의 최소 흉내.

    동기 클라이언트라 `parse()`도 동기다(채팅 쪽 AsyncAPIResponse와 다르다).
    """

    def __init__(self, vector: list[float], headers: dict):
        self.headers = headers
        self._vector = vector

    def parse(self):
        return SimpleNamespace(data=[SimpleNamespace(embedding=self._vector)])


def _fake_client(create):
    return SimpleNamespace(embeddings=SimpleNamespace(with_raw_response=SimpleNamespace(create=create)))


def test_임베딩_헤더가_미터에_반영된다(monkeypatch):
    """embedding 버킷에는 헤더를 주는 다른 경로가 없다 — 여기서 못 읽으면 그 버킷은
    영원히 콜드 스타트 가정값만 가리킨다."""

    def create(**kwargs):
        return _FakeRaw([0.1, 0.2], _HEADERS)

    monkeypatch.setattr(graphrag, "_client", _fake_client(create))
    벡터 = graphrag.MeteredEmbedder(model=MODEL).embed_query("안녕")

    assert 벡터 == [0.1, 0.2]
    assert llm_limit.remaining(MODEL).remaining == 999000
    assert llm_limit.remaining_requests(MODEL).remaining == 2997, "RPM 축이 실제 병목이다"


def test_모델과_입력이_그대로_전달된다(monkeypatch):
    """모델이 어긋나면 벡터 공간이 달라져 검색이 조용히 무의미해진다."""
    seen = {}

    def create(**kwargs):
        seen.update(kwargs)
        return _FakeRaw([0.0], _HEADERS)

    monkeypatch.setattr(graphrag, "_client", _fake_client(create))
    graphrag.MeteredEmbedder(model=MODEL).embed_query("질의 텍스트")

    assert seen == {"input": "질의 텍스트", "model": MODEL}


def test_동시_임베딩이_상한에_묶인다(monkeypatch):
    """탐지 검색은 스레드풀에서 돌므로 asyncio가 아니라 threading 세마포어를 타야 한다."""
    monkeypatch.setattr(llm_limit, "_MAX_CONCURRENCY", 3)
    monkeypatch.setattr(llm_limit, "_EMBEDDING_CONCURRENCY", 3)

    상태_잠금 = threading.Lock()
    현재 = 0
    최대 = 0

    def create(**kwargs):
        nonlocal 현재, 최대
        with 상태_잠금:
            현재 += 1
            최대 = max(최대, 현재)
        threading.Event().wait(0.01)
        with 상태_잠금:
            현재 -= 1
        return _FakeRaw([0.0], _HEADERS)

    monkeypatch.setattr(graphrag, "_client", _fake_client(create))
    embedder = graphrag.MeteredEmbedder(model=MODEL)

    스레드들 = [threading.Thread(target=lambda: embedder.embed_query("x")) for _ in range(9)]
    for t in 스레드들:
        t.start()
    for t in 스레드들:
        t.join()

    assert 최대 == 3


def test_라이브러리_Embedder_계약을_만족한다():
    """retriever·TextChunkEmbedder가 이 인터페이스로만 부른다.

    추상 메서드를 안 채우면 인스턴스 생성 자체가 TypeError로 막히지만, async 쪽은 기본
    구현이라 조용히 빠질 수 있어 함께 확인한다.
    """
    from neo4j_graphrag.embeddings.base import Embedder

    embedder = graphrag.MeteredEmbedder(model=MODEL)
    assert isinstance(embedder, Embedder)
    assert callable(embedder.embed_query)
    assert callable(embedder.async_embed_query)
