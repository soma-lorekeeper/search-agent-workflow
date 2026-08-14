"""설정 오류 탐지 파이프라인의 "회차 상한" 테스트.

검사 대상이 N화면 대조 기준은 N화 **직전**까지의 세계관이어야 한다. 상한이 없으면 두 가지가
동시에 망가진다: N화를 N화 자신이 만든 사실과 대조해 "일치"라고 자평하고, 아직 나오지 않은
N+1화 이후의 반전을 N화에 심어둔 모순으로 읽는다.

Neo4j 드라이버는 가짜로 갈아끼운다(LLM·DB 비용 없음). 그래프 덤프 렌더러만은 진짜
lorekeeper.context.dump_graph_text를 그대로 태워, 걸러낸 레코드로도 렌더가 깨지지 않는지까지
함께 본다(관계 한쪽 끝만 사라지면 렌더러가 KeyError로 죽는다).
"""

from __future__ import annotations

import asyncio

import pytest

from src.common.tenant import Tenant
from src.contradiction import pipeline

# 배경 컨텍스트를 만들 소설 한 편. 그래프를 읽는 모든 쿼리가 이 키로 좁혀진다.
TENANT = Tenant.of(42, 1)

# --- 가짜 그래프: 3화의 사건 하나와 7화의 사건·상태 하나, 그리고 둘 다에 등장하는 인물 하나 ---
NODE_RECORDS = [
    {"id": "c1", "labels": ["Character", "__Entity__"], "props": {"name": "카엘"}},
    {"id": "e3", "labels": ["Event"], "props": {"name": "카엘이 부상당한다", "chapter": 3.0}},
    {"id": "e7", "labels": ["Event"], "props": {"name": "카엘이 죽는다", "chapter": 7.0}},
    {"id": "s7", "labels": ["CharacterState"], "props": {"name": "사망"}},
]
REL_RECORDS = [
    {"s": "c1", "t": "APPEARS_IN", "e": "e3", "props": {}},
    {"s": "c1", "t": "APPEARS_IN", "e": "e7", "props": {}},
    {"s": "c1", "t": "HAS_STATE", "e": "s7", "props": {}},
    {"s": "s7", "t": "ESTABLISHED_IN", "e": "e7", "props": {}},
]
CHAPTER_SUMMARIES = [
    {"number": 3, "summary": "카엘이 부상당한다."},
    {"number": 5, "summary": "카엘이 검을 든다."},
    {"number": 7, "summary": "카엘이 죽는다."},
]
# 5화 상한에서 걸러져야 할 노드 — 실제로는 _FUTURE_NODES_CYPHER가 그래프에서 뽑는다.
FUTURE_IDS = [{"id": "e7"}, {"id": "s7"}]


class FakeDriver:
    """dump_graph_text·load_summaries·파이프라인 쿼리에 정해진 레코드를 돌려주는 가짜 드라이버.

    어떤 쿼리인지는 그 쿼리에만 있는 조각으로 알아본다. 받은 파라미터를 그대로 기록해서,
    회차 상한이 실제로 쿼리까지 내려갔는지 테스트가 확인할 수 있게 한다.
    """

    def __init__(self):
        self.calls: list[tuple[str, dict | None]] = []
        self.closed = False

    def execute_query(self, query, parameters_=None, database_=None, **kwargs):
        self.calls.append((query, parameters_))
        if "coalesce(n.chapter" in query:  # _FUTURE_NODES_CYPHER
            return FUTURE_IDS, None, None
        if "labels(n) AS labels" in query:  # 덤프: 노드
            return list(NODE_RECORDS), None, None
        if "type(rel) AS t" in query:  # 덤프: 관계
            return list(REL_RECORDS), None, None
        if "MATCH (s:CharacterState)" in query:  # 덤프: 상태의 성립 회차
            return [{"id": "s7", "chapter": 7.0}], None, None
        if "s:Story" in query:  # 전역 요약
            return [{"summary": "전역 요약(7화까지 반영됨)"}], None, None
        if "$up_to_chapter" in query:  # 우리 쪽 회차 요약(상한 적용 대상)
            up_to = (parameters_ or {}).get("up_to_chapter")
            rows = [r for r in CHAPTER_SUMMARIES if up_to is None or r["number"] < up_to]
            return rows, None, None
        if "LIMIT $window" in query:  # lorekeeper의 최근 회차 요약(파이프라인은 쓰지 않는다)
            return [], None, None
        raise AssertionError(f"예상하지 못한 쿼리: {query}")

    def close(self):
        self.closed = True


@pytest.fixture
def fake_driver(monkeypatch):
    driver = FakeDriver()
    monkeypatch.setattr(pipeline, "get_driver", lambda: driver)
    return driver


# ---------- 상한이 걸린 배경 컨텍스트 ----------


def test_bounded_context_drops_future_chapters(fake_driver):
    context = pipeline.background_context(TENANT, up_to_chapter=5)

    # 그래프: 3화 사건은 남고, 7화 사건과 거기서 성립한 상태는 사라진다.
    assert "카엘이 부상당한다" in context
    assert "카엘이 죽는다" not in context
    assert "사망" not in context
    # 인물 자체는 남는다 — 3화에 이미 나온 인물을 지우면 배경이 오히려 틀린다.
    assert "카엘" in context

    # 회차 요약: 5화 자신과 그 뒤(7화)는 빠진다. 자기 자신과 대조하면 늘 "일치"가 나온다.
    assert "[3화]" in context
    assert "[5화]" not in context
    assert "[7화]" not in context

    # 전역 요약은 전 회차를 한 문자열로 압축한 것이라 뒷부분만 덜어낼 수 없다 — 통째로 뺀다.
    assert "전역 요약" not in context


def test_bound_reaches_the_queries(fake_driver):
    pipeline.background_context(TENANT, up_to_chapter=5)
    # 미래 노드를 골라내는 쿼리도 테넌트로 좁는다. 상한만 걸면 그래프 전체를 훑어
    # 남의 작품 노드 id까지 제외 집합에 담는다(결과는 안 틀리지만 전수 스캔이 된다).
    params = [p for q, p in fake_driver.calls if "coalesce(n.chapter" in q]
    assert params == [{"up_to_chapter": 5, **TENANT.params()}]
    # 회차 요약 쿼리는 상한과 테넌트를 함께 받는다 — 상한만 걸고 테넌트를 빠뜨리면 남의
    # 작품 줄거리가 "기존 설정"으로 섞여 들어온다.
    params = [p for q, p in fake_driver.calls if "c.summary IS NOT NULL" in q]
    assert params == [{"up_to_chapter": 5, **TENANT.params()}]
    # 그래프 덤프(노드 조회)도 같은 테넌트로 좁혀졌다.
    params = [p for q, p in fake_driver.calls if "labels(n) AS labels" in q]
    assert params == [TENANT.params()]
    assert fake_driver.closed


def test_unbounded_context_is_unchanged(fake_driver):
    """회차 번호를 모르는 호출(CLI)은 예전 그대로 그래프 전체를 배경으로 쓴다."""
    context = pipeline.background_context(TENANT)

    assert "카엘이 죽는다" in context
    assert "전역 요약(7화까지 반영됨)" in context
    assert "[3화]" in context and "[5화]" in context and "[7화]" in context


# ---------- 상한 드라이버 자체 ----------


def test_bounded_driver_drops_relationships_touching_removed_nodes(fake_driver):
    """관계 한쪽 끝만 걸러내고 관계를 남기면 덤프 렌더러가 없는 노드를 찾다 죽는다."""
    bounded = pipeline._ChapterBoundedDriver(fake_driver, "neo4j", TENANT, 5)

    nodes, _, _ = bounded.execute_query(
        "MATCH (n) RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props"
    )
    assert [r["id"] for r in nodes] == ["c1", "e3"]

    rels, _, _ = bounded.execute_query(
        "MATCH (a)-[rel]->(b) RETURN elementId(a) AS s, type(rel) AS t, elementId(b) AS e, "
        "properties(rel) AS props"
    )
    assert [(r["s"], r["e"]) for r in rels] == [("c1", "e3")]


def test_bounded_driver_passes_other_queries_through(fake_driver):
    """상한 드라이버는 덤프가 던지는 쿼리만 손댄다 — 모르는 결과 모양은 그대로 통과시킨다."""
    bounded = pipeline._ChapterBoundedDriver(fake_driver, "neo4j", TENANT, 5)
    records, _, _ = bounded.execute_query(
        "MATCH (s:Story {id:'main'}) RETURN s.summary AS summary"
    )
    assert records == [{"summary": "전역 요약(7화까지 반영됨)"}]


# ---------- 회차 번호가 파이프라인 안까지 내려가는가 ----------


def test_streaming_passes_the_bound_to_the_context(monkeypatch):
    captured: list = []

    def _fake_context(tenant, up_to_chapter=None):
        captured.append((tenant, up_to_chapter))
        return "배경"

    async def _no_claims(text, background_context):
        return []

    monkeypatch.setattr(pipeline, "background_context", _fake_context)
    monkeypatch.setattr(pipeline, "extract_claims", _no_claims)

    # 테넌트와 회차 상한이 둘 다 배경 컨텍스트까지 내려가야 한다.
    assert asyncio.run(pipeline.check_new_episode_streaming("5화 원고", TENANT, 5)) == []
    assert captured == [(TENANT, 5)]

    # 회차를 모르면 상한 없이(=예전 동작) 돈다. 테넌트는 그래도 반드시 따라간다.
    assert asyncio.run(pipeline.check_new_episode_streaming("원고", TENANT)) == []
    assert captured == [(TENANT, 5), (TENANT, None)]
