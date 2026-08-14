"""
사실(Fact) 계층 검색 인덱스 생성.

Event/CharacterState에 공통 보조 라벨 `:Fact`를 붙이고, name+description을 임베딩해
벡터·풀텍스트 인덱스를 건다. retrieval.fact_search가 이 두 인덱스를 하이브리드로 쓴다.

왜 보조 라벨인가:
  Neo4j 벡터 인덱스는 대상 라벨을 하나만 받는다. 그런데 사실은 Event와 CharacterState
  두 라벨에 나뉘어 있어서, 라벨별로 인덱스를 만들면 검색할 때마다 두 인덱스를 각각
  질의하고 점수를 정규화·융합해 top_k를 다시 자르는 로직을 직접 짜야 한다. 공통 라벨
  `:Fact`를 하나 더 붙이면 인덱스가 하나로 합쳐져 라이브러리의 HybridCypherRetriever를
  그대로 재사용할 수 있다(융합·정규화·top_k가 공짜).
  풀텍스트 인덱스는 `FOR (n:A|B)`처럼 다중 라벨이 문법상 가능하지만, 하이브리드 융합은
  두 인덱스가 **같은 노드 집합**을 봐야 공정하므로 벡터와 같은 `:Fact`에 건다.

라벨은 기존 라벨을 대체하지 않고 추가되기만 한다(Event → Event:Fact). 그래서 기존
Cypher(`MATCH (e:Event)`)와 스키마는 전부 그대로 동작한다.
"""

from __future__ import annotations

from neo4j_graphrag.embeddings import OpenAIEmbeddings
from neo4j_graphrag.indexes import create_vector_index

from .chunks import EMBEDDING_DIMENSIONS
from .pipeline import EMBEDDING_MODEL

# 사실 계층 보조 라벨과 인덱스 이름. retrieval.py가 이 상수들을 단일 출처로 import한다.
FACT_LABEL = "Fact"
FACT_VECTOR_INDEX = "fact_emb"
FACT_FULLTEXT_INDEX = "fact_text_ft"
# Chunk 쪽(chunks.CHUNK_FULLTEXT_ANALYZER)과 같은 analyzer를 쓴다 — 한국어 조사·어미 결합에 강함.
FACT_FULLTEXT_ANALYZER = "cjk"


def _fact_text(name: str | None, description: str | None) -> str:
    """임베딩할 문자열. 이름만으로는 짧아 변별이 어려우므로 설명을 이어 붙인다."""
    parts = [p for p in (name, description) if p]
    return ": ".join(parts)


def ensure_fact_layer(driver, database: str) -> int:
    """
    Event/CharacterState에 `:Fact` 라벨 부여 + 임베딩 백필 + 인덱스 2종 보장.

    멱등이다 — 라벨은 SET(이미 있으면 무해), 임베딩은 `embedding IS NULL`인 노드만,
    인덱스는 IF NOT EXISTS. 회차가 늘어 사실이 추가되면 다시 부르면 된다.

    Event/CharacterState는 resolver가 병합하지 않는 라벨이라 description이 생성 후
    바뀌지 않는다. 그래서 "embedding이 없는 것만 채운다"는 조건으로 충분하고, 내용이
    바뀌었는지 검사하는 dirty-check가 필요 없다.

    반환: 이번 호출에서 새로 임베딩한 노드 수.
    """
    # 1. 보조 라벨 부여(기존 라벨은 유지되고 :Fact가 추가되기만 한다).
    driver.execute_query(
        f"MATCH (f) WHERE f:Event OR f:CharacterState SET f:{FACT_LABEL}",
        database_=database,
    )

    # 2. 임베딩 백필. 아직 벡터가 없는 노드만 골라 name+description을 임베딩한다.
    records, _, _ = driver.execute_query(
        f"MATCH (f:{FACT_LABEL}) WHERE f.embedding IS NULL "
        "RETURN elementId(f) AS eid, f.name AS name, f.description AS description",
        database_=database,
    )
    embedder = OpenAIEmbeddings(model=EMBEDDING_MODEL)
    embedded = 0
    for r in records:
        text = _fact_text(r["name"], r["description"])
        if not text:
            # name도 description도 없는 노드는 임베딩할 게 없다(검색 대상에서 자연 제외).
            continue
        vec = embedder.embed_query(text)
        driver.execute_query(
            "MATCH (f) WHERE elementId(f) = $eid "
            "CALL db.create.setNodeVectorProperty(f, 'embedding', $vec)",
            {"eid": r["eid"], "vec": vec},
            database_=database,
        )
        embedded += 1

    # 3. 벡터 인덱스 보장(idempotent). Chunk 쪽(chunks.write_chunk_layer)과 같은 설정 —
    #    같은 임베딩 모델이라 차원·유사도 함수가 일치해야 한다.
    create_vector_index(
        driver,
        FACT_VECTOR_INDEX,
        label=FACT_LABEL,
        embedding_property="embedding",
        dimensions=EMBEDDING_DIMENSIONS,
        similarity_fn="cosine",
        fail_if_exists=False,
        neo4j_database=database,
    )

    # 4. 풀텍스트 인덱스 보장. 라이브러리 create_fulltext_index는 analyzer를 지정할 수 없어
    #    raw Cypher로 cjk를 준다(chunks.py와 같은 이유). name과 description 둘 다 인덱싱한다.
    driver.execute_query(
        f"CREATE FULLTEXT INDEX {FACT_FULLTEXT_INDEX} IF NOT EXISTS "
        f"FOR (n:{FACT_LABEL}) ON EACH [n.name, n.description] "
        f"OPTIONS {{ indexConfig: {{ `fulltext.analyzer`: '{FACT_FULLTEXT_ANALYZER}' }} }}",
        database_=database,
    )

    # 5. 인덱스가 ONLINE이 될 때까지 기다린다. 생성은 비동기라, 기다리지 않고 바로 검색하면
    #    예외 없이 '빈 결과'가 와서 원인을 찾기 어려운 실패가 된다.
    driver.execute_query("CALL db.awaitIndexes(60)", database_=database)

    return embedded
