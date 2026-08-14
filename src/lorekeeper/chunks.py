"""
Chunk/Chapter provenance 레이어 생성.

회차 원고의 KSS 조각을 Chunk 노드(임베딩 포함)로 저장하고, 회차를 Chapter 노드로 승격해
각 Chunk를 IN_CHAPTER로 잇는다. Chunk 벡터·풀텍스트 인덱스도 여기서 보장한다(그래서 소비 쪽은
검색 전에 인덱스를 따로 만들 필요가 없다).

Chunk 노드·NEXT_CHUNK·임베딩은 라이브러리 컴포넌트(TextChunkEmbedder + LexicalGraphBuilder +
Neo4jWriter)로 만들고, Chapter 노드와 IN_CHAPTER는 number를 int로 정확히 넣기 위해 후처리
Cypher(MERGE)로 만든다.
"""

from __future__ import annotations

from neo4j_graphrag.experimental.components.kg_writer import Neo4jWriter
from neo4j_graphrag.experimental.components.lexical_graph import LexicalGraphBuilder
from neo4j_graphrag.experimental.components.types import TextChunk, TextChunks
from neo4j_graphrag.indexes import create_vector_index

from .pipeline import build_embedder

# 임베딩 차원(text-embedding-3-small) 및 Chunk 벡터 인덱스 이름.
EMBEDDING_DIMENSIONS = 1536
CHUNK_VECTOR_INDEX = "chunk_emb"
# Chunk 풀텍스트 인덱스(하이브리드 검색용). analyzer는 cjk 고정(한국어 조사·어미 결합에 강함).
# retrieval.py가 이 상수들을 단일 출처로 import해 쓴다.
CHUNK_FULLTEXT_INDEX = "chunk_text_ft"
CHUNK_FULLTEXT_ANALYZER = "cjk"


async def write_chunk_layer(driver, database: str, chapter: int, raw_chunks) -> None:
    """
    회차의 KSS 조각(raw_chunks)을 Chunk 노드로 쓰고 Chapter로 묶는다.

    raw_chunks: KSSSentenceSplitter가 낸 TextChunk 리스트(text/index만 채워진 상태).
    각 조각에 결정적 uid(chunk-{chapter}-{index})와 chapter metadata를 부여해 재실행 시 upsert되고
    Chunk.chapter property가 채워지게 한다.
    """
    # 결정적 uid·chapter metadata 부여(uid=upsert 키, metadata.chapter=Chunk.chapter property).
    chunks = [
        TextChunk(
            text=c.text,
            index=c.index,
            uid=f"chunk-{chapter}-{c.index}",
            metadata={"chapter": chapter},
        )
        for c in raw_chunks
    ]

    # Chunk 노드·임베딩·NEXT_CHUNK 생성(라이브러리 재사용). 임베딩은 Neo4jWriter가
    # setNodeVectorProperty로 벡터 property에 쓴다. 기본 LexicalGraphConfig의 lexical 라벨에
    # Chunk가 포함돼 __Entity__가 안 붙는다 → resolver가 Chunk를 안 건드린다.
    embedded = await build_embedder().run(TextChunks(chunks=chunks))
    chunk_graph = (await LexicalGraphBuilder().run(embedded)).graph
    await Neo4jWriter(
        driver=driver, neo4j_database=database, clean_db=False
    ).run(chunk_graph)

    # Chapter 노드 + IN_CHAPTER(후처리 Cypher). number를 int로 정확히 넣고 MERGE라 idempotent.
    # 직접 Cypher 생성이라 __Entity__가 안 붙는다(resolver-safe).
    driver.execute_query(
        "MERGE (c:Chapter {number: $chapter})",
        {"chapter": chapter},
        database_=database,
    )
    driver.execute_query(
        "MATCH (ck:Chunk {chapter: $chapter}) "
        "MATCH (c:Chapter {number: $chapter}) "
        "MERGE (ck)-[:IN_CHAPTER]->(c)",
        {"chapter": chapter},
        database_=database,
    )

    # 벡터 인덱스 보장(1회, idempotent). 이후 회차 Chunk는 인덱스가 자동 갱신한다.
    create_vector_index(
        driver,
        CHUNK_VECTOR_INDEX,
        label="Chunk",
        embedding_property="embedding",
        dimensions=EMBEDDING_DIMENSIONS,
        similarity_fn="cosine",
        fail_if_exists=False,
        neo4j_database=database,
    )

    # 풀텍스트 인덱스 보장(하이브리드 검색용, idempotent). 라이브러리 create_fulltext_index는
    # analyzer를 지정할 수 없어 raw Cypher로 cjk analyzer를 준다. 인덱싱 때 함께 만들어 두므로
    # 소비 쪽은 검색 전에 ensure_search_indexes()를 따로 부를 필요가 없다.
    driver.execute_query(
        f"CREATE FULLTEXT INDEX {CHUNK_FULLTEXT_INDEX} IF NOT EXISTS "
        f"FOR (n:Chunk) ON EACH [n.text] "
        f"OPTIONS {{ indexConfig: {{ `fulltext.analyzer`: '{CHUNK_FULLTEXT_ANALYZER}' }} }}",
        database_=database,
    )
