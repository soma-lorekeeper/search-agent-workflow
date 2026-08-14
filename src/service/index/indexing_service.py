"""
회차 누적 인덱싱 진입점 함수 (컴포넌트).

`indexing(chapter, text)` 한 번 호출 = 한 회차 인덱싱. 이전 회차까지 누적된 결과 위에 새 회차를
얹는다(Neo4jWriter는 upsert만 하고 기존 데이터를 지우지 않는다). 두 개의 병렬 산출을 만든다.

  1. 추출(KG): 회차 원고 전체를 단일 청크로 넣어(WholeTextSplitter) Character/Event/CharacterState
     등 도메인 그래프를 추출한다. 회차 내 coreference를 한 컨텍스트에서 해소하기 위함.
  2. 근거·벡터(Chunk): 같은 원고를 KSS로 잘게 쪼개 Chunk 노드로 저장하고 text-embedding-3-small로
     임베딩한다. 추출된 Event/CharacterState는 근거 문장이 속한 Chunk를 evidence_chunk 번호로
     가리키며, 후처리(evidence.link_evidence)가 이를 EVIDENCED_BY 관계로 잇는다. 회차는 Chapter
     노드로 승격하고 각 Chunk를 IN_CHAPTER로 잇는다. 요약은 두 층으로 관리한다 — 회차별
     Chapter.summary(원천)와, 매 회차 일정 크기로 갱신되는 전역 Story.summary(컨텍스트 주입용).

배경 컨텍스트(novel_context) 조립과 회차 요약은 context.py, 근거 링크는 evidence.py로 분리했고,
이 모듈은 그 컴포넌트들을 순서대로 엮는 오케스트레이터다. '새 회차 우선' 지침으로 anchoring을
막는다(extractor.py).

호출:
    await indexing(tenant, chapter, text)
"""

from __future__ import annotations

import os

from src.common.tenant import Tenant
from src.repository.neo4j.chunk import write_chunk_layer
from src.repository.neo4j.client import DATABASE, get_driver
from src.service.index.context_service import (
    build_context,
    dump_graph_text,
    load_summaries,
    summarize_episode,
    update_global_summary,
)
from src.repository.neo4j.evidence import link_evidence
from src.service.index.extraction_examples import EXTRACTION_FEW_SHOT
from src.repository.neo4j.fact import ensure_fact_layer
from src.service.index.extraction_pipeline import build_pipeline
from src.service.index.resolver import PerLabelResolver, collapse_merged_descriptions
from src.service.index.graph_schema import NODE_TYPES, PATTERNS, RELATIONSHIP_TYPES
from src.repository.neo4j.tenant_bootstrap import ensure_tenant_indexes
from src.service.index.splitters import KSSSentenceSplitter, WholeTextSplitter


# 추출 LLM의 reasoning effort. 기본값 'high'(luna 기본 세팅).
# LOREKEEPER_REASONING 환경변수로 덮어쓸 수 있다.
_EXTRACT_REASONING = os.environ.get("LOREKEEPER_REASONING", "high")

# 근거·벡터용 KSS 조각 크기(글자). 100자면 청크당 평균 약 3문장으로, 근거를 문장 단위로
# 정밀하게 짚기에 적합하다.
DEFAULT_KSS_CHUNK_SIZE = 100


def _label_counts(driver, database: str, tenant: Tenant) -> dict[str, int]:
    """메타 라벨을 제외한 라벨별 노드 수(간단한 결과 출력용). 이 소설 범위만 센다."""
    records, _, _ = driver.execute_query(
        "MATCH (n) WHERE n.tenant_id = $tenant_id "
        "UNWIND labels(n) AS lab RETURN lab AS lab, count(*) AS cnt",
        tenant.params(),
        database_=database,
    )
    return {
        r["lab"]: r["cnt"]
        for r in records
        if r["lab"] not in {"__Entity__", "__KGBuilder__"}
    }


def _rel_counts(driver, database: str, tenant: Tenant) -> dict[str, int]:
    """관계 타입별 수. 양끝이 모두 이 소설인 관계만 센다."""
    records, _, _ = driver.execute_query(
        "MATCH (a)-[r]->(b) WHERE a.tenant_id = $tenant_id AND b.tenant_id = $tenant_id "
        "RETURN type(r) AS t, count(*) AS cnt",
        tenant.params(),
        database_=database,
    )
    return {r["t"]: r["cnt"] for r in records}


async def indexing(tenant: Tenant, chapter: int, text: str) -> dict:
    """
    한 회차를 누적 인덱싱한다.

    tenant: 이 회차가 속한 소설. 이 함수가 만드는 모든 노드에 새겨진다.
    chapter: 회차 번호(정수). Event.chapter·Chapter.number·Chunk.chapter의 근거가 된다.
    text: 회차 원고 전체.
    반환: 라벨/관계 카운트·토큰·요약을 담은 dict.

    드라이버·DB·reasoning·청크 크기는 파라미터가 아니라 모듈 상수를 쓴다
    (DATABASE / _EXTRACT_REASONING / DEFAULT_KSS_CHUNK_SIZE). 드라이버는 이 함수가 열고 닫는다.
    """
    database = DATABASE
    driver = get_driver()
    try:
        # 0. 테넌트 조합 인덱스 보장(멱등). 모든 읽기가 tenant_id로 좁히므로 인덱스가 없으면
        #    소설이 늘수록 전부 함께 느려진다.
        ensure_tenant_indexes(driver, database)

        # 1. 배경 컨텍스트(전역 줄거리 요약 + 최근 회차 요약 + 그래프 덤프) 조합.
        #    덤프의 Event/CharacterState는 이름·구조 정보만 실린다(description 제외).
        #    전부 이 소설 범위로만 읽는다 — 스코프가 없으면 남의 소설 줄거리가 추출
        #    프롬프트의 '지금까지의 이야기'로 들어간다.
        graph_dump = dump_graph_text(driver, database, tenant)
        global_summary, recent_summaries = load_summaries(driver, database, tenant)
        context = build_context(graph_dump, global_summary, recent_summaries)
        print(
            f"배경 컨텍스트 길이: {len(context)}자 "
            f"(그래프 {len(graph_dump)}자 + 전역 요약 {len(global_summary)}자 "
            f"+ 최근 요약 {len(recent_summaries)}자)"
        )

        # 2. 근거·벡터용 KSS 청킹. raw.chunks는 (a)Chunk 레이어 생성과 (b)추출 마커 조립에
        #    함께 쓰이므로 여기서 한 번만 쪼갠다. overlap=0: 겹침이 있으면 경계 문장이 인접
        #    두 [C{i}] 마커에 중복 노출돼 LLM의 evidence_chunk 번호 선택이 모호해지므로 끈다.
        raw = await KSSSentenceSplitter(
            chunk_size=DEFAULT_KSS_CHUNK_SIZE, chunk_overlap=0
        ).run(text)

        # 3. Chunk/Chapter provenance 레이어 생성(Chunk 노드·임베딩·NEXT_CHUNK + Chapter +
        #    IN_CHAPTER + 벡터 인덱스). 상세는 chunks.write_chunk_layer 참고.
        await write_chunk_layer(driver, database, tenant, chapter, raw.chunks)

        # 4. 추출 텍스트 조립: [chapter:N] 헤더 + 각 KSS 조각 앞에 [C{index}] 마커.
        #    마커 번호는 Chunk.index와 일치 → LLM의 evidence_chunk 번호가 Chunk에 매핑된다.
        marked = f"[chapter:{chapter}]\n" + " ".join(
            f"[C{c.index}] {c.text}" for c in raw.chunks
        )

        # 5. 추출 파이프라인 실행(회차 통째 단일 청크, DB 누적). resolver는 라벨별 전략
        #    (Character=fuzzy / Item·Location·Organization=정규화 exact / CharacterState·Event=무병합).
        pipe, llm = build_pipeline(
            WholeTextSplitter(),
            PerLabelResolver(driver=driver, tenant=tenant, neo4j_database=database),
            driver,
            database,
            tenant,
            reasoning_effort=_EXTRACT_REASONING,
            novel_context=context,
            clean_db=True,
        )
        data = {
            "splitter": {"text": marked},
            "schema": {
                "node_types": NODE_TYPES,
                "relationship_types": RELATIONSHIP_TYPES,
                "patterns": PATTERNS,
            },
            "extractor": {"examples": EXTRACTION_FEW_SHOT},
        }
        await pipe.run(data)

        # 6. evidence_chunk 번호 → EVIDENCED_BY 관계(resolver 뒤).
        link_evidence(driver, database, tenant, chapter)

        # 7. 병합으로 배열이 된 description을 LLM으로 한 문자열로 합친다(resolver 뒤, evidence 라벨은
        #    병합되지 않으므로 link_evidence와 순서 무관).
        await collapse_merged_descriptions(driver, database, tenant)

        # 7-1. 사실 계층 검색 인덱스 보장(:Fact 라벨 + 임베딩 백필 + fact_emb/fact_text_ft).
        #      description 병합(7)이 끝나 텍스트가 확정된 뒤에 임베딩해야 하고, 요약 LLM
        #      호출(8)보다 앞에 두어 실패 시 비용 낭비를 줄인다. 멱등이라 재실행에 안전하다.
        ensure_fact_layer(driver, database, tenant)

        # 8. 이 회차 요약 생성 → Chapter.summary에 저장(전역 요약의 입력이자 drift 시 재구축 원천).
        summary = await summarize_episode(text)
        driver.execute_query(
            "MATCH (c:Chapter {number: $chapter, tenant_id: $tenant_id}) SET c.summary = $summary",
            {"chapter": chapter, "summary": summary, **tenant.params()},
            database_=database,
        )
        # 이어서 전역 줄거리 요약(Story.summary)을 일정 크기로 갱신하고 Chapter를 Story에 잇는다.
        await update_global_summary(driver, database, tenant, chapter, summary)

        # 9. 결과 요약(누적된 전체 DB 기준).
        labels = _label_counts(driver, database, tenant)
        rels = _rel_counts(driver, database, tenant)
        print(f"\n=== {chapter}화 인덱싱 완료 (누적) ===")
        print(f"    라벨: {labels}")
        print(f"    관계: {rels}")
        print(
            f"    추출 토큰(req/resp/total): {llm.total_request_tokens}/"
            f"{llm.total_response_tokens}/{llm.total_tokens} "
            f"(LLM 호출 {llm.call_count}회)"
        )
        print(f"    이 회차 요약:\n{summary}")
        return {
            "chapter": chapter,
            "labels": labels,
            "rels": rels,
            "tokens": {
                "request": llm.total_request_tokens,
                "response": llm.total_response_tokens,
                "total": llm.total_tokens,
            },
            "summary": summary,
        }
    finally:
        driver.close()
