"""
근거(EVIDENCED_BY) 후처리.

추출된 Event/CharacterState는 근거 문장이 속한 Chunk를 evidence_chunk 번호(예: 'C3')로 들고
있다. 이 모듈이 그 번호를 실제 Chunk 노드에 EVIDENCED_BY 관계로 잇고 임시 property를 제거한다.
추출 파이프라인(resolver 포함)이 끝난 뒤 호출한다.
"""

from __future__ import annotations

from src.common.tenant import Tenant


def link_evidence(driver, database: str, tenant: Tenant, chapter: int) -> None:
    """
    Event/CharacterState의 evidence_chunk 번호를 EVIDENCED_BY 관계로 잇는다.

    'C3' → index 3, 'C3,C4' → 두 Chunk에 각각 연결. 링크 후 임시 property를 제거한다.

    "evidence_chunk를 가진 노드 = 이번 회차 추출분"이라는 전제로 도는데, 그 전제는 테넌트
    안에서만 참이다. 다른 소설을 인덱싱하다 중간에 실패해 남은 evidence_chunk가 있으면,
    스코프 없는 쿼리는 그 잔해를 이번 회차 Chunk에 이어 붙인다 — 남의 소설 사건이 내 소설
    본문을 근거로 갖게 된다. 그래서 두 쿼리 모두 테넌트로 좁힌다.
    """
    driver.execute_query(
        """
        MATCH (fact)
        WHERE (fact:Event OR fact:CharacterState)
          AND fact.evidence_chunk IS NOT NULL
          AND fact.tenant_id = $tenant_id
        UNWIND [x IN split(fact.evidence_chunk, ',') | trim(x)] AS tok
        WITH fact, tok WHERE tok =~ 'C[0-9]+'
        MATCH (ck:Chunk {chapter: $chapter, tenant_id: $tenant_id, index: toInteger(substring(tok, 1))})
        MERGE (fact)-[:EVIDENCED_BY]->(ck)
        """,
        {"chapter": chapter, **tenant.params()},
        database_=database,
    )
    driver.execute_query(
        """
        MATCH (fact)
        WHERE (fact:Event OR fact:CharacterState)
          AND fact.evidence_chunk IS NOT NULL
          AND fact.tenant_id = $tenant_id
        REMOVE fact.evidence_chunk
        """,
        tenant.params(),
        database_=database,
    )
