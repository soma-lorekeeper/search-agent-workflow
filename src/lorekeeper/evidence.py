"""
근거(EVIDENCED_BY) 후처리.

추출된 Event/CharacterState는 근거 문장이 속한 Chunk를 evidence_chunk 번호(예: 'C3')로 들고
있다. 이 모듈이 그 번호를 실제 Chunk 노드에 EVIDENCED_BY 관계로 잇고 임시 property를 제거한다.
추출 파이프라인(resolver 포함)이 끝난 뒤 호출한다.
"""

from __future__ import annotations


def link_evidence(driver, database: str, chapter: int) -> None:
    """
    Event/CharacterState의 evidence_chunk 번호를 EVIDENCED_BY 관계로 잇는다.

    이 시점에 evidence_chunk property를 가진 노드는 모두 이번 회차 추출분이다(이전 회차 노드의
    evidence_chunk는 링크 후 제거했으므로). 그래서 현재 chapter의 Chunk에 매칭한다.
    'C3' → index 3, 'C3,C4' → 두 Chunk에 각각 연결. 링크 후 임시 property를 제거한다.
    """
    driver.execute_query(
        """
        MATCH (fact)
        WHERE (fact:Event OR fact:CharacterState) AND fact.evidence_chunk IS NOT NULL
        UNWIND [x IN split(fact.evidence_chunk, ',') | trim(x)] AS tok
        WITH fact, tok WHERE tok =~ 'C[0-9]+'
        MATCH (ck:Chunk {chapter: $chapter, index: toInteger(substring(tok, 1))})
        MERGE (fact)-[:EVIDENCED_BY]->(ck)
        """,
        {"chapter": chapter},
        database_=database,
    )
    driver.execute_query(
        """
        MATCH (fact)
        WHERE (fact:Event OR fact:CharacterState) AND fact.evidence_chunk IS NOT NULL
        REMOVE fact.evidence_chunk
        """,
        database_=database,
    )
