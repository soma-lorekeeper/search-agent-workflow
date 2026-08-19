"""추출 프롬프트에 실을 "과거 회차에서 확립된 인물과 그 상태" 목록.

추출기는 이 목록으로 두 가지를 한다 — 원고의 "나"·"그 여자" 같은 지칭이 누구인지 고르고,
원고의 서술이 이 상태와 어긋나면 그 상태를 축(axis)으로 세운다. 목록 자체가 추출 범위는
아니다(그 경고는 프롬프트 머리말에 있다).

**검사 대상 회차 직전까지만** 싣는다. 상한이 없으면 5화를 검사하면서 6화 이후에 확립된
상태를 "이미 알려진 설정"으로 보게 되고, 뒤에서 밝혀질 반전을 5화의 오류로 읽는다.
"""

from __future__ import annotations

from src.common.tenant import Tenant
from src.repository.neo4j.client import DATABASE, get_driver

# 인물과 그 상태를 한 번에 긁어온다.
#
# 회차 컷은 CharacterState가 어느 사건에서 확립됐는지(ESTABLISHED_IN → Event.chapter)로
# 판정하고, 그 사건이 없으면 근거 청크(EVIDENCED_BY → Chunk.chapter)로 폴백한다.
# 상태 노드 자체에는 회차 속성이 없다.
#
# 인물(Character)에는 어떤 회차 표시도 없다 — 회차마다 같은 노드로 MERGE되는 정준
# 엔티티라 "언제 생겼는지"가 그래프에 남지 않는다. 그래서 검사 회차 이후에 처음 등장한
# 인물의 이름은 목록에 남는다. 이름만으로는 설정 정보가 거의 없어 실해가 작다고 보고
# 지금은 그대로 둔다.
_ENTITY_NODE_QUERY = """
MATCH (c:Character {tenant_id: $tenant_id})
OPTIONAL MATCH (c)-[:HAS_STATE]->(s:CharacterState {tenant_id: $tenant_id})
OPTIONAL MATCH (s)-[:ESTABLISHED_IN]->(ev:Event)
OPTIONAL MATCH (s)-[:EVIDENCED_BY]->(ck:Chunk)
WITH c, s, coalesce(min(ev.chapter), min(ck.chapter)) AS est_chapter
WHERE s IS NULL OR $up_to_chapter IS NULL OR est_chapter IS NULL OR est_chapter < $up_to_chapter
OPTIONAL MATCH (s)-[:ABOUT]->(t)
WITH c, s, t ORDER BY s.name
RETURN c.name AS name, c.aliases AS aliases,
       [x IN collect(DISTINCT {st: s.name, tgt: t.name}) WHERE x.st IS NOT NULL] AS states
ORDER BY c.name
"""


def render(tenant: Tenant, up_to_chapter: int | None = None) -> str:
    """인물 노드 목록을 프롬프트에 실을 텍스트로 렌더한다.

    형식은 인물마다 `이름 [별칭: …]` 한 줄, 그 아래 상태마다 `  - {상태} → {대상}`이다.

    캐시하지 않는다. 하네스는 프로세스당 한 번만 조회하고 재사용했지만(평가 중에는 그래프가
    안 변한다), 서비스는 인덱싱이 계속 돌아 그래프가 수시로 바뀐다 — 캐시하면 방금 인덱싱한
    회차의 설정을 못 보고 검사하게 된다.
    """
    driver = get_driver()
    try:
        records, _, _ = driver.execute_query(
            _ENTITY_NODE_QUERY,
            {"up_to_chapter": up_to_chapter, **tenant.params()},
            database_=DATABASE,
        )
    finally:
        driver.close()

    lines: list[str] = []
    for r in records:
        head = r["name"]
        if r["aliases"]:
            head += f" [별칭: {r['aliases']}]"
        lines.append(head)
        for s in r["states"]:
            tgt = f" → {s['tgt']}" if s.get("tgt") else ""
            lines.append(f"  - {s['st']}{tgt}")
    return "\n".join(lines)
