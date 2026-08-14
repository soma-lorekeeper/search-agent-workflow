"""KG에 실제로 인덱싱된 회차 목록.

채팅 컨텍스트의 세 개념 중 하나다. 나머지 둘(집필 중인 회차, 보고 있는 회차)은 요청이
알려주지만, **이건 요청에서 받지 않는다** — 진실의 원천이 Neo4j 그래프 자신이기 때문이다.

"인덱싱됐다"의 정의는 `Chapter-[:IN_STORY]->Story` 마커다. lorekeeper 인덱싱의 마지막
쓰기가 이 관계라서(전역 요약 갱신과 같은 쿼리에서 MERGE된다), 이게 있으면 추출·근거링크·
요약까지 전부 끝났다는 뜻이다. 인덱싱 접수의 빠른 경로(webapp._already_indexed)도 같은
마커를 본다. 다른 출처(Spring의 회차 목록, 인덱싱 작업 상태, 환경변수 …)는 전부 이 마커와
어긋날 수 있다 — 회차가 저장돼 있어도 인덱싱은 아직 안 끝났을 수 있고, 작업이 done이어도
그건 이 서버 메모리의 주장일 뿐이다. 그래서 그래프에 직접 묻는다.

**프로세스 단위로 캐시하지 않는다.** 인덱싱이 끝나는 순간 목록이 늘어나므로, 한 번 읽어
모듈 전역에 담아두면 방금 인덱싱된 회차를 계속 "없다"고 답하게 된다. 요청 하나 안에서만
재사용한다(run_chat이 턴 시작에 한 번 부른다).
"""

from __future__ import annotations

import logging

from src.lorekeeper.client import get_driver
from src.lorekeeper.indexing import DATABASE as LOREKEEPER_DATABASE

from src.chat.kg_scope import kg_scope

logger = logging.getLogger("chat.indexed")

# 그래프 전체에서 마커가 달린 화 번호를 오름차순으로 전부 가져온다. 화 수가 많아야 수백이라
# 한 번에 다 읽어도 부담이 없고, "몇 화부터 몇 화까지"가 아니라 "어떤 화들이" 있는지를
# 알아야 중간에 빈 화(3, 4화만 인덱싱 실패)를 정확히 말해줄 수 있다.
INDEXED_EPISODES_CYPHER = """
MATCH (c:Chapter)-[:IN_STORY]->(s:Story)
RETURN c.number AS number
ORDER BY c.number
"""


def fetch_indexed_episodes(work_id: int) -> list[int]:
    """KG에 인덱싱이 끝난 화 번호를 오름차순으로 돌려준다.

    조회가 실패하면 빈 리스트다 — 그래프에 못 닿았는데 "전부 인덱싱돼 있다"고 가정하면
    모델이 없는 자료를 찾아 헤매다 지어낸 답을 내놓는다. 반대로 빈 리스트면 모델은
    "조회할 수 있는 회차가 없다"고 솔직히 말하게 되고, 그게 안전한 실패다.
    """
    kg_scope(work_id)
    try:
        driver = get_driver()
        try:
            records, _, _ = driver.execute_query(
                INDEXED_EPISODES_CYPHER, {}, database_=LOREKEEPER_DATABASE
            )
        finally:
            driver.close()
    except Exception as exc:  # noqa: BLE001 — 조회 실패가 대화 자체를 막으면 안 된다
        logger.warning("인덱싱된 회차 조회 실패 — 없는 것으로 취급한다 | %s", exc)
        return []

    return sorted({int(r["number"]) for r in records if r["number"] is not None})
