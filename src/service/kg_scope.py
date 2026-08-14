"""KG 조회의 작품 범위(work 격리)를 다루는 유일한 지점.

지금 Neo4j 그래프에는 작품 구분이 아예 없다 — lorekeeper-poc는
`MERGE (c:Chapter {number: $chapter})`처럼 화 번호만으로 노드를 병합하고, 어떤 노드/관계에도
work_id를 심지 않는다. 즉 그래프 전체가 "인덱싱된 작품 하나"다.

그렇다고 호출부에서 work_id를 아예 빼버리면, 나중에 격리가 생겼을 때 API·도구·에이전트
시그니처를 전부 다시 손대야 한다. 그래서 인터페이스는 처음부터 "작품별로 격리된다"는 전제로
짜두되(모든 KG 도구가 work_id를 첫 인자로 받는다), 실제 필터는 이 파일 하나에서만 무력화한다.

lorekeeper-poc가 work_id를 갖게 되면 바뀌는 건 이 파일뿐이다 — kg_scope()가 빈 dict 대신
{"work_id": work_id} 같은 쿼리 파라미터를 돌려주도록 고치면 되고, 도구/에이전트/웹앱 코드는
그대로다.
"""

from __future__ import annotations

import logging
import os

from fastapi import HTTPException

logger = logging.getLogger("chat.kg_scope")

# 이 서버가 바라보는 Neo4j에 실제로 인덱싱돼 있는 작품 id. 그래프가 스스로 밝힐 방법이 없어
# 환경변수로 못박는다(운영에서 다른 작품을 인덱싱했다면 이 값만 바꾸면 경고가 정상화된다).
KG_INDEXED_WORK_ID = int(os.environ.get("KG_INDEXED_WORK_ID", "1"))


def kg_scope(work_id: int) -> dict:
    """KG 조회에 덧붙일 범위 필터를 돌려준다 — 지금은 격리가 없어 항상 빈 dict다.

    요청한 작품이 인덱싱된 작품과 다르면 경고를 남긴다. 조회를 막지는 않는다: 막으면 데모가
    통째로 죽고, 어차피 그래프에는 답할 데이터가 그것뿐이라 "다른 작품 데이터가 샜다"가 아니라
    "요청한 작품 데이터가 애초에 없다"에 가깝다. 대신 로그로 그 사실을 반드시 남겨,
    나중에 엉뚱한 답변의 원인을 추적할 수 있게 한다.
    """
    if work_id != KG_INDEXED_WORK_ID:
        logger.warning(
            "KG에 작품 격리가 없어 work_id=%s 요청을 인덱싱된 단일 그래프(work_id=%s)로 응답한다",
            work_id,
            KG_INDEXED_WORK_ID,
        )
    return {}


def require_indexed_work(work_id: int) -> None:
    """다른 작품의 요청이면 400으로 거절한다. HTTP 경계(POST /api/index·/api/chat·/api/detect)
    에서만 부른다.

    kg_scope()는 "조회 범위"를 다루고 이 함수는 "요청을 받을지 말지"를 다룬다 — 다른 판단이지만
    근거가 같아서(그래프에 작품 격리가 없다) 같은 파일에 둔다. 격리가 생기면 kg_scope()가
    진짜 필터를 돌려주는 그 순간 이 함수도 통째로 사라져야 한다. 두 곳에 흩어져 있으면 하나만
    지우고 다른 하나가 남아 "격리가 있는데도 다른 작품을 거절하는" 서버가 된다.

    왜 경고가 아니라 거절인가: 그래프가 작품을 구분하지 못하므로, 작품 B의 6화를 받아 인덱싱하면
    작품 A의 6화 위에 덮어써지고, 이미 A의 6화가 있으면 완료 마커에 걸려 **아무 일도 하지 않은 채
    done**으로 보고된다(조용한 데이터 유실). 조회(채팅·탐지)도 마찬가지로 남의 작품 그래프를
    자기 작품인 양 답한다. 어느 쪽이든 호출자는 성공으로 알기 때문에, 여기서 시끄럽게 실패하는
    편이 유일하게 안전하다.

    kg_scope()는 여전히 경고만 남긴다 — 그쪽은 이 관문을 통과한 뒤에 불리므로 실제로는
    work_id가 항상 일치하고, 관문 없이 불리는 내부 경로(도구·CLI)까지 막을 이유는 없다.
    """
    if work_id != KG_INDEXED_WORK_ID:
        raise HTTPException(
            status_code=400,
            detail=(
                f"workId={work_id}는 이 에이전트가 다룰 수 없습니다. 지식 그래프에 아직 작품 격리가 "
                f"없어 그래프 전체가 인덱싱된 작품 하나(workId={KG_INDEXED_WORK_ID})이고, 다른 작품의 "
                f"회차를 받으면 기존 작품의 그래프를 덮어쓰거나 남의 작품 데이터로 답하게 됩니다."
            ),
        )
