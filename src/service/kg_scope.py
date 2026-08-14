"""요청의 (userId, workId)를 KG 테넌트로 해소하는 유일한 지점.

예전에는 이 파일이 "격리가 없다"는 사실을 감추는 자리였다 — 범위 함수는 빈 필터를
돌려주며 경고만 남겼고, 별도의 관문이 인덱싱된 작품 하나 외의 요청을 전부 400으로
거절해 그 구멍을 막았다.

이제 그래프가 테넌트를 구분하므로 둘 다 사라진다. 남는 것은 "요청을 테넌트 키로 바꾸는"
일 하나뿐이고, 그건 Tenant가 한다. 이 파일은 그 호출을 API 경계에서 한 번만 하도록
모아 두는 자리다 — 서비스마다 Tenant.of()를 직접 부르면 '어디서 스코프가 정해지는가'가
다시 흩어진다.
"""

from __future__ import annotations

from src.common.tenant import Tenant


def kg_scope(user_id: int, work_id: int) -> Tenant:
    """요청이 가리키는 KG 테넌트를 돌려준다.

    이 함수를 통과한 뒤에는 어떤 코드도 user_id/work_id를 직접 보지 않는다 — 그래프를
    읽고 쓰는 모든 자리는 Tenant만 받는다. 그래야 필터를 빠뜨린 경로가 타입으로 드러난다.
    """
    return Tenant.of(user_id, work_id)
