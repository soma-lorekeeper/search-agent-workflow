"""테넌트 필터가 인덱스를 타도록 보장한다.

테넌트 격리는 모든 읽기 쿼리에 `n.tenant_id = $tenant_id`를 덧붙이는 방식이다. 인덱스가
없으면 그 조건이 전체 스캔이 되고, 테넌트가 늘수록 모든 조회가 함께 느려진다.

벡터·풀텍스트 인덱스는 여기서 만들지 않는다 — 그쪽은 인덱싱 경로(chunk.py/fact.py)가
자기 데이터를 쓰면서 함께 보장한다. 여기 있는 것은 순수하게 "테넌트로 좁히기 위한" 인덱스뿐이다.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("neo4j.tenant")

# 테넌트로 좁혀 읽는 라벨과, 그 라벨에서 테넌트와 함께 자주 걸리는 두 번째 property.
# 조합 인덱스라 순서가 중요하다 — tenant_id가 앞이어야 테넌트로 먼저 좁힌다.
_TENANT_INDEXES: list[tuple[str, str, tuple[str, ...]]] = [
    # (인덱스 이름, 라벨, property 순서)
    ("chunk_tenant", "Chunk", ("tenant_id", "chapter")),
    ("chapter_tenant", "Chapter", ("tenant_id", "number")),
    ("story_tenant", "Story", ("tenant_id",)),
    ("fact_tenant", "Fact", ("tenant_id",)),
    ("character_tenant", "Character", ("tenant_id", "name")),
    ("item_tenant", "Item", ("tenant_id", "name")),
    ("location_tenant", "Location", ("tenant_id", "name")),
    ("organization_tenant", "Organization", ("tenant_id", "name")),
    ("event_tenant", "Event", ("tenant_id", "chapter")),
    ("characterstate_tenant", "CharacterState", ("tenant_id",)),
    # resolver의 병합 스캔이 __Entity__ 전체를 훑으므로 여기에도 인덱스가 필요하다.
    ("entity_tenant", "__Entity__", ("tenant_id",)),
]


def ensure_tenant_indexes(driver, database: str) -> None:
    """테넌트 조합 인덱스를 보장한다(멱등).

    실패해도 예외를 올리지 않는다 — 인덱스가 없으면 느려질 뿐 답은 맞고, 서버 기동이나
    인덱싱을 여기서 막을 이유가 없다. 대신 경고로 남겨 성능 저하의 원인을 추적할 수 있게 한다.
    """
    for name, label, props in _TENANT_INDEXES:
        columns = ", ".join(f"n.{p}" for p in props)
        try:
            driver.execute_query(
                f"CREATE INDEX {name} IF NOT EXISTS FOR (n:{label}) ON ({columns})",
                database_=database,
            )
        except Exception:  # noqa: BLE001 — 인덱스 실패는 기능 실패가 아니다
            logger.warning("테넌트 인덱스 생성 실패 | index=%s label=%s", name, label)
