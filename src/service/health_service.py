"""연동 상태 점검.

프로덕션에서 이 서버가 실제로 두 DB 에 닿는지 확인하는 용도다. API 서버(Spring)가
이 결과를 받아 자기 것과 합쳐서 프론트에 내려준다.

각 점검을 독립적으로 잡아서, 하나가 죽어도 나머지 결과는 보이게 한다 — 어디가
끊겼는지 구분되지 않으면 점검의 의미가 없다.
"""

import os
import time

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "neo4j")
DATABASE_URL = os.environ.get("DATABASE_URL", "")


def _timed(fn):
    """점검 하나를 실행하고 결과를 공통 모양으로 감싼다."""
    started = time.monotonic()
    try:
        detail = fn()
        ok, error = True, None
    except Exception as exc:  # 어떤 실패든 점검 결과로 바꿔서 돌려준다
        detail, ok, error = None, False, f"{type(exc).__name__}: {exc}"
    return {
        "ok": ok,
        "detail": detail,
        "error": error,
        "latency_ms": round((time.monotonic() - started) * 1000, 1),
    }


def _check_neo4j():
    # lorekeeper 가 이미 드라이버를 들고 있지만, 점검은 그 상태에 의존하지 않고
    # 매번 새로 확인한다. 캐시된 드라이버가 살아 있어도 서버가 죽었을 수 있다.
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            session.run("RETURN 1").consume()
        return {"uri": NEO4J_URI}
    finally:
        driver.close()


def _check_postgres():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL 이 설정되지 않았다")

    import psycopg

    with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
    # 자격증명이 섞인 URL 을 그대로 노출하지 않는다. 호스트/DB 만 남긴다.
    _, _, hostpart = DATABASE_URL.rpartition("@")
    return {"server": version.split(",")[0], "target": hostpart or "unknown"}


def collect():
    """두 DB 를 점검하고 전체 상태를 함께 돌려준다."""
    checks = {
        "neo4j": _timed(_check_neo4j),
        "postgres": _timed(_check_postgres),
    }
    return {
        "service": "agent",
        "status": "ok" if all(c["ok"] for c in checks.values()) else "degraded",
        "checks": checks,
    }
