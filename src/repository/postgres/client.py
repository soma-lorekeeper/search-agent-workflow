"""PostgreSQL 커넥션 팩토리.

Spring과 같은 DB를 본다 — 원고·작품 정보를 읽고, 탐지 결과를 그쪽 테이블에 쓴다.
접속 정보는 DATABASE_URL 하나이고 값은 배포 환경이 채운다(로컬은 .env, 운영은 RDS).
"""

from __future__ import annotations

import os

import psycopg

# 연결 타임아웃을 둔다. DB가 죽었을 때 무한정 기다리면 그동안 이벤트 루프의 스레드가
# 잠겨 채팅·탐지·헬스체크가 함께 멈춘다.
_CONNECT_TIMEOUT = 5


def connect():
    """DATABASE_URL로 커넥션을 연다. 호출자가 with로 닫는다.

    환경변수를 모듈 로드 시점이 아니라 호출 시점에 읽는다 — 로드 시점에 굳히면
    import 순서에 따라 .env가 아직 안 실린 상태의 값이 박힐 수 있다.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL이 설정되지 않았습니다.")
    return psycopg.connect(dsn, connect_timeout=_CONNECT_TIMEOUT)
