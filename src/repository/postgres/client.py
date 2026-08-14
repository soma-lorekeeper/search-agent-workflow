"""PostgreSQL 커넥션 팩토리. 이 DB에 닿는 유일한 지점이다.

Spring과 **같은 DB**를 본다 — 원고·작품 정보를 읽고, 탐지 결과를 그쪽 테이블에 쓴다.

접속 정보는 `DATABASE_URL` 하나다. Spring은 같은 DB를 `SPRING_DATASOURCE_URL`(JDBC 형식)
+ `_USERNAME` + `_PASSWORD` 세 개로 나눠 받는데, 우리는 psycopg를 쓰므로 자격증명까지 담은
libpq DSN 하나로 받는다. 두 형식이 가리키는 곳은 같아야 한다:

    Spring : jdbc:postgresql://HOST:5432/lorekeeper  +  USERNAME/PASSWORD
    여기   : postgresql://USERNAME:PASSWORD@HOST:5432/lorekeeper

로컬은 docker compose가 띄우는 같은 컨테이너를 함께 본다(양쪽 레포의 compose가 같은
포트·계정을 쓴다). 프로덕션은 같은 RDS이고, 값은 배포 스크립트가 인스턴스에
`/opt/agent/agent.env`로 넣어 준다 — 이 레포에는 자격증명이 없다.
"""

from __future__ import annotations

import os

import psycopg

# 연결 타임아웃을 둔다. DB가 죽었을 때 무한정 기다리면 그동안 이벤트 루프의 스레드가
# 잠겨 채팅·탐지·헬스체크가 함께 멈춘다.
_CONNECT_TIMEOUT = 5


def dsn() -> str:
    """접속 문자열. 없으면 즉시 실패한다.

    환경변수를 모듈 로드 시점이 아니라 호출 시점에 읽는다 — 로드 시점에 굳히면
    import 순서에 따라 .env가 아직 안 실린 상태의 값이 박힐 수 있다.
    """
    value = os.environ.get("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL이 설정되지 않았습니다.")
    return value


def target() -> str:
    """접속 대상을 자격증명 없이 표시한다(호스트/DB만). 점검 응답·로그에 쓴다."""
    _, _, hostpart = dsn().rpartition("@")
    return hostpart or "unknown"


def connect():
    """커넥션을 연다. 호출자가 with로 닫는다."""
    return psycopg.connect(dsn(), connect_timeout=_CONNECT_TIMEOUT)
