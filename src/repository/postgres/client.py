"""PostgreSQL 커넥션 팩토리. 이 DB에 닿는 유일한 지점이다.

Spring과 **같은 DB**를 본다 — 원고·작품 정보를 읽고, 탐지 결과를 그쪽 테이블에 쓴다.

접속 정보는 Spring과 **같은 형식**으로 받는다 — `SPRING_DATASOURCE_URL`(JDBC 형식)
+ `_USERNAME` + `_PASSWORD` 세 개. 같은 DB를 보는 두 서비스가 같은 변수를 쓰면 값을
한 벌만 관리하면 된다. psycopg는 JDBC 형식을 모르므로 여기서 libpq DSN으로 변환한다:

    받는 값 : jdbc:postgresql://HOST:5432/lorekeeper  +  USERNAME/PASSWORD
    변환 후 : postgresql://USERNAME:PASSWORD@HOST:5432/lorekeeper

`DATABASE_URL`(libpq DSN 하나)도 여전히 받는다 — 프로덕션 배포 스크립트(mvp-infra-iac의
deploy-agent-remote.sh)가 이 키로 `/opt/agent/agent.env`를 만들기 때문이다. 둘 다 있으면
DATABASE_URL이 이긴다(네이티브 형식이 명시적 지정에 가깝다).

로컬은 docker compose가 띄우는 같은 컨테이너를 함께 본다(양쪽 레포의 compose가 같은
포트·계정을 쓴다). 프로덕션은 같은 RDS이고, 값은 배포 스크립트가 인스턴스에
`/opt/agent/agent.env`로 넣어 준다 — 이 레포에는 자격증명이 없다.
"""

from __future__ import annotations

import os
from urllib.parse import quote

import psycopg

# 연결 타임아웃을 둔다. DB가 죽었을 때 무한정 기다리면 그동안 이벤트 루프의 스레드가
# 잠겨 채팅·탐지·헬스체크가 함께 멈춘다.
_CONNECT_TIMEOUT = 5


def dsn() -> str:
    """접속 문자열(libpq DSN). 어느 형식으로도 없으면 즉시 실패한다.

    환경변수를 모듈 로드 시점이 아니라 호출 시점에 읽는다 — 로드 시점에 굳히면
    import 순서에 따라 .env가 아직 안 실린 상태의 값이 박힐 수 있다.
    """
    # 네이티브 형식이 있으면 그대로 쓴다(프로덕션 agent.env 경로).
    value = os.environ.get("DATABASE_URL")
    if value:
        return value

    # Spring 형식: jdbc: 접두사를 벗기고 자격증명을 URL에 심는다.
    jdbc = os.environ.get("SPRING_DATASOURCE_URL")
    if jdbc:
        if not jdbc.startswith("jdbc:postgresql://"):
            raise RuntimeError(
                f"SPRING_DATASOURCE_URL은 jdbc:postgresql:// 로 시작해야 합니다: {jdbc!r}"
            )
        user = os.environ.get("SPRING_DATASOURCE_USERNAME") or ""
        password = os.environ.get("SPRING_DATASOURCE_PASSWORD") or ""
        # 계정에 :나 @ 같은 URL 예약 문자가 섞여도 깨지지 않게 percent-encoding 한다.
        auth = f"{quote(user, safe='')}:{quote(password, safe='')}@" if user else ""
        rest = jdbc.removeprefix("jdbc:postgresql://")
        return f"postgresql://{auth}{rest}"

    raise RuntimeError(
        "DB 접속 정보가 없습니다. DATABASE_URL 또는 "
        "SPRING_DATASOURCE_URL(+_USERNAME/_PASSWORD)을 설정하세요."
    )


def target() -> str:
    """접속 대상을 자격증명 없이 표시한다(호스트/DB만). 점검 응답·로그에 쓴다."""
    _, _, hostpart = dsn().rpartition("@")
    return hostpart or "unknown"


def connect():
    """커넥션을 연다. 호출자가 with로 닫는다."""
    return psycopg.connect(dsn(), connect_timeout=_CONNECT_TIMEOUT)
