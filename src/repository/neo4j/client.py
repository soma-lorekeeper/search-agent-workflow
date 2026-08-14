"""Neo4j 드라이버와 데이터베이스 이름의 단일 출처.

환경변수(.env)는 src.config가 import 시점에 이미 로드해 둔다 — 여기서 다시 부르지
않는다. 두 곳에서 부르면 어느 .env가 이겼는지가 import 순서에 달리게 된다.
"""

import os

from neo4j import Driver, GraphDatabase

# 접속할 데이터베이스. Community 에디션은 표준 DB가 하나뿐이라 사실상 "neo4j" 고정이지만,
# 로컬에서 다른 이름으로 띄우는 경우가 있어 환경변수로 열어 둔다.
# 예전에는 인덱싱과 검색이 각자 이 상수를 정의해 두 벌이 있었다 — 한쪽만 바꾸면
# 쓰는 DB가 갈라지므로 여기 한 곳으로 모은다.
DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")


def get_driver() -> Driver:
    """Neo4j 드라이버를 만들고 연결을 검증한 뒤 돌려준다.

    연결 실패 시 ServiceUnavailable 예외가 난다. 호출자는 쓰고 나면 close()한다.
    """
    uri = os.environ["NEO4J_URI"]
    user = os.environ["NEO4J_USER"]
    password = os.environ["NEO4J_PASSWORD"]

    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    return driver
