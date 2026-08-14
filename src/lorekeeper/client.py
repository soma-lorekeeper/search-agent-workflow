"""
Neo4j 드라이버 연결 모듈.
환경변수(.env)에서 연결 정보를 읽어 드라이버를 반환한다.
"""

import os
from neo4j import GraphDatabase, Driver
from dotenv import load_dotenv

load_dotenv()


def get_driver() -> Driver:
    """
    Neo4j 드라이버를 생성하고 연결을 검증한 뒤 반환한다.
    연결 실패 시 ServiceUnavailable 예외가 발생한다.
    """
    uri = os.environ["NEO4J_URI"]
    user = os.environ["NEO4J_USER"]
    password = os.environ["NEO4J_PASSWORD"]

    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    print(f"Neo4j 연결 성공: {uri}")
    return driver


if __name__ == "__main__":
    driver = get_driver()
    driver.close()
