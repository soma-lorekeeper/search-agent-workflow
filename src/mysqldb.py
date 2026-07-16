from pathlib import Path

import pymysql
import pymysql.cursors

from src.config import MYSQL_DATABASE, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USER

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def init_schema() -> None:
    statements = SCHEMA_PATH.read_text().split(";")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for statement in statements:
                statement = statement.strip()
                if statement:
                    cur.execute(statement)
    finally:
        conn.close()


def get_or_create_entity(name: str, entity_type: str, episode_introduced: int, description: str = "") -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM entities WHERE name=%s AND type=%s", (name, entity_type)
            )
            row = cur.fetchone()
            if row:
                return row["id"]
            cur.execute(
                "INSERT INTO entities (name, type, episode_introduced, description) "
                "VALUES (%s, %s, %s, %s)",
                (name, entity_type, episode_introduced, description),
            )
            return cur.lastrowid
    finally:
        conn.close()


def insert_fact(
    subject_id: int,
    predicate: str,
    episode: int,
    object_id: int | None = None,
    object_text: str | None = None,
    valid_from_episode: int | None = None,
    valid_until_episode: int | None = None,
    note: str = "",
) -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO facts "
                "(subject_id, predicate, object_id, object_text, episode, "
                " valid_from_episode, valid_until_episode, note) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    subject_id,
                    predicate,
                    object_id,
                    object_text,
                    episode,
                    valid_from_episode,
                    valid_until_episode,
                    note,
                ),
            )
            return cur.lastrowid
    finally:
        conn.close()


def upsert_episode_summary(episode: int, summary: str) -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO episode_summaries (episode, summary) VALUES (%s, %s) "
                "ON DUPLICATE KEY UPDATE summary=VALUES(summary)",
                (episode, summary),
            )
    finally:
        conn.close()


def list_entity_names(limit: int = 200) -> list[str]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name FROM entities ORDER BY id DESC LIMIT %s", (limit,)
            )
            return [row["name"] for row in cur.fetchall()]
    finally:
        conn.close()


class UnsafeSQLError(Exception):
    pass


def run_select(sql: str, max_rows: int = 100) -> list[dict]:
    """읽기 전용 SELECT만 실행 (Text2SQL 도구용 안전장치)."""
    normalized = sql.strip().rstrip(";")
    if not normalized.lower().startswith("select"):
        raise UnsafeSQLError("SELECT 문만 실행할 수 있습니다.")
    forbidden = ("insert", "update", "delete", "drop", "alter", "truncate", "create", ";")
    lowered = normalized.lower()
    if any(word in lowered for word in forbidden):
        raise UnsafeSQLError("허용되지 않는 SQL 구문이 포함되어 있습니다.")
    if "limit" not in lowered:
        normalized = f"{normalized} LIMIT {max_rows}"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(normalized)
            return cur.fetchall()
    finally:
        conn.close()
