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


def upsert_episode(episode: int, title: str, raw_text: str, summary: str) -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO episodes (episode, title, raw_text, summary) VALUES (%s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE title=VALUES(title), raw_text=VALUES(raw_text), "
                "summary=VALUES(summary)",
                (episode, title, raw_text, summary),
            )
    finally:
        conn.close()


def list_episodes() -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT episode, title, indexed_at FROM episodes ORDER BY episode")
            return cur.fetchall()
    finally:
        conn.close()


def get_all_episode_summaries() -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT episode, title, summary FROM episodes ORDER BY episode")
            return cur.fetchall()
    finally:
        conn.close()
