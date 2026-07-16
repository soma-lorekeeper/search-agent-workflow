"""이미 인덱싱된 MySQL entities/facts를 Neo4j 그래프로 옮긴다.
LLM을 다시 호출하지 않고 기존 추출 결과를 재사용하므로 추가 비용이 없다."""

from src import graphdb, mysqldb


def migrate() -> None:
    graphdb.init_schema()

    conn = mysqldb.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, type, episode_introduced, description FROM entities"
            )
            entities = cur.fetchall()

            cur.execute(
                "SELECT f.predicate, f.episode, f.object_text, f.valid_from_episode, "
                "       f.valid_until_episode, f.note, "
                "       s.name AS subject_name, s.type AS subject_type, "
                "       o.name AS object_name, o.type AS object_type "
                "FROM facts f "
                "JOIN entities s ON f.subject_id = s.id "
                "LEFT JOIN entities o ON f.object_id = o.id"
            )
            facts = cur.fetchall()
    finally:
        conn.close()

    for e in entities:
        graphdb.upsert_entity(
            e["name"], e["type"], e["episode_introduced"], e["description"] or ""
        )
    print(f"엔티티 {len(entities)}개 마이그레이션 완료")

    for f in facts:
        graphdb.add_fact(
            subject_name=f["subject_name"],
            subject_type=f["subject_type"],
            predicate=f["predicate"],
            episode=f["episode"],
            object_name=f["object_name"],
            object_type=f["object_type"],
            object_text=f["object_text"],
            valid_from_episode=f["valid_from_episode"],
            valid_until_episode=f["valid_until_episode"],
            note=f["note"] or "",
        )
    print(f"사실(Fact) {len(facts)}개 마이그레이션 완료")


if __name__ == "__main__":
    migrate()
