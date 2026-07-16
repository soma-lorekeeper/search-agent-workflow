from neo4j import GraphDatabase
from neo4j import READ_ACCESS

from src.config import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER

# entities.type -> Neo4j 노드 라벨
ENTITY_LABELS = {
    "character": "Character",
    "item": "Item",
    "location": "Location",
    "faction": "Faction",
    "skill": "Skill",
}

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return _driver


def label_for_type(entity_type: str) -> str:
    return ENTITY_LABELS.get(entity_type, "Entity")


def init_schema() -> None:
    driver = get_driver()
    with driver.session() as session:
        for label in {*ENTITY_LABELS.values(), "Entity"}:
            session.run(
                f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.name IS UNIQUE"
            )


def upsert_entity(
    name: str, entity_type: str, episode_introduced: int | None, description: str
) -> None:
    label = label_for_type(entity_type)
    driver = get_driver()
    with driver.session() as session:
        session.run(
            f"MERGE (n:{label} {{name: $name}}) "
            "SET n.episode_introduced = $episode_introduced, n.description = $description",
            name=name,
            episode_introduced=episode_introduced,
            description=description,
        )


def add_fact(
    subject_name: str,
    subject_type: str,
    predicate: str,
    episode: int,
    object_name: str | None = None,
    object_type: str | None = None,
    object_text: str | None = None,
    valid_from_episode: int | None = None,
    valid_until_episode: int | None = None,
    note: str = "",
) -> None:
    """사실 하나를 (주어)-[:HAS_FACT]->(Fact)-[:ABOUT]->(목적어) 형태로 저장한다.
    목적어가 엔티티가 아니라 자유 서술(object_text)이면 ABOUT 관계 없이 Fact 노드에만 남긴다."""
    subject_label = label_for_type(subject_type)
    driver = get_driver()
    with driver.session() as session:
        if object_name:
            object_label = label_for_type(object_type or "")
            session.run(
                f"MATCH (s:{subject_label} {{name: $subject_name}}) "
                f"MATCH (o:{object_label} {{name: $object_name}}) "
                "CREATE (s)-[:HAS_FACT]->(f:Fact {predicate: $predicate, episode: $episode, "
                "valid_from_episode: $valid_from_episode, valid_until_episode: $valid_until_episode, "
                "note: $note}) "
                "CREATE (f)-[:ABOUT]->(o)",
                subject_name=subject_name,
                object_name=object_name,
                predicate=predicate,
                episode=episode,
                valid_from_episode=valid_from_episode,
                valid_until_episode=valid_until_episode,
                note=note,
            )
        else:
            session.run(
                f"MATCH (s:{subject_label} {{name: $subject_name}}) "
                "CREATE (s)-[:HAS_FACT]->(f:Fact {predicate: $predicate, object_text: $object_text, "
                "episode: $episode, valid_from_episode: $valid_from_episode, "
                "valid_until_episode: $valid_until_episode, note: $note})",
                subject_name=subject_name,
                predicate=predicate,
                object_text=object_text,
                episode=episode,
                valid_from_episode=valid_from_episode,
                valid_until_episode=valid_until_episode,
                note=note,
            )


class UnsafeCypherError(Exception):
    pass


_FORBIDDEN = (
    "create",
    "merge",
    "set ",
    "delete",
    "detach",
    "remove",
    "drop",
    "call ",
    "load csv",
    "foreach",
)


def run_cypher(query: str, max_rows: int = 100) -> list[dict]:
    """읽기 전용 Cypher만 실행 (Text2Cypher 도구용 안전장치)."""
    lowered = query.lower()
    if any(kw in lowered for kw in _FORBIDDEN):
        raise UnsafeCypherError("허용되지 않는 Cypher 구문이 포함되어 있습니다 (읽기 전용만 허용).")
    driver = get_driver()
    with driver.session(default_access_mode=READ_ACCESS) as session:
        result = session.run(query)
        rows = [record.data() for record in result]
        return rows[:max_rows]
