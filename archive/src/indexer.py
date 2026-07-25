import re

from src import graphdb, mysqldb
from src.config import DATA_DIR
from src.llm_extractor import extract_episode


def _guess_title(text: str, episode_num: int) -> str:
    first_line = text.strip().splitlines()[0] if text.strip() else ""
    m = re.match(r"\[?\s*\d+\s*화\s*\]?\s*(.*)", first_line)
    if m and m.group(1).strip():
        return m.group(1).strip()
    return f"{episode_num}화"


def index_episode(episode_num: int, path) -> dict:
    text = path.read_text(encoding="utf-8")

    known_entities = graphdb.list_entity_names()
    extracted = extract_episode(episode_num, text, known_entities)

    mysqldb.upsert_episode(
        episode=episode_num,
        title=_guess_title(text, episode_num),
        raw_text=text,
        summary=extracted.get("summary", ""),
    )

    entity_type_by_name: dict[str, str] = {}
    for entity in extracted.get("entities", []):
        name = entity["name"]
        etype = entity.get("type", "character")
        entity_type_by_name[name] = etype
        graphdb.upsert_entity(
            name=name,
            entity_type=etype,
            episode_introduced=episode_num,
            description=entity.get("description", ""),
        )

    n_facts = 0
    for fact in extracted.get("facts", []):
        subject_name = fact.get("subject")
        if not subject_name:
            continue
        subject_type = entity_type_by_name.get(subject_name)
        if subject_type is None:
            subject_type = "character"
            graphdb.upsert_entity(subject_name, subject_type, episode_num, "")
            entity_type_by_name[subject_name] = subject_type

        object_name = fact.get("object")
        object_type = None
        if object_name:
            object_type = entity_type_by_name.get(object_name)
            if object_type is None:
                object_type = "character"
                graphdb.upsert_entity(object_name, object_type, episode_num, "")
                entity_type_by_name[object_name] = object_type

        graphdb.add_fact(
            subject_name=subject_name,
            subject_type=subject_type,
            predicate=fact.get("predicate", "related_to"),
            episode=episode_num,
            object_name=object_name,
            object_type=object_type,
            object_text=fact.get("object_text"),
            valid_from_episode=fact.get("valid_from_episode") or episode_num,
            valid_until_episode=fact.get("valid_until_episode"),
            note=fact.get("note", ""),
        )
        n_facts += 1

    return {
        "episode": episode_num,
        "entities": len(extracted.get("entities", [])),
        "facts": n_facts,
    }


def run_indexing(episodes: range = range(1, 7)) -> None:
    graphdb.init_schema()
    mysqldb.init_schema()
    for ep in episodes:
        path = DATA_DIR / f"episode{ep}.txt"
        result = index_episode(ep, path)
        print(f"[episode {ep}] entities={result['entities']} facts={result['facts']}")


if __name__ == "__main__":
    run_indexing()
