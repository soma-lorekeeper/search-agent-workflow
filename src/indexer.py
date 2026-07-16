import re

from src import mysqldb, vectordb
from src.config import DATA_DIR
from src.llm_extractor import extract_episode

CHUNK_TARGET_CHARS = 1200


def chunk_text(text: str, target_chars: int = CHUNK_TARGET_CHARS) -> list[str]:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    chunks: list[str] = []
    buffer = ""
    for para in paragraphs:
        if buffer and len(buffer) + len(para) > target_chars:
            chunks.append(buffer.strip())
            buffer = para
        else:
            buffer = f"{buffer}\n\n{para}" if buffer else para
    if buffer.strip():
        chunks.append(buffer.strip())
    return chunks


def index_episode(episode_num: int, path) -> dict:
    text = path.read_text(encoding="utf-8")

    chunks = chunk_text(text)
    n_chunks = vectordb.upsert_chunks(episode_num, chunks)

    known_entities = mysqldb.list_entity_names()
    extracted = extract_episode(episode_num, text, known_entities)

    mysqldb.upsert_episode_summary(episode_num, extracted.get("summary", ""))

    entity_type_by_name = {}
    for entity in extracted.get("entities", []):
        name = entity["name"]
        etype = entity.get("type", "character")
        entity_type_by_name[name] = etype
        mysqldb.get_or_create_entity(
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
        subject_type = entity_type_by_name.get(subject_name, "character")
        subject_id = mysqldb.get_or_create_entity(
            name=subject_name, entity_type=subject_type, episode_introduced=episode_num
        )

        object_id = None
        object_name = fact.get("object")
        if object_name:
            object_type = entity_type_by_name.get(object_name, "character")
            object_id = mysqldb.get_or_create_entity(
                name=object_name, entity_type=object_type, episode_introduced=episode_num
            )

        mysqldb.insert_fact(
            subject_id=subject_id,
            predicate=fact.get("predicate", "related_to"),
            episode=episode_num,
            object_id=object_id,
            object_text=fact.get("object_text"),
            valid_from_episode=fact.get("valid_from_episode") or episode_num,
            valid_until_episode=fact.get("valid_until_episode"),
            note=fact.get("note", ""),
        )
        n_facts += 1

    return {
        "episode": episode_num,
        "chunks": n_chunks,
        "entities": len(extracted.get("entities", [])),
        "facts": n_facts,
    }


def run_indexing(episodes: range = range(1, 7)) -> None:
    vectordb.ensure_collection()
    mysqldb.init_schema()
    for ep in episodes:
        path = DATA_DIR / f"episode{ep}.txt"
        result = index_episode(ep, path)
        print(
            f"[episode {ep}] chunks={result['chunks']} "
            f"entities={result['entities']} facts={result['facts']}"
        )


if __name__ == "__main__":
    run_indexing()
