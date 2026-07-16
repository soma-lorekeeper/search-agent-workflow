import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from src.config import QDRANT_COLLECTION, QDRANT_URL
from src.embeddings import embed_text, embed_texts

_client = QdrantClient(url=QDRANT_URL)

EMBEDDING_DIM = 1536  # text-embedding-3-small


def ensure_collection() -> None:
    if _client.collection_exists(QDRANT_COLLECTION):
        return
    _client.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
    )


def upsert_chunks(episode: int, chunks: list[str]) -> int:
    if not chunks:
        return 0
    vectors = embed_texts(chunks)
    points = [
        PointStruct(
            id=str(uuid.uuid4()),
            vector=vector,
            payload={"episode": episode, "text": chunk},
        )
        for chunk, vector in zip(chunks, vectors)
    ]
    _client.upsert(collection_name=QDRANT_COLLECTION, points=points)
    return len(points)


def search(query: str, top_k: int = 5) -> list[dict]:
    vector = embed_text(query)
    hits = _client.query_points(
        collection_name=QDRANT_COLLECTION, query=vector, limit=top_k
    ).points
    return [
        {"episode": hit.payload["episode"], "text": hit.payload["text"], "score": hit.score}
        for hit in hits
    ]
