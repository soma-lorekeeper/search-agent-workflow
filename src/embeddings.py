from openai import OpenAI

from src.config import OPENAI_API_KEY, OPENAI_EMBEDDING_MODEL

_client = OpenAI(api_key=OPENAI_API_KEY)


def embed_texts(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    response = _client.embeddings.create(model=OPENAI_EMBEDDING_MODEL, input=texts)
    return [item.embedding for item in response.data]


def embed_text(text: str) -> list[float]:
    return embed_texts([text])[0]
