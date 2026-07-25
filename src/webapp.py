from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from lorekeeper.client import get_driver

# lorekeeper/__init__.py가 `indexing`이라는 이름을 함수(async def indexing(...))로 재노출해서
# `from lorekeeper import indexing`은 함수를 가리킨다. DATABASE 상수는 서브모듈 경로로
# 직접 가져와야 한다(패키지 네임스페이스의 재바인딩을 건너뛴다).
from lorekeeper.indexing import DATABASE as LOREKEEPER_DATABASE
from lorekeeper.indexing import indexing as run_indexing

from src import config  # noqa: F401 — import 시점에 .env를 로드해 NEO4J_*/OPENAI_API_KEY를 환경변수로 채운다
from src.config import DATA_DIR

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
DATA_DIR.mkdir(exist_ok=True)

app = FastAPI()

# NOTE: 예전 구현(archive/src/agent.py, archive/src/contradiction_check.py)의
# Q&A/설정오류탐지 API는 lorekeeper 스키마/리트리버 기준으로 다시 짜야 해서 아직 없다.
# 지금 연결된 건 인덱싱(원고 접수)과 원고 목록/뷰어 두 기능뿐이다.


@app.get("/")
def upload_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "upload.html")


@app.get("/library")
def library_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "library.html")


@app.get("/report")
def report_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "report.html")


@app.get("/chat")
def chat_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "chat.html")


# ---------- 인덱싱 API ----------


class IndexRequest(BaseModel):
    chapter: int
    text: str


class IndexResponse(BaseModel):
    chapter: int
    labels: dict[str, int]
    rels: dict[str, int]
    tokens: dict[str, int]
    summary: str


@app.post("/api/index", response_model=IndexResponse)
async def index_chapter(req: IndexRequest) -> IndexResponse:
    """lorekeeper.indexing()을 호출해 한 회차를 누적 인덱싱한다.
    lorekeeper는 Chunk 단위(문장 몇 개 분량)로만 원문을 보관하므로, "원고 목록" 뷰어에서
    보여줄 원문 전체는 우리 쪽에서 별도로 data/episode{N}.txt에 저장해 둔다."""
    (DATA_DIR / f"episode{req.chapter}.txt").write_text(req.text, encoding="utf-8")
    result = await run_indexing(req.chapter, req.text)
    return IndexResponse(**result)


# ---------- 원고 목록/뷰어 API ----------


class EpisodeSummary(BaseModel):
    chapter: int
    summary: str
    chars: int


class EpisodeDetail(BaseModel):
    chapter: int
    summary: str
    raw_text: str
    chars: int


def _episode_chars(chapter: int) -> int:
    path = DATA_DIR / f"episode{chapter}.txt"
    return len(path.read_text(encoding="utf-8")) if path.exists() else 0


@app.get("/api/episodes", response_model=list[EpisodeSummary])
def list_episodes() -> list[EpisodeSummary]:
    driver = get_driver()
    try:
        records, _, _ = driver.execute_query(
            "MATCH (c:Chapter) RETURN c.number AS chapter, c.summary AS summary ORDER BY c.number",
            database_=LOREKEEPER_DATABASE,
        )
    finally:
        driver.close()
    return [
        EpisodeSummary(
            chapter=r["chapter"],
            summary=r["summary"] or "",
            chars=_episode_chars(r["chapter"]),
        )
        for r in records
    ]


@app.get("/api/episodes/{chapter}", response_model=EpisodeDetail)
def get_episode(chapter: int) -> EpisodeDetail:
    driver = get_driver()
    try:
        records, _, _ = driver.execute_query(
            "MATCH (c:Chapter {number: $chapter}) RETURN c.summary AS summary",
            {"chapter": chapter},
            database_=LOREKEEPER_DATABASE,
        )
    finally:
        driver.close()
    if not records:
        raise HTTPException(status_code=404, detail=f"{chapter}화는 아직 접수되지 않았습니다.")
    path = DATA_DIR / f"episode{chapter}.txt"
    raw_text = path.read_text(encoding="utf-8") if path.exists() else ""
    return EpisodeDetail(
        chapter=chapter,
        summary=records[0]["summary"] or "",
        raw_text=raw_text,
        chars=len(raw_text),
    )
