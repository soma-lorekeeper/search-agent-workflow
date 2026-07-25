from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from src import config  # noqa: F401 — import 시점에 .env를 로드해 NEO4J_*/OPENAI_API_KEY를 환경변수로 채운다

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

app = FastAPI()

# NOTE: 예전 구현(archive/src/agent.py, archive/src/contradiction_check.py)의
# POST /chat, POST /check_episode 는 lorekeeper 스키마/리트리버 기준으로 다시 짜야 해서
# 아직 없다. 지금은 archive에서 그대로 가져온 정적 mock 페이지만 서빙한다.


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
