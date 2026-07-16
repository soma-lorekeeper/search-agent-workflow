from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src.agent import ask
from src.contradiction_check import check_new_episode, generate_report
from src.logging_config import setup_logging

setup_logging()

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

app = FastAPI()


class ChatRequest(BaseModel):
    message: str
    thread_id: str = "default"


class ChatResponse(BaseModel):
    answer: str


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/check")
def check_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "check.html")


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    answer = ask(req.message, thread_id=req.thread_id)
    return ChatResponse(answer=answer)


class ContradictionCheckRequest(BaseModel):
    text: str
    episode_label: str = "신규 회차"


class ContradictionCheckResponse(BaseModel):
    report_markdown: str
    results: list[dict]


@app.post("/check_episode", response_model=ContradictionCheckResponse)
def check_episode(req: ContradictionCheckRequest) -> ContradictionCheckResponse:
    results = check_new_episode(req.text)
    report = generate_report(results, req.episode_label)
    return ContradictionCheckResponse(report_markdown=report, results=results)
