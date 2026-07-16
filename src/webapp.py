from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src.agent import ask
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


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    answer = ask(req.message, thread_id=req.thread_id)
    return ChatResponse(answer=answer)
