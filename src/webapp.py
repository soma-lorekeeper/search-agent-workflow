import asyncio
import json
import logging
from contextlib import asynccontextmanager
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
from src.contradiction import save_report_files
from src.contradiction.pipeline import REPORTS_DIR, check_new_episode_streaming

logger = logging.getLogger("webapp")

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
DATA_DIR.mkdir(exist_ok=True)

# ---------- 인덱싱 작업 큐 ----------
# 브라우저 요청/연결과 완전히 분리된 백그라운드 워커가 처리한다 — 탭을 닫아도,
# 애초에 요청을 보낸 브라우저가 없어도 서버 프로세스가 떠 있는 한 계속 진행된다.
# PriorityQueue를 화 번호로 정렬해서, 여러 화를 한꺼번에 큐에 넣어도(예: 4화를 2화보다
# 먼저 요청해도) lorekeeper가 요구하는 오름차순 순차 인덱싱이 지켜지게 한다.

_job_state: dict[int, dict] = {}  # chapter -> {"status": "queued"|"running"|"done"|"error", ...}
_job_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()


async def _index_worker() -> None:
    """서버가 떠 있는 동안 계속 도는 단일 워커. 큐에서 화 번호가 가장 작은 것부터 하나씩
    꺼내 순차 인덱싱한다. 동시에 여러 화를 처리하지 않는다(lorekeeper의 누적 컨텍스트가
    이전 화 완료를 전제하므로 순차 처리 필수)."""
    while True:
        chapter, text = await _job_queue.get()
        _job_state[chapter] = {"status": "running"}
        try:
            result = await run_indexing(chapter, text)
            _job_state[chapter] = {"status": "done", "result": result}
        except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 프론트가 보여줄 수 있다
            _job_state[chapter] = {"status": "error", "detail": str(exc)}
        finally:
            _job_queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_index_worker())
    yield


app = FastAPI(lifespan=lifespan)

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


@app.get("/reports")
def report_history_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "report_history.html")


@app.get("/chat")
def chat_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "chat.html")


# ---------- 인덱싱 API ----------


class IndexRequest(BaseModel):
    chapter: int
    text: str


class IndexAck(BaseModel):
    chapter: int
    status: str


@app.post("/api/index", response_model=IndexAck, status_code=202)
async def index_chapter(req: IndexRequest) -> IndexAck:
    """인덱싱을 큐에 넣고 즉시 응답한다. 실제 처리(lorekeeper.indexing 호출)는
    _index_worker가 이 요청의 커넥션과 무관하게 백그라운드에서 진행한다.
    lorekeeper는 Chunk 단위로만 원문을 보관하므로, "원고 목록" 뷰어에서 보여줄 원문
    전체는 우리 쪽에서 별도로 data/episode{N}.txt에 저장해 둔다(이건 큐잉 전에 바로 처리)."""
    (DATA_DIR / f"episode{req.chapter}.txt").write_text(req.text, encoding="utf-8")
    _job_state[req.chapter] = {"status": "queued"}
    await _job_queue.put((req.chapter, req.text))
    return IndexAck(chapter=req.chapter, status="queued")


class JobStatus(BaseModel):
    chapter: int
    status: str
    detail: str | None = None
    result: dict | None = None


@app.get("/api/index/status", response_model=list[JobStatus])
def index_status() -> list[JobStatus]:
    """지금 서버가 알고 있는 인덱싱 작업 상태 전체. 서버 프로세스 메모리에만 있으므로
    재시작하면 사라진다 — "실제로 뭐가 인덱싱됐는지"의 정답은 /api/episodes(Neo4j) 쪽이고,
    이건 "지금 큐/진행 상황이 어떤지"를 보여주는 용도다."""
    return [
        JobStatus(
            chapter=chapter,
            status=state["status"],
            detail=state.get("detail"),
            result=state.get("result"),
        )
        for chapter, state in sorted(_job_state.items())
    ]


# ---------- 설정 오류 리포트 API ----------
# 인덱싱과 달리 회차 순서를 지킬 필요가 없다(검사 대상 회차 하나를 그 시점의 그래프와
# 대조할 뿐, 검사끼리 서로 의존하지 않는다). 그래서 큐 없이 요청마다
# asyncio.create_task로 바로 백그라운드에 띄운다 — 브라우저 연결과 무관하게 계속 진행되는
# 성질은 인덱싱 워커와 동일하다.

_report_state: dict[str, dict] = {}  # label -> {"status": "running"|"done"|"error", "claims": [...], ...}


async def _run_report_check(label: str, text: str) -> None:
    def on_claims_extracted(claims: list[dict]) -> None:
        # claim 추출 직후 시점 — 프론트가 이 시점부터 claim별 진행 목록을 그릴 수 있게 한다.
        _report_state[label]["claims"] = [
            {
                "index": i,
                "quote": c.get("quote", ""),
                "category": c.get("category", "기타"),
                "status": "running",
                "label": None,
                "established_fact": None,
                "source_episode": None,
                "explanation": None,
            }
            for i, c in enumerate(claims)
        ]

    def on_claim_done(index: int, result: dict) -> None:
        entry = _report_state[label]["claims"][index]
        entry.update(
            {
                "status": "done",
                "label": result.get("label"),
                "established_fact": result.get("established_fact"),
                "source_episode": result.get("source_episode"),
                "explanation": result.get("explanation"),
            }
        )

    try:
        results = await check_new_episode_streaming(text, on_claims_extracted, on_claim_done)
        save_report_files(results, label)
        _report_state[label]["status"] = "done"
        _report_state[label]["results"] = results
    except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 프론트가 보여줄 수 있다
        logger.exception("설정 오류 검사 실패 | label=%s", label)
        _report_state[label]["status"] = "error"
        _report_state[label]["detail"] = str(exc)


class ReportRequest(BaseModel):
    label: str
    text: str


class ReportAck(BaseModel):
    label: str
    status: str


@app.post("/api/report", response_model=ReportAck, status_code=202)
async def start_report(req: ReportRequest) -> ReportAck:
    """설정 오류 검사를 백그라운드로 시작하고 즉시 응답한다. 실제 검사(claim 추출 → claim별
    병렬 검증 → 리포트 생성)는 이 요청의 커넥션과 무관하게 진행된다."""
    _report_state[req.label] = {"status": "running", "claims": []}
    asyncio.create_task(_run_report_check(req.label, req.text))
    return ReportAck(label=req.label, status="queued")


class ReportStatus(BaseModel):
    label: str
    status: str
    detail: str | None = None
    claims: list[dict] | None = None
    results: list[dict] | None = None


@app.get("/api/report/{label}", response_model=ReportStatus)
def get_report(label: str) -> ReportStatus:
    state = _report_state.get(label)
    if state is None:
        raise HTTPException(status_code=404, detail=f"'{label}' 검사 기록이 없습니다.")
    return ReportStatus(
        label=label,
        status=state["status"],
        detail=state.get("detail"),
        claims=state.get("claims"),
        results=state.get("results"),
    )


# ---------- 검증 히스토리 API ----------
# _report_state(위)는 서버 프로세스 메모리라 재시작하면 사라진다. 히스토리는 그와 무관하게
# reports/ 디렉터리에 저장된 *_contradiction_report.json 파일을 그대로 진실의 원천으로 삼는다
# (save_report_files가 검사 완료 시마다 label·생성시각·results를 함께 기록해둔다).


def _report_counts(results: list[dict]) -> dict[str, int]:
    counts = {"contradiction": 0, "consistent": 0, "unknown": 0}
    for r in results:
        if r.get("label") in counts:
            counts[r["label"]] += 1
    return counts


def _load_report_file(label: str) -> dict:
    path = REPORTS_DIR / f"{label}_contradiction_report.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"'{label}' 리포트가 없습니다.")
    return json.loads(path.read_text(encoding="utf-8"))


class ReportHistoryItem(BaseModel):
    label: str
    generated_at: str
    total: int
    counts: dict[str, int]


@app.get("/api/reports", response_model=list[ReportHistoryItem])
def list_reports() -> list[ReportHistoryItem]:
    if not REPORTS_DIR.exists():
        return []
    items = []
    for path in sorted(REPORTS_DIR.glob("*_contradiction_report.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        results = payload.get("results", [])
        items.append(
            ReportHistoryItem(
                label=payload.get("label", path.stem),
                generated_at=payload.get("generated_at", ""),
                total=len(results),
                counts=_report_counts(results),
            )
        )
    items.sort(key=lambda it: it.generated_at, reverse=True)
    return items


class ReportDetail(BaseModel):
    label: str
    generated_at: str
    results: list[dict]


@app.get("/api/reports/{label}", response_model=ReportDetail)
def get_saved_report(label: str) -> ReportDetail:
    payload = _load_report_file(label)
    return ReportDetail(
        label=payload.get("label", label),
        generated_at=payload.get("generated_at", ""),
        results=payload.get("results", []),
    )


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
