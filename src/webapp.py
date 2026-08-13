import asyncio
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
from src.chat import run_chat
from src.chat.kg_scope import kg_scope
from src.config import DATA_DIR
from src.contradiction import save_report_files
from src.contradiction.pipeline import check_new_episode_streaming
from src.health import collect as collect_health

logger = logging.getLogger("webapp")

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
DATA_DIR.mkdir(exist_ok=True)

# ---------- 인덱싱 작업 큐 ----------
# 작업 id(job_id)는 이 서버가 만들지 않는다 — API 서버(Spring)가 발급해서 넘겨준다.
# 이 서버는 "Spring이 준 id로 일하는 워커"일 뿐이고, 작업의 정체성과 결과의 진실의 원천은
# Spring(PostgreSQL) 쪽에 있다. 그래야 응답이 유실되거나 이 서버가 재시작해도 Spring이
# 자기가 발급한 id로 다시 물어볼 수 있다(모르는 id면 404 — 아래 조회 API 참고).
#
# 처리는 요청 커넥션과 완전히 분리된 백그라운드 워커가 한다 — 요청한 쪽이 끊겨도,
# 애초에 기다리는 클라이언트가 없어도 서버 프로세스가 떠 있는 한 계속 진행된다.
# PriorityQueue를 화 번호로 정렬해서, 여러 화를 한꺼번에 큐에 넣어도(예: 4화를 2화보다
# 먼저 요청해도) lorekeeper가 요구하는 오름차순 순차 인덱싱이 지켜지게 한다.
#
# 큐와 워커는 작품 구분 없이 전역이다. 지금 KG에는 작품 격리가 아예 없어서(kg_scope 참고)
# 그래프 전체가 "인덱싱된 작품 하나"이고, 여러 작품을 동시에 인덱싱하는 상황 자체가 없다.
# 여기서 work_id별로 큐를 쪼개면 없는 격리를 있는 척하게 될 뿐이다 — KG가 work_id를 갖게
# 되는 시점에 큐/워커를 작품별로 나눈다.

_index_jobs: dict[str, dict] = {}  # job_id -> {"status": "queued"|"running"|"done"|"error", ...}
_index_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()


async def _index_worker() -> None:
    """서버가 떠 있는 동안 계속 도는 단일 워커. 큐에서 화 번호가 가장 작은 것부터 하나씩
    꺼내 순차 인덱싱한다. 동시에 여러 화를 처리하지 않는다(lorekeeper의 누적 컨텍스트가
    이전 화 완료를 전제하므로 순차 처리 필수)."""
    while True:
        episode_number, job_id, work_id, text = await _index_queue.get()
        _index_jobs[job_id] = {"status": "running"}
        try:
            # KG의 작품 범위를 아는 유일한 지점. 지금은 격리가 없어 빈 필터를 돌려주고 경고만
            # 남기지만, work_id를 여기서 반드시 통과시켜야 격리가 생겼을 때 고칠 곳이 하나로 남는다.
            kg_scope(work_id)
            result = await run_indexing(episode_number, text)
            _index_jobs[job_id] = {"status": "done", "result": result}
        except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 호출자가 보여줄 수 있다
            logger.exception("인덱싱 실패 | job_id=%s episode=%s", job_id, episode_number)
            _index_jobs[job_id] = {"status": "error", "detail": str(exc)}
        finally:
            _index_queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_index_worker())
    yield


app = FastAPI(lifespan=lifespan)

# NOTE: 이 서버의 API는 전부 API 서버(Spring)가 호출하는 내부 API다. 작업형 API(인덱싱·설정
# 오류 탐지)는 "job_id는 Spring이 발급하고 이 서버는 그 id로 일한다"는 한 가지 규칙을 공유한다.
# static/ 아래 페이지들은 그 위에 얹힌 개발용 데모지 제품 화면이 아니다.


@app.get("/api/health")
def health() -> dict:
    """이 서버가 두 DB 에 실제로 닿는지 점검한다.

    API 서버(Spring)가 이걸 호출해 자기 점검 결과와 합쳐 프론트에 내려준다.
    프로덕션에서 이 서버는 127.0.0.1 에만 떠 있어 외부에서 직접 부를 수 없다.

    DB 가 죽어도 HTTP 200 을 준다 — 상태는 본문의 status 로 구분한다. 여기서 5xx 를
    내면 "에이전트가 죽음"과 "에이전트는 살아있고 DB 만 죽음"을 호출자가 구분하지 못한다.
    """
    return collect_health()


@app.get("/")
def upload_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "upload.html")


@app.get("/library")
def library_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "library.html")


# ---------- 인덱싱 API ----------


class IndexRequest(BaseModel):
    # job_id는 호출자(Spring)가 만들어 넘긴다 — 이 서버는 그 id로 일할 뿐이다.
    # work_id는 지금 KG에서 실제 필터로 쓰이지 않지만(kg_scope 참고) 인터페이스에는 처음부터
    # 둔다. 나중에 격리가 생겨도 API 계약을 다시 깨지 않기 위해서다.
    job_id: str
    work_id: int
    episode_number: int
    text: str


class JobAck(BaseModel):
    job_id: str
    status: str


@app.post("/api/index", response_model=JobAck, status_code=202)
async def index_episode(req: IndexRequest) -> JobAck:
    """인덱싱을 큐에 넣고 즉시 응답한다. 실제 처리(lorekeeper.indexing 호출)는
    _index_worker가 이 요청의 커넥션과 무관하게 백그라운드에서 진행한다.
    lorekeeper는 Chunk 단위로만 원문을 보관하므로, "원고 목록" 뷰어에서 보여줄 원문
    전체는 우리 쪽에서 별도로 data/episode{N}.txt에 저장해 둔다(이건 큐잉 전에 바로 처리)."""
    known = _index_jobs.get(req.job_id)
    if known is not None:
        # 같은 job_id로 다시 들어온 요청은 새 작업이 아니라 호출자의 재시도(응답 유실·타임아웃)로
        # 본다. 다시 큐에 넣으면 같은 화를 두 번 인덱싱해 그래프가 오염되고 LLM 비용도 두 배다.
        return JobAck(job_id=req.job_id, status=known["status"])

    (DATA_DIR / f"episode{req.episode_number}.txt").write_text(req.text, encoding="utf-8")
    _index_jobs[req.job_id] = {"status": "queued"}
    # 정렬 키는 화 번호가 먼저다(오름차순 순차 인덱싱). 뒤의 job_id는 같은 화가 두 번
    # 들어왔을 때의 타이브레이커 — 없으면 우선순위 비교가 원고 본문끼리의 문자열 비교로 넘어간다.
    await _index_queue.put((req.episode_number, req.job_id, req.work_id, req.text))
    return JobAck(job_id=req.job_id, status="queued")


class IndexStatus(BaseModel):
    job_id: str
    status: str
    detail: str | None = None
    result: dict | None = None


@app.get("/api/index/{job_id}", response_model=IndexStatus)
def get_index_job(job_id: str) -> IndexStatus:
    """작업 하나의 진행 상태. 서버 프로세스 메모리에만 있으므로 재시작하면 사라진다 —
    그래도 되는 건 이게 "작업 중 상태"일 뿐 진실이 아니기 때문이다. 무엇이 실제로 인덱싱됐는지의
    정답은 Neo4j(/api/episodes)이고, 작업 이력의 정답은 API 서버의 DB다.

    모르는 job_id는 404다. 그래야 호출자가 "접수된 적 없음/재시작으로 유실됨"과
    "아직 처리 중"을 구분할 수 있다(빈 상태를 200으로 주면 영원히 기다리게 된다).
    """
    state = _index_jobs.get(job_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"'{job_id}' 인덱싱 작업 기록이 없습니다.")
    return IndexStatus(
        job_id=job_id,
        status=state["status"],
        detail=state.get("detail"),
        result=state.get("result"),
    )


# ---------- 설정 오류 탐지 API ----------
# 인덱싱과 달리 회차 순서를 지킬 필요가 없다(검사 대상 회차 하나를 그 시점의 그래프와
# 대조할 뿐, 검사끼리 서로 의존하지 않는다). 그래서 큐 없이 요청마다
# asyncio.create_task로 바로 백그라운드에 띄운다 — 요청 커넥션과 무관하게 계속 진행되는
# 성질은 인덱싱 워커와 동일하다.

_detect_jobs: dict[str, dict] = {}  # job_id -> {"status": ..., "claims": [...], "findings": [...]}


async def _run_detect(job_id: str, work_id: int, episode_number: int, text: str) -> None:
    def on_claims_extracted(claims: list[dict]) -> None:
        # claim 추출 직후 시점 — 호출자가 이 시점부터 claim별 진행 목록을 그릴 수 있게 한다.
        _detect_jobs[job_id]["claims"] = [
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
        entry = _detect_jobs[job_id]["claims"][index]
        entry.update(
            {
                "status": "done",
                "label": result.get("label"),
                "established_fact": result.get("established_fact"),
                "source_episode": result.get("source_episode"),
                "explanation": result.get("explanation"),
            }
        )

    _detect_jobs[job_id]["status"] = "running"
    try:
        # 인덱싱 워커와 같은 이유로 여기서도 kg_scope를 통과시킨다 — 파이프라인이 보는 그래프의
        # 작품 범위를 정하는 지점은 이 프로젝트에서 kg_scope 하나뿐이어야 한다.
        kg_scope(work_id)
        findings = await check_new_episode_streaming(text, on_claims_extracted, on_claim_done)
        save_report_files(findings, job_id, display_label=f"{episode_number}화")
        _detect_jobs[job_id]["status"] = "done"
        _detect_jobs[job_id]["findings"] = findings
    except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 호출자가 보여줄 수 있다
        logger.exception("설정 오류 탐지 실패 | job_id=%s episode=%s", job_id, episode_number)
        _detect_jobs[job_id]["status"] = "error"
        _detect_jobs[job_id]["detail"] = str(exc)


class DetectRequest(BaseModel):
    job_id: str
    work_id: int
    episode_number: int
    text: str


@app.post("/api/detect", response_model=JobAck, status_code=202)
async def start_detect(req: DetectRequest) -> JobAck:
    """설정 오류 탐지를 백그라운드로 시작하고 즉시 응답한다. 실제 검사(claim 추출 → claim별
    병렬 검증 → 판정 집계)는 이 요청의 커넥션과 무관하게 진행된다."""
    known = _detect_jobs.get(req.job_id)
    if known is not None:
        # 인덱싱과 같은 이유(재시도 방어). 여기선 그래프가 더러워지진 않지만 회차 하나 검사에
        # 수십 번의 LLM 호출이 들어가므로 중복 실행 비용이 특히 크다.
        return JobAck(job_id=req.job_id, status=known["status"])

    _detect_jobs[req.job_id] = {"status": "queued", "claims": []}
    asyncio.create_task(_run_detect(req.job_id, req.work_id, req.episode_number, req.text))
    return JobAck(job_id=req.job_id, status="queued")


class DetectStatus(BaseModel):
    job_id: str
    status: str
    detail: str | None = None
    # claims: 진행 상황(검사 중에도 채워진다). findings: 최종 판정 결과(status=done일 때만).
    # 필드 이름은 파이프라인이 만들어내는 그대로다 — 여기서 바꾸면 이름만 다른 같은 값이 둘이 된다.
    claims: list[dict] | None = None
    findings: list[dict] | None = None


@app.get("/api/detect/{job_id}", response_model=DetectStatus)
def get_detect_job(job_id: str) -> DetectStatus:
    """검사 하나의 진행 상태와 결과. 인덱싱 조회와 마찬가지로 모르는 job_id는 404다."""
    state = _detect_jobs.get(job_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"'{job_id}' 탐지 작업 기록이 없습니다.")
    return DetectStatus(
        job_id=job_id,
        status=state["status"],
        detail=state.get("detail"),
        claims=state.get("claims"),
        findings=state.get("findings"),
    )


# ---------- AI 채팅 API ----------
# 인덱싱/검사와 달리 백그라운드 작업이 아니다 — 작가가 답을 기다리고 있으므로 이 요청 안에서
# 끝까지 처리해 JSON으로 한 번에 돌려준다(스트리밍 아님). 대화 기록은 서버 메모리에 남기지
# 않는다: 진실의 원천은 API 서버(Spring)의 chat_messages 테이블이고, 여기는 매 턴 통째로
# 받아서 답만 만들어 주는 무상태 계산기다(그래야 이 서버가 재시작해도 대화가 안 끊긴다).


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatContext(BaseModel):
    # 작가가 "이번 화"라고 말할 때의 기준점. 편집기를 안 열고 대화만 하는 경우도 있어 없을 수 있다.
    current_episode_number: int | None = None


class ChatRequest(BaseModel):
    work_id: int
    session_id: int
    messages: list[ChatMessage]
    context: ChatContext = ChatContext()


class ChatToolCall(BaseModel):
    name: str
    summary: str
    status: str


class ChatResponse(BaseModel):
    content: str
    tool_calls: list[ChatToolCall] = []
    suggested_title: str | None = None


@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    """작가의 질문 하나에 답한다.

    에이전트가 KG(인물 상태·사건)와 원고 DB를 직접 조회해서 답을 만든다. 어떤 도구를 몇 번
    부를지는 질문마다 다르므로 모델이 스스로 고른다 — 그 내역을 tool_calls로 함께 내려보내
    프론트가 "무엇을 찾아봤는지"를 보여줄 수 있게 한다(근거 없는 답변처럼 보이지 않게 하는 게
    이 필드의 목적이다).

    suggested_title은 대화 첫 턴에만 채워진다. 세션 제목을 저장할지 말지는 API 서버가 정한다.
    """
    result = await run_chat(
        work_id=req.work_id,
        session_id=req.session_id,
        messages=[m.model_dump() for m in req.messages],
        context=req.context.model_dump(),
    )
    return ChatResponse(**result)


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
