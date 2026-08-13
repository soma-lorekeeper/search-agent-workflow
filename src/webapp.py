import asyncio
import logging
import math
import os
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from openai import RateLimitError
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

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
# 작업 id(jobId)는 이 서버가 UUID로 발급한다. 예전에는 Spring이 발급한 id로 일했지만,
# 이제 요청 하나가 여러 화를 묶어서 들어오고 그 묶음의 진행 상태(화별 waiting/running/
# done/error)를 관리하는 주체가 이 서버라서, 묶음의 이름도 여기서 짓는 게 맞다.
# Spring은 201 응답으로 받은 jobId로 진행 상황을 조회한다.
#
# 처리는 요청 커넥션과 완전히 분리된 백그라운드 워커가 한다 — 요청한 쪽이 끊겨도,
# 애초에 기다리는 클라이언트가 없어도 서버 프로세스가 떠 있는 한 계속 진행된다.
# 큐는 단순 FIFO다: 예전에는 화 번호로 정렬하는 PriorityQueue였지만, 이제 "오름차순"은
# 요청 자체가 보장해야 하는 조건이고(어기면 400) 한 요청 안의 화들은 받은 순서대로
# 처리하므로, 큐가 순서를 다시 정할 이유가 없다.
#
# 큐와 워커는 작품 구분 없이 전역이고 워커는 하나뿐이다. 지금 KG에는 작품 격리가 아예
# 없어서(kg_scope 참고) 그래프 전체가 "인덱싱된 작품 하나"이고, 여러 작품을 동시에
# 인덱싱하는 상황 자체가 없다. 여기서 workId별로 큐를 쪼개면 없는 격리를 있는 척하게 될
# 뿐이다 — KG가 workId를 갖게 되는 시점에 큐/워커를 작품별로 나눈다.
#
# 작업 상태는 이 프로세스 메모리에만 있다. 재시작하면 상태가 사라지고 진행 중이던 인덱싱도
# 함께 끊긴다 — 스펙이 인정하는 정상 시나리오다(Spring은 조회에서 404를 보면 다시 POST한다).
# 그렇게 다시 보내도 안전한 근거는 _already_indexed의 완료 마커다.
#
# job 한 건의 모양:
#   {"user_id": 42, "work_id": 7, "requested_at": "2026-08-11T03:11:00Z",
#    "episodes": [{"episode_id": 101, "episode_no": 6, "text": "...",
#                  "status": "waiting", "error": None}, ...]}
# text를 상태에 함께 들고 있는 건 워커가 나중에 꺼내 쓰기 때문이고, 조회 응답에는 나가지
# 않는다(응답 모델이 episodeId/status/error만 뽑는다).

_index_jobs: dict[str, dict] = {}  # job_id -> 위 모양의 dict
_index_queue: asyncio.Queue = asyncio.Queue()  # 접수된 job_id의 FIFO


async def _index_worker() -> None:
    """서버가 떠 있는 동안 계속 도는 단일 워커. 큐에서 작업을 하나씩 꺼내 그 작업의 화들을
    순서대로 인덱싱한다. 동시에 여러 화를 처리하지 않는다(lorekeeper의 누적 컨텍스트가
    이전 화 완료를 전제하므로 순차 처리 필수)."""
    while True:
        job_id = await _index_queue.get()
        try:
            await _run_index_job(job_id)
        except Exception:  # noqa: BLE001 — 워커가 죽으면 이후 모든 작업이 멈춘다
            logger.exception("인덱싱 작업 처리 중 예기치 못한 오류 | jobId=%s", job_id)
        finally:
            _index_queue.task_done()


async def _run_index_job(job_id: str) -> None:
    """작업 하나(=여러 화)를 episodeNo 오름차순으로 순차 인덱싱한다.

    한 화가 실패하면 뒤의 화는 아예 시도하지 않고 전부 error로 표시한다. 인덱싱은 이전
    화까지 누적된 그래프를 컨텍스트로 읽어서 다음 화를 해석하므로, 중간이 빈 채로 뒤를
    이어붙이면 그래프가 조용히 잘못된 상태가 된다 — 멈추는 편이 낫다.
    """
    job = _index_jobs[job_id]
    failed_no: int | None = None

    for episode in job["episodes"]:
        if episode["status"] == "done":
            # 접수 시점에 완료 마커가 확인된 화 — 다시 인덱싱하지 않는다.
            continue
        if failed_no is not None:
            episode["status"] = "error"
            episode["error"] = f"Skipped due to preceding episode ({failed_no}) failure"
            continue

        episode["status"] = "running"
        try:
            # KG의 작품 범위를 아는 유일한 지점. 지금은 격리가 없어 빈 필터를 돌려주고 경고만
            # 남기지만, workId를 여기서 반드시 통과시켜야 격리가 생겼을 때 고칠 곳이 하나로 남는다.
            # userId는 kg_scope에도 넘기지 않는다 — 그래프에는 workId조차 없어서 사용자 단위
            # 격리는 더더욱 불가능하고, 지금 넘겨봐야 무시되는 인자가 하나 늘 뿐이다. 사용자는
            # 로그로만 남겨 "누가 넣은 그래프인지" 추적할 수 있게 한다.
            kg_scope(job["work_id"])
            logger.info(
                "인덱싱 시작 | jobId=%s userId=%s workId=%s episodeId=%s episodeNo=%s",
                job_id,
                job["user_id"],
                job["work_id"],
                episode["episode_id"],
                episode["episode_no"],
            )
            await _run_indexing_with_retry(episode["episode_no"], episode["text"])
            episode["status"] = "done"
        except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 호출자가 보여줄 수 있다
            logger.exception(
                "인덱싱 실패 | jobId=%s episodeId=%s episodeNo=%s",
                job_id,
                episode["episode_id"],
                episode["episode_no"],
            )
            episode["status"] = "error"
            episode["error"] = str(exc)
            failed_no = episode["episode_no"]


# 접수 시점의 TPM 계산은 어디까지나 추정이라, 201을 받은 요청도 실제 호출에서 순간적으로
# 한도에 걸릴 수 있다. 그건 Spring에 돌려줘봐야 똑같은 요청을 다시 만들 뿐이므로 여기서
# 삼키고 재시도한다(스펙 6절: "서버가 내부적으로 백오프 재시도, Spring은 관여하지 않는다").
_RATE_LIMIT_RETRIES = 3
_RATE_LIMIT_BACKOFF_SECONDS = 20  # 20초 → 40초 → 80초. TPM 창이 60초라 한 번 쉬면 대개 회복된다.


async def _run_indexing_with_retry(episode_no: int, text: str) -> dict:
    """lorekeeper 인덱싱을 부르되, OpenAI의 rate limit(429)만 백오프 재시도한다.

    다른 예외는 재시도해도 결과가 달라지지 않으므로(원고 파싱 실패, DB 연결 끊김 등)
    그대로 올려보내 화 상태를 error로 만든다.
    """
    for attempt in range(_RATE_LIMIT_RETRIES + 1):
        try:
            return await run_indexing(episode_no, text)
        except RateLimitError:
            if attempt == _RATE_LIMIT_RETRIES:
                raise
            delay = _RATE_LIMIT_BACKOFF_SECONDS * (2**attempt)
            logger.warning(
                "OpenAI rate limit — %d초 후 재시도 | episodeNo=%s (%d/%d)",
                delay,
                episode_no,
                attempt + 1,
                _RATE_LIMIT_RETRIES,
            )
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_index_worker())
    yield


app = FastAPI(lifespan=lifespan)

# NOTE: 이 서버의 API는 전부 API 서버(Spring)가 호출하는 내부 API다. 인증은 없다 —
# 호출자가 Spring 하나뿐이고 이 서버는 127.0.0.1에만 떠 있어 외부에서 닿을 수 없다.
# 작업형 API 둘은 작업 id 발급 주체가 서로 다르다: 인덱싱은 이 서버가 jobId를 발급하고
# (요청 하나가 여러 화를 묶는 단위라서), 설정 오류 탐지는 여전히 Spring이 발급한 job_id로
# 일한다. 필드 표기도 그래서 다르다 — 인덱싱만 스펙대로 camelCase이고 나머지는 snake_case다.
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
# 요청·응답 필드는 스펙대로 camelCase다. 파이썬 쪽 필드 이름은 snake_case로 두고 alias로만
# 바꾼다 — 필드 이름 자체를 camelCase로 쓰면 이 파일 안에 이름 규칙이 두 개 생긴다.


class CamelModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


# ---------- 완료 마커 ----------
# "이 화가 인덱싱됐는가"의 진실은 이 서버의 메모리가 아니라 Neo4j다. lorekeeper 인덱싱의
# 마지막 쓰기가 Chapter-[:IN_STORY]->Story(id:'main') 관계이므로(lorekeeper-poc의
# context.update_global_summary — 전역 요약 갱신과 같은 쿼리에서 한 번에 MERGE된다),
# 이 관계가 있으면 그 화는 추출·근거링크·요약까지 전부 끝났다는 뜻이다.
#
# 그래서 접수 시점에 화마다 이 마커를 확인한다:
#   - 있으면 → 일하지 않고 즉시 done
#   - 없으면 → 처음부터 다시 인덱싱(중간까지 쓰다 만 결과 위에 다시 돌려도 Neo4jWriter가
#     전부 upsert라 같은 값으로 수렴한다)
# 재시작으로 진행 상태가 날아가 Spring이 같은 화를 다시 보내도 안전한 이유가 이것이다.
_INDEXED_MARKER_CYPHER = """
MATCH (c:Chapter)-[:IN_STORY]->(:Story {id: 'main'})
WHERE c.number IN $chapters
RETURN c.number AS chapter
"""


def _already_indexed(episode_nos: list[int]) -> set[int]:
    """완료 마커가 있는 화 번호 집합을 Neo4j에 한 번에 물어본다(요청당 쿼리 1회).

    조회 자체가 실패하면(그래프가 죽었거나 접속 불가) "모른다"가 아니라 "안 됐다"로 본다.
    실제로 인덱싱돼 있었다면 다시 돌려도 같은 결과로 수렴하지만, 반대로 판단하면 안 된 화를
    done으로 보고해 영영 빠진 화가 생긴다.
    """
    try:
        driver = get_driver()
        try:
            records, _, _ = driver.execute_query(
                _INDEXED_MARKER_CYPHER,
                {"chapters": episode_nos},
                database_=LOREKEEPER_DATABASE,
            )
        finally:
            driver.close()
    except Exception:  # noqa: BLE001 — 마커 확인 실패는 작업 거절 사유가 아니다
        logger.warning("완료 마커 확인 실패 — 전부 인덱싱 대상으로 취급한다 | episodeNos=%s", episode_nos)
        return set()
    return {r["chapter"] for r in records}


# ---------- TPM(분당 토큰) ----------
# 한 화를 인덱싱하면 LLM을 여러 번 부른다(추출 + 회차 요약 + 전역 요약 …). 조직 단위 분당
# 토큰 한도를 넘기면 OpenAI가 429를 돌려주는데, 그건 이미 절반쯤 일한 뒤라 되돌리기 어렵다.
# 그래서 접수 시점에 "이 요청이 대략 몇 토큰을 쓸지"를 추정해 남은 여유와 비교하고, 모자라면
# 아예 받지 않는다(429 + Retry-After). 받지 않은 요청은 어디에도 저장하지 않는다.
#
# 추정 휴리스틱 — 정확할 필요는 없고 과소추정만 피하면 된다:
#   화당 토큰 ≈ 원고 글자수 / _CHARS_PER_TOKEN * _PASSES_PER_EPISODE + _CONTEXT_TOKENS_PER_EPISODE
#   - _CHARS_PER_TOKEN=1.5: 한국어는 대략 1.5자에 1토큰(o200k 기준 경험값).
#   - _PASSES_PER_EPISODE=4: 원고 전문이 프롬프트/응답에 실리는 횟수 — 추출 입력, 추출 결과
#     (그래프 JSON), 회차 요약 입력, 전역 요약 입력. 원고가 몇 번 왕복하는지의 대략치다.
#   - _CONTEXT_TOKENS_PER_EPISODE=15000: 원고 길이와 무관하게 매 화 깔리는 고정 비용
#     (그래프 덤프 + 누적 요약 + few-shot 예시).
#
# 이 값은 예약이 아니라 추정이다: 201을 받은 요청도 실제 호출에서 순간 한도에 걸릴 수 있고,
# 그때는 워커가 백오프로 알아서 재시도한다(_run_indexing_with_retry).
INDEX_TPM_LIMIT = int(os.environ.get("INDEX_TPM_LIMIT", "200000"))
_TPM_WINDOW_SECONDS = 60
_CHARS_PER_TOKEN = 1.5
_PASSES_PER_EPISODE = 4
_CONTEXT_TOKENS_PER_EPISODE = 15000

# (기록 시각(monotonic), 추정 토큰). 최근 _TPM_WINDOW_SECONDS초치만 남기는 슬라이딩 윈도우다.
_tpm_window: deque[tuple[float, int]] = deque()


def _estimate_tokens(texts: list[str]) -> int:
    """인덱싱할 원고들이 쓸 토큰 추정치(위 휴리스틱)."""
    return sum(
        int(len(text) / _CHARS_PER_TOKEN * _PASSES_PER_EPISODE) + _CONTEXT_TOKENS_PER_EPISODE
        for text in texts
    )


def _tpm_remaining(now: float) -> int:
    """창을 흘려보내고 남은 여유 토큰을 돌려준다(음수는 0으로 자른다)."""
    while _tpm_window and now - _tpm_window[0][0] >= _TPM_WINDOW_SECONDS:
        _tpm_window.popleft()
    used = sum(tokens for _, tokens in _tpm_window)
    return max(0, INDEX_TPM_LIMIT - used)


def _tpm_retry_after(now: float) -> int:
    """가장 오래된 기록이 창 밖으로 나가 여유가 생기기까지 남은 초."""
    if not _tpm_window:
        return _TPM_WINDOW_SECONDS
    return max(1, math.ceil(_TPM_WINDOW_SECONDS - (now - _tpm_window[0][0])))


def _now_rfc3339() -> str:
    """RFC 3339 UTC(2026-08-11T03:11:00Z). 이 서버의 모든 시각 표기는 이 형식이다."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class IndexEpisode(CamelModel):
    # episodeId는 Spring의 식별자다 — 이 서버는 해석하지 않고 상태 조회에서 그대로 돌려주기만 한다.
    # episodeNo가 실제 인덱싱 단위(lorekeeper의 Chapter.number)다.
    episode_id: int
    episode_no: int
    # text는 없으면 400으로 돌려주려고 일부러 옵셔널로 받는다. 필수로 선언하면 FastAPI가
    # 먼저 422를 내는데, 스펙은 검증 실패를 400 {"detail": ...}로 정해뒀다.
    text: str | None = None


class IndexRequest(CamelModel):
    # userId × workId가 작품 하나를 가리킨다. 둘 다 지금 KG에서 실제 필터로 쓰이지 않지만
    # (kg_scope 참고) 인터페이스에는 처음부터 둔다 — 격리가 생겨도 API 계약을 다시 깨지 않게.
    user_id: int
    work_id: int
    # 비었을 때 422가 아니라 400을 주려고 기본값을 둔다(text와 같은 이유).
    episodes: list[IndexEpisode] = []


class IndexAccepted(CamelModel):
    job_id: str
    user_id: int
    work_id: int
    episode_ids: list[int]
    requested_at: str
    remaining_tpm: int


@app.post("/api/index", response_model=IndexAccepted, status_code=201)
async def index_episodes(req: IndexRequest):
    """여러 화를 한 작업으로 접수하고 즉시 응답한다.

    실제 처리(lorekeeper.indexing 호출)는 _index_worker가 이 요청의 커넥션과 무관하게
    백그라운드에서, 받은 순서대로 하나씩 진행한다.

    접수 순서: 입력 검증 → 화별 완료 마커 확인 → TPM 여유 확인 → 저장·큐잉.
    TPM에서 거절(429)당한 요청은 어디에도 남지 않아야 하므로, 상태 등록과 원고 파일 쓰기는
    모두 그 확인을 통과한 뒤에 한다.
    """
    if not req.episodes:
        raise HTTPException(status_code=400, detail="episodes must not be empty")

    previous_no: int | None = None
    for episode in req.episodes:
        if not episode.text:
            raise HTTPException(
                status_code=400, detail=f"episode {episode.episode_id} must have text"
            )
        # 같은 번호가 두 번 오는 것도 막는다(오름차순 위반). 한 요청 안에서 같은 화를 두 번
        # 인덱싱하는 건 그래프에 이득이 없고 비용만 두 배다.
        if previous_no is not None and episode.episode_no <= previous_no:
            raise HTTPException(
                status_code=400, detail="episodes must be sorted by ascending episodeNo"
            )
        previous_no = episode.episode_no

    # 이미 인덱싱된 화는 일하지 않으므로 TPM도 쓰지 않는다 — 재제출뿐인 요청이 429로
    # 거절당하면 "안전한 재제출"이라는 스펙의 전제가 깨진다.
    indexed = _already_indexed([e.episode_no for e in req.episodes])
    pending_texts = [e.text or "" for e in req.episodes if e.episode_no not in indexed]

    now = time.monotonic()
    remaining = _tpm_remaining(now)
    estimated = _estimate_tokens(pending_texts)
    if estimated > remaining:
        logger.warning(
            "TPM 부족으로 인덱싱 요청 거절 | userId=%s workId=%s 추정=%d 여유=%d",
            req.user_id,
            req.work_id,
            estimated,
            remaining,
        )
        return JSONResponse(
            status_code=429,
            headers={"Retry-After": str(_tpm_retry_after(now))},
            content={
                "detail": "TPM limit exceeded. Retry after the Retry-After period.",
                "remainingTpm": remaining,
            },
        )
    if estimated:
        _tpm_window.append((now, estimated))

    job_id = str(uuid.uuid4())
    requested_at = _now_rfc3339()
    _index_jobs[job_id] = {
        "user_id": req.user_id,
        "work_id": req.work_id,
        "requested_at": requested_at,
        "episodes": [
            {
                "episode_id": e.episode_id,
                "episode_no": e.episode_no,
                "text": e.text or "",
                # 마커가 이미 있는 화는 큐에 들어가기 전부터 done이다.
                "status": "done" if e.episode_no in indexed else "waiting",
                "error": None,
            }
            for e in req.episodes
        ],
    }

    # lorekeeper는 Chunk 단위로만 원문을 보관하므로, "원고 목록" 뷰어에서 보여줄 원문 전체는
    # 우리 쪽에서 별도로 data/episode{N}.txt에 저장해 둔다.
    for episode in req.episodes:
        (DATA_DIR / f"episode{episode.episode_no}.txt").write_text(
            episode.text or "", encoding="utf-8"
        )

    await _index_queue.put(job_id)
    return IndexAccepted(
        job_id=job_id,
        user_id=req.user_id,
        work_id=req.work_id,
        episode_ids=[e.episode_id for e in req.episodes],
        requested_at=requested_at,
        remaining_tpm=_tpm_remaining(now),
    )


class IndexEpisodeStatus(CamelModel):
    episode_id: int
    # waiting | running | done | error. 모든 화가 done 또는 error면 그 작업은 끝난 것이다
    # (작업 단위 status 필드는 따로 두지 않는다 — 화별 상태에서 유도되는 값이라 두 곳에
    # 같은 사실을 적어두면 어긋날 수 있다).
    status: str
    error: str | None = None


class IndexJobStatus(CamelModel):
    job_id: str
    user_id: int
    work_id: int
    episodes: list[IndexEpisodeStatus]


@app.get("/api/index/jobs/{job_id}", response_model=IndexJobStatus)
def get_index_job(job_id: str) -> IndexJobStatus:
    """작업 하나의 화별 진행 상태. 서버 프로세스 메모리에만 있으므로 재시작하면 사라지고,
    진행 중이던 인덱싱도 함께 끊긴다 — 그래도 되는 건 이게 "작업 중 상태"일 뿐 진실이 아니기
    때문이다. 무엇이 실제로 인덱싱됐는지의 정답은 Neo4j(완료 마커)이고, 작업 이력의 정답은
    API 서버의 DB다.

    모르는 jobId는 404다. 그래야 호출자가 "접수된 적 없음/재시작으로 유실됨"과 "아직 처리 중"을
    구분할 수 있다(빈 상태를 200으로 주면 영원히 기다리게 된다). Spring은 404를 보면 같은
    화들을 다시 POST하면 된다 — 끝난 화는 완료 마커 덕분에 다시 인덱싱되지 않는다.
    """
    job = _index_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"'{job_id}' 인덱싱 작업 기록이 없습니다.")
    return IndexJobStatus(
        job_id=job_id,
        user_id=job["user_id"],
        work_id=job["work_id"],
        episodes=[
            IndexEpisodeStatus(
                episode_id=e["episode_id"], status=e["status"], error=e["error"]
            )
            for e in job["episodes"]
        ],
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


class JobAck(BaseModel):
    # 인덱싱과 달리 여기서는 job_id를 Spring이 발급한다 — 검사 요청 하나가 회차 하나라
    # 호출자가 부여한 id를 그대로 쓰는 편이 단순하다. 필드도 기존 snake_case 그대로다.
    job_id: str
    status: str


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
