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
from src.chat.kg_scope import KG_INDEXED_WORK_ID, kg_scope, require_indexed_work
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

# 지금까지 큐에 넣은 화 번호의 최대치. 오름차순 규칙을 "요청 하나 안"이 아니라 "큐 전체"에서
# 지키기 위한 워터마크다.
#
# 왜 필요한가: 화별 검증 루프(index_episodes)는 한 요청 안의 순서만 본다. 그래서 POST [5,6]
# 직후 POST [3,4]가 들어오면 둘 다 200을 받고 큐는 FIFO라 [5,6,3,4] 순으로 실행된다 — 3화를
# 추출할 때 그래프와 Story.summary에는 이미 5·6화가 들어 있어서, 추출기가 "지금까지의 줄거리"
# 라며 미래를 읽는다. 그래프가 조용히 오염되고 되돌릴 방법이 없다. 같은 구멍으로 같은 화가
# 두 요청에 겹쳐 들어와 두 번 인덱싱되기도 한다(LLM 비용 전액 이중 지출).
#
# 그래서 요청 하나의 루프가 강제하는 규칙(episodeNo는 반드시 증가)을 큐 범위로 끌어올린다.
# 이미 완료 마커가 있는 화는 비교에서 뺀다 — 재제출은 일하지 않으므로 순서를 깨지 않는다.
#
# 프로세스 메모리라서 재시작하면 0으로 돌아간다. 그때는 마커가 있는 화만 걸러질 뿐 "그래프에는
# 6화까지 있는데 3화를 새로 넣는" 요청을 막지 못한다 — 워터마크를 그래프에서 복원하려면
# _already_indexed와 다른 쿼리(요청에 없는 화까지 보는)가 필요해서 지금은 하지 않는다.
_max_queued_episode_no = 0


def _active_episode_nos() -> set[int]:
    """아직 끝나지 않은(waiting·running) 화 번호. 워터마크 판정에서 제외할 대상이다.

    완료 마커는 인덱싱이 **끝나야** 찍히므로, 큐에 들어가 처리 중인 화는 마커가 없다.
    그 상태에서 계약대로 재제출이 오면 마커로 걸러지지 않아 워터마크에 걸리고, 정상적인
    재제출이 영구 실패가 된다. 마커와 이 집합을 함께 봐야 "이미 알고 있는 화"가 온전해진다.
    """
    return {
        episode["episode_no"]
        for job in _index_jobs.values()
        for episode in job["episodes"]
        if episode["status"] in ("waiting", "running")
    }


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
            # 상태와 사유는 한 번의 update로 함께 쓴다 — 조회 API는 다른 스레드에서 돌아서,
            # 두 줄로 나눠 쓰면 그 사이에 들어온 조회가 "error인데 사유는 없음"을 본다.
            episode.update(
                {
                    "error": f"Skipped due to preceding episode ({failed_no}) failure",
                    "status": "error",
                }
            )
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
            # 위와 같은 이유로 사유와 상태를 한 번에 쓴다(터진 읽기 방지).
            episode.update({"error": str(exc), "status": "error"})
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
#
# 이 마커는 화 번호만으로 판정하고 작품을 구분하지 못한다 — 그래프에 workId가 아예 없기
# 때문이다(kg_scope 참고). 그래서 작품 B의 6화가 작품 A의 6화 마커에 걸려 "이미 인덱싱됨"으로
# 보고되는 사고가 성립하는데, 그 구멍은 여기가 아니라 접수 관문(require_indexed_work)에서
# 막는다: 애초에 인덱싱된 작품 외의 요청을 받지 않으면 이 쿼리는 항상 같은 작품 안에서만 답한다.
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


# ---------- 원고 원문 파일 ----------
# 파일 이름에 작품을 넣는다. 예전 이름(episode{N}.txt)은 화 번호만 써서, 작품이 달라도 같은
# 파일을 가리켰다 — 뷰어가 다른 작품의 원고를 보여주거나 서로 덮어쓰는 사고가 난다.
# 지금은 접수 관문이 작품 하나만 통과시키므로 실제로 섞일 일이 없지만, 이름이 작품을 구분하지
# 못한다는 사실 자체가 그 관문을 지웠을 때 되살아나는 지뢰라서 지금 고쳐 둔다.


def _episode_path(work_id: int, chapter: int) -> Path:
    return DATA_DIR / f"work{work_id}_episode{chapter}.txt"


def _write_episode_files(work_id: int, episodes: list[tuple[int, str]]) -> None:
    """원고 전문을 화마다 한 파일씩 쓴다(동기 I/O — 호출자가 스레드로 뺀다)."""
    for chapter, text in episodes:
        _episode_path(work_id, chapter).write_text(text, encoding="utf-8")


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

    접수 순서: 작품 확인 → 입력 검증 → 화별 완료 마커 확인 → 큐 전체 오름차순 확인 →
    TPM 여유 확인 → 저장·큐잉.
    TPM에서 거절(429)당한 요청은 어디에도 남지 않아야 하므로, 상태 등록과 원고 파일 쓰기는
    모두 그 확인을 통과한 뒤에 한다.
    """
    require_indexed_work(req.work_id)

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
    #
    # Neo4j 왕복이라 스레드로 뺀다. async 함수 안에서 그냥 부르면 그동안 이벤트 루프가 통째로
    # 멈춰 인덱싱 워커·채팅·헬스체크가 다 같이 선다(그래프가 죽어 있으면 드라이버에 접속
    # 타임아웃이 없어 30초쯤 멈춘다).
    indexed = await asyncio.to_thread(_already_indexed, [e.episode_no for e in req.episodes])
    pending = [e for e in req.episodes if e.episode_no not in indexed]

    # 여기부터 큐에 넣기까지는 await가 없다 — 그래야 동시에 들어온 두 요청이 같은 워터마크를
    # 읽고 둘 다 통과하는 일이 없다(같은 화가 두 번 인덱싱되는 경로).
    global _max_queued_episode_no

    # 워터마크는 **처음 보는 화**에만 적용한다.
    #
    # 재제출은 이 API의 계약이다 — 429·404·타임아웃이면 호출자가 같은 묶음을 그대로 다시
    # 보낸다. 그런데 완료 마커는 인덱싱이 **끝나야** 찍히므로, 큐에 들어갔지만 아직 처리 중인
    # 화는 마커가 없다. 그 상태에서 재제출이 오면 위의 pending 에 그대로 남고, 자기가 올려둔
    # 워터마크에 자기가 걸려 400을 받는다. 계약대로 재제출했을 뿐인데 영구 실패가 된다
    # (실제로 이 경로로 이미 인덱싱이 끝난 회차가 화면에 "반영 실패"로 표시된 적이 있다).
    #
    # 그래서 지금 큐/처리 중인 화 번호도 마커와 똑같이 취급해 워터마크 판정에서 뺀다.
    # 막으려던 것은 "이미 지나간 화를 뒤늦게 새로 넣는 것"이지 "같은 화를 다시 보내는 것"이
    # 아니다.
    fresh = [e for e in pending if e.episode_no not in _active_episode_nos()]
    if fresh and fresh[0].episode_no <= _max_queued_episode_no:
        raise HTTPException(
            status_code=400,
            detail=(
                f"episodeNo {fresh[0].episode_no} was already queued behind "
                f"{_max_queued_episode_no}; episodes must be submitted in ascending order "
                f"across requests, not just within one"
            ),
        )

    now = time.monotonic()
    remaining = _tpm_remaining(now)
    estimated = _estimate_tokens([e.text or "" for e in pending])
    # 한도 자체를 넘는 묶음은 지금 여유가 아무리 생겨도 통과할 수 없다. 그런데도 429를 주면
    # 호출자는 Retry-After만큼 기다렸다 똑같은 묶음을 영원히 다시 보낸다 — 끝나지 않는 재시도
    # 루프다. 회차당 고정 비용(_CONTEXT_TOKENS_PER_EPISODE)만으로도 14화쯤이면 여기 걸리므로
    # 드문 경우도 아니다. "기다려라"가 아니라 "쪼개서 다시 보내라"고 말해야 한다.
    if estimated > INDEX_TPM_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=(
                f"estimated {estimated} tokens exceeds the per-minute limit "
                f"({INDEX_TPM_LIMIT}) on its own — this bundle can never be accepted. "
                f"Split it into smaller requests."
            ),
        )
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
    # 워터마크는 실제로 큐에 넣는 요청만 올린다(429/400으로 거절한 요청은 없던 일이어야 하므로).
    _max_queued_episode_no = max(_max_queued_episode_no, req.episodes[-1].episode_no)
    # 워터마크를 올린 요청은 같은 블록에서 큐에 들어가야 한다. 사이에 await를 하나라도 두면
    # 뒤 요청이 그 틈에 끼어들어 먼저 큐에 들어갈 수 있고, 그러면 워터마크로 막으려던 역순
    # 실행이 그대로 일어난다. 큐에 상한이 없어 put_nowait은 절대 막히지 않는다(= await 불필요).
    _index_queue.put_nowait(job_id)

    # lorekeeper는 Chunk 단위로만 원문을 보관하므로, "원고 목록" 뷰어에서 보여줄 원문 전체는
    # 우리 쪽에서 별도로 파일에 저장해 둔다. 화 하나가 5만 자까지 가고 한 요청에 여러 화가
    # 실리므로, 이벤트 루프에서 직접 쓰면 그동안 서버 전체가 멈춘다 — 스레드로 뺀다.
    # 큐잉 뒤에 쓰는 건 순서 보장이 우선이기 때문이다. 워커는 이 파일을 읽지 않고 job에 실린
    # 원고로 인덱싱하므로, 파일이 조금 늦게 생겨도 인덱싱에는 영향이 없다(뷰어만 잠깐 늦는다).
    await asyncio.to_thread(
        _write_episode_files,
        req.work_id,
        [(e.episode_no, e.text or "") for e in req.episodes],
    )

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
        # claim은 LLM이 만든 JSON이라 키가 있어도 값이 null일 수 있다. 기본값을 or로 씌워
        # 조회 API가 응답 검증에서 500을 내지 않게 한다(진행 조회는 무슨 일이 있어도 살아야 한다).
        _detect_jobs[job_id]["claims"] = [
            {
                "index": i,
                "quote": c.get("quote") or "",
                "category": c.get("category") or "기타",
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
        # episode_number를 파이프라인에 넘긴다. 이게 없으면 검사 대상 회차를 **자기 자신을 포함한
        # 그래프 전체**와 대조하게 된다 — 5화를 5화가 만든 사실과 비교해 "일치"라고 자평하고,
        # 6~10화가 나중에 밝힌 반전을 5화에 심어둔 모순으로 읽는다.
        findings = await check_new_episode_streaming(
            text, episode_number, on_claims_extracted, on_claim_done
        )
        # 리포트는 파일 두 개(md+json)를 쓴다 — 이벤트 루프에서 직접 쓰지 않는다.
        await asyncio.to_thread(
            save_report_files, findings, job_id, display_label=f"{episode_number}화"
        )
        # 결과와 상태를 한 번의 update로 쓴다. 조회 API(get_detect_job)는 다른 스레드에서 도는데,
        # 두 줄로 나눠 쓰면 그 사이에 들어온 폴링이 status="done" + findings=null을 보고,
        # 호출자는 폴링을 멈춘 뒤 "오류 0건"인 빈 리포트를 확정 저장한다.
        _detect_jobs[job_id].update({"findings": findings, "status": "done"})
    except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 호출자가 보여줄 수 있다
        logger.exception("설정 오류 탐지 실패 | job_id=%s episode=%s", job_id, episode_number)
        # 같은 이유로 사유와 상태를 함께 쓴다(status="error" + detail=null을 보이지 않게).
        _detect_jobs[job_id].update({"detail": str(exc), "status": "error"})


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
    require_indexed_work(req.work_id)

    known = _detect_jobs.get(req.job_id)
    if known is not None:
        # 인덱싱과 같은 이유(재시도 방어). 여기선 그래프가 더러워지진 않지만 회차 하나 검사에
        # 수십 번의 LLM 호출이 들어가므로 중복 실행 비용이 특히 크다.
        return JobAck(job_id=req.job_id, status=known["status"])

    _detect_jobs[req.job_id] = {"status": "queued", "claims": []}
    asyncio.create_task(_run_detect(req.job_id, req.work_id, req.episode_number, req.text))
    return JobAck(job_id=req.job_id, status="queued")


class DetectClaimProgress(BaseModel):
    """검사 중인 claim 하나의 진행 상황. 프론트가 검사가 끝나기 전에 목록을 그리기 위한 것이다.

    claim 추출이 끝나는 순간 전부 status="running"으로 한꺼번에 나타나고, 검증이 끝난 것부터
    하나씩 status="done"으로 바뀐다(claim들은 병렬 검증이라 끝나는 순서는 index 순이 아니다).
    index는 이 배열 안에서 고정이라 프론트가 행을 안정적으로 식별할 수 있다.
    """

    index: int  # 0부터. 검사가 끝날 때까지 이 claim의 고정 식별자다.
    quote: str  # 신규 회차 원문에서 뽑은 서술 그대로
    category: str  # 생사/소유물/능력/관계/소속/시점 등. 추출기가 정하고 미지정이면 "기타"
    status: str  # "running" | "done"
    # 아래 넷은 status="done"이 되기 전까지 전부 null이다(판정 전에는 알 수 없는 값이라서).
    label: str | None = None  # "contradiction" | "consistent" | "unknown"
    established_fact: str | None = None
    # 모델이 "3" 또는 "3화"처럼 돌려줄 수 있어 숫자로 강제하지 않는다 — 조회 API가 판정 결과의
    # 표기 때문에 500을 내면 안 된다.
    source_episode: int | str | None = None
    explanation: str | None = None


class DetectStatus(BaseModel):
    job_id: str
    status: str
    detail: str | None = None
    # claims: 진행 상황(검사 중에도 채워진다). 접수 직후엔 빈 배열이고 절대 null이 아니다.
    # findings: 최종 판정 결과(status="done"일 때만 채워진다). claims와 달리 파이프라인이 만든
    # dict 그대로 나간다 — tool_calls_used·entities처럼 claims에 없는 필드가 더 들어 있다.
    claims: list[DetectClaimProgress] = []
    findings: list[dict] | None = None


@app.get("/api/detect/{job_id}", response_model=DetectStatus)
def get_detect_job(job_id: str) -> DetectStatus:
    """검사 하나의 진행 상태와 결과. 인덱싱 조회와 마찬가지로 모르는 job_id는 404다."""
    state = _detect_jobs.get(job_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"'{job_id}' 탐지 작업 기록이 없습니다.")
    # 검사 작업은 이벤트 루프에서, 이 조회는 FastAPI의 스레드풀에서 돈다. 필드를 하나씩 읽으면
    # 읽는 도중에 상태가 바뀌어 "status는 새 값, findings는 옛 값" 같은 조합을 볼 수 있다.
    # dict(state) 한 번으로 스냅샷을 떠서 그 한 시점만 보고 응답을 만든다.
    snapshot = dict(state)
    return DetectStatus(
        job_id=job_id,
        status=snapshot["status"],
        detail=snapshot.get("detail"),
        claims=snapshot.get("claims") or [],
        findings=snapshot.get("findings"),
    )


# ---------- AI 채팅 API ----------
# 인덱싱/검사와 달리 백그라운드 작업이 아니다 — 작가가 답을 기다리고 있으므로 이 요청 안에서
# 끝까지 처리해 JSON으로 한 번에 돌려준다(스트리밍 아님). 대화 기록은 서버 메모리에 남기지
# 않는다: 진실의 원천은 API 서버(Spring)의 chat_messages 테이블이고, 여기는 매 턴 통째로
# 받아서 답만 만들어 주는 무상태 계산기다(그래야 이 서버가 재시작해도 대화가 안 끊긴다).


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatEditingEpisode(BaseModel):
    """작가가 지금 고쳐 쓰고 있는 회차. 본문은 발췌가 아니라 **전문**이다.

    number가 없을 수 있다 — API 서버의 DRAFT는 화수가 확정되기 전이라 번호가 null이다.
    """

    # 화수. DRAFT(아직 확정 전)면 없다.
    number: int | None = None
    title: str | None = None
    text: str | None = None


class ChatContext(BaseModel):
    """이번 질문의 회차 컨텍스트.

    회차에 얽힌 개념은 셋인데 여기 실리는 건 둘뿐이다:
      - editing_episode  : 집필 중인 회차(전문 포함). API 서버가 도메인 규칙으로 정한다.
      - viewing_episode_number : 화면에 열어 둔 회차. 프론트만 알 수 있다.
    셋째인 "인덱싱된 회차"는 **일부러 요청에 없다.** 그건 Neo4j 그래프의 사실이고, 요청이
    들고 온 값은 인덱싱이 진행되는 동안 곧바로 낡는다. 에이전트가 매 턴 직접 조회한다
    (src/chat/indexed.py).

    셋 다 없어도 대화는 성립한다 — 편집기를 열지 않고 질문만 하는 경우가 있다.
    """

    editing_episode: ChatEditingEpisode | None = None
    viewing_episode_number: int | None = None


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
    require_indexed_work(req.work_id)

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
    # 뷰어는 작품을 인자로 받지 않는다 — 그래프가 작품 하나뿐이라 여기서 보여줄 수 있는 원고도
    # 그 작품(KG_INDEXED_WORK_ID)의 것뿐이다. 격리가 생기면 이 두 API도 workId를 받아야 한다.
    path = _episode_path(KG_INDEXED_WORK_ID, chapter)
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
    path = _episode_path(KG_INDEXED_WORK_ID, chapter)
    raw_text = path.read_text(encoding="utf-8") if path.exists() else ""
    return EpisodeDetail(
        chapter=chapter,
        summary=records[0]["summary"] or "",
        raw_text=raw_text,
        chars=len(raw_text),
    )
