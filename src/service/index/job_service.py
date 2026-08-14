"""인덱싱 작업의 접수·큐·워커·상태를 관리한다.

작업 id(jobId)는 이 서버가 UUID로 발급한다. 예전에는 Spring이 발급한 id로 일했지만,
이제 요청 하나가 여러 화를 묶어서 들어오고 그 묶음의 진행 상태(화별 waiting/running/
done/error)를 관리하는 주체가 이 서버라서, 묶음의 이름도 여기서 짓는 게 맞다.
Spring은 201 응답으로 받은 jobId로 진행 상황을 조회한다.

처리는 요청 커넥션과 완전히 분리된 백그라운드 워커가 한다 — 요청한 쪽이 끊겨도,
애초에 기다리는 클라이언트가 없어도 서버 프로세스가 떠 있는 한 계속 진행된다.
큐는 단순 FIFO다: 예전에는 화 번호로 정렬하는 PriorityQueue였지만, 이제 "오름차순"은
요청 자체가 보장해야 하는 조건이고(어기면 400) 한 요청 안의 화들은 받은 순서대로
처리하므로, 큐가 순서를 다시 정할 이유가 없다.

큐와 워커는 작품 구분 없이 전역이고 워커는 하나뿐이다. 지금 KG에는 작품 격리가 아예
없어서(kg_scope 참고) 그래프 전체가 "인덱싱된 작품 하나"이고, 여러 작품을 동시에
인덱싱하는 상황 자체가 없다. 여기서 workId별로 큐를 쪼개면 없는 격리를 있는 척하게 될
뿐이다 — KG가 workId를 갖게 되는 시점에 큐/워커를 작품별로 나눈다.

작업 상태는 이 프로세스 메모리에만 있다. 재시작하면 상태가 사라지고 진행 중이던 인덱싱도
함께 끊긴다 — 스펙이 인정하는 정상 시나리오다(Spring은 조회에서 404를 보면 다시 POST한다).
그렇게 다시 보내도 안전한 근거는 _already_indexed의 완료 마커다.

job 한 건의 모양:
  {"user_id": 42, "work_id": 7, "requested_at": "2026-08-11T03:11:00Z",
   "episodes": [{"episode_id": 101, "episode_no": 6, "text": "...",
                 "status": "waiting", "error": None}, ...]}
text를 상태에 함께 들고 있는 건 워커가 나중에 꺼내 쓰기 때문이고, 조회 응답에는 나가지
않는다(응답 모델이 episodeId/status/error만 뽑는다).
"""

import asyncio
import logging
import math
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from openai import RateLimitError

from src.dto.index_dto import IndexAccepted, IndexEpisodeStatus, IndexJobStatus, IndexRequest
from src.lorekeeper.client import get_driver

# lorekeeper/__init__.py가 `indexing`이라는 이름을 함수(async def indexing(...))로 재노출해서
# `from src.lorekeeper import indexing`은 함수를 가리킨다. DATABASE 상수는 서브모듈 경로로
# 직접 가져와야 한다(패키지 네임스페이스의 재바인딩을 건너뛴다).
from src.lorekeeper.indexing import DATABASE as LOREKEEPER_DATABASE
from src.lorekeeper.indexing import indexing as run_indexing
from src.service.kg_scope import kg_scope, require_indexed_work

logger = logging.getLogger("index")

_index_jobs: dict[str, dict] = {}  # job_id -> 위 모양의 dict
_index_queue: asyncio.Queue = asyncio.Queue()  # 접수된 job_id의 FIFO

# 지금까지 큐에 넣은 화 번호의 최대치. 오름차순 규칙을 "요청 하나 안"이 아니라 "큐 전체"에서
# 지키기 위한 워터마크다.
#
# 왜 필요한가: 화별 검증 루프(submit)는 한 요청 안의 순서만 본다. 그래서 POST [5,6]
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


async def worker() -> None:
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


# ---------- 완료 마커 ----------
# "이 화가 인덱싱됐는가"의 진실은 이 서버의 메모리가 아니라 Neo4j다. lorekeeper 인덱싱의
# 마지막 쓰기가 Chapter-[:IN_STORY]->Story(id:'main') 관계이므로(context.update_global_summary
# — 전역 요약 갱신과 같은 쿼리에서 한 번에 MERGE된다), 이 관계가 있으면 그 화는 추출·근거링크·
# 요약까지 전부 끝났다는 뜻이다.
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


async def submit(req: IndexRequest):
    """여러 화를 한 작업으로 접수하고 즉시 응답한다.

    실제 처리(lorekeeper.indexing 호출)는 worker가 이 요청의 커넥션과 무관하게
    백그라운드에서, 받은 순서대로 하나씩 진행한다.

    접수 순서: 작품 확인 → 입력 검증 → 화별 완료 마커 확인 → 큐 전체 오름차순 확인 →
    TPM 여유 확인 → 저장·큐잉.
    TPM에서 거절(429)당한 요청은 어디에도 남지 않아야 하므로, 상태 등록은 그 확인을
    통과한 뒤에 한다.
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

    return IndexAccepted(
        job_id=job_id,
        user_id=req.user_id,
        work_id=req.work_id,
        episode_ids=[e.episode_id for e in req.episodes],
        requested_at=requested_at,
        remaining_tpm=_tpm_remaining(now),
    )


def get_status(job_id: str) -> IndexJobStatus:
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
            IndexEpisodeStatus(episode_id=e["episode_id"], status=e["status"], error=e["error"])
            for e in job["episodes"]
        ],
    )
