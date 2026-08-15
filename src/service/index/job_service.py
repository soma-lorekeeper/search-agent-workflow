"""인덱싱 작업의 접수·큐·워커·상태를 관리한다.

작업 id(jobId)는 이 서버가 UUID로 발급한다. 예전에는 Spring이 발급한 id로 일했지만,
이제 요청 하나가 여러 화를 묶어서 들어오고 그 묶음의 진행 상태(화별 QUEUED/RUNNING/
DONE/ERROR)를 관리하는 주체가 이 서버라서, 묶음의 이름도 여기서 짓는 게 맞다.
Spring은 201 응답으로 받은 jobId로 진행 상황을 조회한다.

처리는 요청 커넥션과 완전히 분리된 백그라운드 워커가 한다 — 요청한 쪽이 끊겨도,
애초에 기다리는 클라이언트가 없어도 서버 프로세스가 떠 있는 한 계속 진행된다.
큐는 단순 FIFO다: 예전에는 화 번호로 정렬하는 PriorityQueue였지만, 이제 "오름차순"은
요청 자체가 보장해야 하는 조건이고(어기면 400) 한 요청 안의 화들은 받은 순서대로
처리하므로, 큐가 순서를 다시 정할 이유가 없다.

큐와 워커는 테넌트 구분 없이 전역이고 워커는 하나뿐이다. 순차 처리는 **테넌트 안에서만**
필요한 규칙이지만(누적 컨텍스트가 이전 회차 완료를 전제한다), 테넌트별로 워커를 쪼개면
동시에 도는 인덱싱 수만큼 LLM 호출이 겹쳐 TPM 한도를 쉽게 넘긴다. 지금은 전역 단일 워커로
두고, 처리량이 문제가 되면 그때 테넌트별 동시성을 연다 — 순서 계약은 이미 테넌트 안에서만
적용되므로 그 변경은 안전하다.

작업 상태는 이 프로세스 메모리에만 있다. 재시작하면 상태가 사라지고 진행 중이던 인덱싱도
함께 끊긴다 — 스펙이 인정하는 정상 시나리오다(Spring은 조회에서 404를 보면 다시 POST한다).
다시 보내도 **끝난 화를 두 번 일하지 않는** 근거가 _already_indexed의 완료 마커다.
(끊긴 화를 다시 도는 것 자체는 아직 안전하지 않다 — 아래 완료 마커 절의 경고 참고.)

job 한 건의 모양:
  {"user_id": 42, "work_id": 7, "tenant_id": "42:7", "requested_at": "2026-08-11T03:11:00Z",
   "episodes": [{"episode_id": 101, "episode_no": 6, "text": "...",
                 "status": "QUEUED", "error": None}, ...]}
text를 상태에 함께 들고 있는 건 워커가 나중에 꺼내 쓰기 때문이고, 조회 응답에는 나가지
않는다(응답 모델이 episodeId/status/error만 뽑는다).
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from fastapi import HTTPException
from fastapi.responses import JSONResponse

from src.common import admission, llm_limit
from src.dto.index_dto import IndexAccepted, IndexEpisodeStatus, IndexJobStatus, IndexRequest
from src.config import EXTRACTION_MODEL
from src.repository.neo4j.client import get_driver

from src.service.index.indexing_service import DATABASE as LOREKEEPER_DATABASE
from src.service.index.indexing_service import indexing as run_indexing
from src.common.tenant import Tenant
from src.service.kg_scope import kg_scope

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
# 프로세스 메모리라서 재시작하면 비워진다. 그때는 마커가 있는 화만 걸러질 뿐 "그래프에는
# 6화까지 있는데 3화를 새로 넣는" 요청을 막지 못한다 — 워터마크를 그래프에서 복원하려면
# _already_indexed와 다른 쿼리(요청에 없는 화까지 보는)가 필요해서 지금은 하지 않는다.
#
# 테넌트별로 따로 센다. 전역 하나면 소설 A의 7화를 넣은 뒤 소설 B의 3화를 넣을 수 없다 —
# 서로 아무 상관 없는 두 소설이 서로의 진도에 발이 묶인다.
_max_queued_episode_no: dict[str, int] = {}


def _active_episode_nos(tenant: Tenant) -> set[int]:
    """아직 끝나지 않은(QUEUED·RUNNING) 화 번호. 워터마크 판정에서 제외할 대상이다.

    완료 마커는 인덱싱이 **끝나야** 찍히므로, 큐에 들어가 처리 중인 화는 마커가 없다.
    그 상태에서 계약대로 재제출이 오면 마커로 걸러지지 않아 워터마크에 걸리고, 정상적인
    재제출이 영구 실패가 된다. 마커와 이 집합을 함께 봐야 "이미 알고 있는 화"가 온전해진다.
    """
    return {
        episode["episode_no"]
        for job in _index_jobs.values()
        if job["tenant_id"] == tenant.id
        for episode in job["episodes"]
        if episode["status"] in ("QUEUED", "RUNNING")
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
        if episode["status"] == "DONE":
            # 접수 시점에 완료 마커가 확인된 화 — 다시 인덱싱하지 않는다.
            continue
        if failed_no is not None:
            # 상태와 사유는 한 번의 update로 함께 쓴다 — 조회 API는 다른 스레드에서 돌아서,
            # 두 줄로 나눠 쓰면 그 사이에 들어온 조회가 "error인데 사유는 없음"을 본다.
            episode.update(
                {
                    "error": f"Skipped due to preceding episode ({failed_no}) failure",
                    "status": "ERROR",
                }
            )
            continue

        episode["status"] = "RUNNING"
        try:
            tenant = kg_scope(job["user_id"], job["work_id"])
            logger.info(
                "인덱싱 시작 | jobId=%s userId=%s workId=%s episodeId=%s episodeNo=%s",
                job_id,
                job["user_id"],
                job["work_id"],
                episode["episode_id"],
                episode["episode_no"],
            )
            await run_indexing(tenant, episode["episode_no"], episode["text"])
            # 원고를 놓아준다. 워커가 꺼내 쓰려고 상태에 담아 둔 것인데 다 썼고, 작업
            # 기록은 재시작 전까지 지워지지 않는다 — 회차당 수만 자가 프로세스가 뜬 내내
            # 남아 쌓인다. 조회 응답에는 애초에 나가지 않는 필드다.
            episode["text"] = ""
            episode["status"] = "DONE"
        except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 호출자가 보여줄 수 있다
            logger.exception(
                "인덱싱 실패 | jobId=%s episodeId=%s episodeNo=%s",
                job_id,
                episode["episode_id"],
                episode["episode_no"],
            )
            # 위와 같은 이유로 사유와 상태를 한 번에 쓴다(터진 읽기 방지).
            # 실패한 화도 마찬가지다 — 뒤 화들은 어차피 건너뛰므로 다시 읽을 일이 없다.
            episode.update({"error": str(exc), "status": "ERROR", "text": ""})
            failed_no = episode["episode_no"]


# ---------- 429 재시도는 여기서 하지 않는다 ----------
#
# 예전에는 이 자리에 회차 단위 백오프 재시도가 있었다(3회, 20/40/80초). 지운 이유는
# **재시도의 단위가 잘못됐기 때문**이다.
#
# indexing()은 8단계짜리이고 그중 네 곳이 LLM 호출이다. 마지막 요약에서 429가 나도 재시도는
# 1단계부터 다시 도는데, 라이브러리 Neo4jWriter의 노드 쓰기는 upsert가 아니라 **CREATE**다
# (neo4j_queries.upsert_node_query: `CREATE (n:__KGBuilder__ {__tmp_internal_id: row.id})`).
# 중복은 그 뒤 resolver가 이름으로 병합해 없애는데, PerLabelResolver는 Character와
# Item/Location/Organization만 병합하고 **Event·CharacterState는 일부러 병합하지 않는다**
# (서술형 이름이라 유사도 병합이 숫자·부위 차이를 뭉갠다). 그래서 재시도할 때마다 사건과
# 인물 상태가 한 벌씩 더 쌓인다 — 예외도 안 나고 검색 결과에만 조용히 중복으로 잡힌다.
#
# 즉 이 층은 "회차를 살리려다 그래프를 오염시키는" 교환이었다. 지금 429 재시도는
# **호출 단위로만** 한다(src/common/openai_client.py, 5회 지수 백오프 + 지터) — 그쪽은
# 그 호출만 다시 보내므로 그래프에 아무것도 남기지 않는다. 스펙 6절의 "서버가 내부적으로
# 백오프 재시도하고 Spring은 관여하지 않는다"는 계약은 그대로 지켜진다. 재시도의 위치만
# 바뀌었다.
#
# 대가: 관문의 재시도(약 17초)를 넘겨 429가 올라오면 그 화가 ERROR가 되고, 같은 작업의 뒤
# 화들은 연쇄 스킵된다. Spring이 재제출하는 것이 복구 경로다.
#
# ⚠️ 재제출도 미완 산출물 위에 다시 쓰는 것은 마찬가지다. "재인덱싱 전에 그 회차의 미완
# 산출물을 지우는" 정리 단계가 아직 없다 — 재시도와 무관하게 남아 있는 문제이고 별도
# 작업으로 다룬다. 아래 완료 마커 주석의 "전부 upsert라 수렴한다"도 그래서 부정확하다.


# ---------- 완료 마커 ----------
# "이 화가 인덱싱됐는가"의 진실은 이 서버의 메모리가 아니라 Neo4j다. lorekeeper 인덱싱의
# 마지막 쓰기가 Chapter-[:IN_STORY]->Story(id:'main') 관계이므로(context.update_global_summary
# — 전역 요약 갱신과 같은 쿼리에서 한 번에 MERGE된다), 이 관계가 있으면 그 화는 추출·근거링크·
# 요약까지 전부 끝났다는 뜻이다.
#
# 그래서 접수 시점에 화마다 이 마커를 확인한다:
#   - 있으면 → 일하지 않고 즉시 done
#   - 없으면 → 처음부터 다시 인덱싱
# 재시작으로 진행 상태가 날아가 Spring이 같은 화를 다시 보내도 **일을 두 번 하지 않는**
# 이유가 이것이다.
#
# ⚠️ 다만 "다시 돌려도 같은 값으로 수렴한다"고는 말할 수 없다. 여기 있던 예전 설명은
# "Neo4jWriter가 전부 upsert"라고 적었는데 사실이 아니다 — 라이브러리의 노드 쓰기는
# `CREATE (n:__KGBuilder__ ...)`이고, 중복 제거는 그 뒤 resolver가 이름으로 병합해 한다.
# 그런데 PerLabelResolver는 Character·Item·Location·Organization만 병합하고
# **Event·CharacterState는 일부러 병합하지 않으므로**, 중간까지 쓰다 만 회차를 다시 돌리면
# 그 둘이 한 벌씩 더 쌓인다.
#
# 그래서 마커가 보장하는 것은 "완료된 화를 두 번 일하지 않는다"까지이고, "미완 화를 다시
# 돌려도 안전하다"는 아니다. 미완 산출물을 지우고 시작하는 정리 단계가 필요한데 아직 없다
# (별도 작업).
#
# 마커는 테넌트 안에서만 유효하다. 예전에는 화 번호만 보고 판정해서 소설 B의 6화가 소설 A의
# 6화 마커에 걸려 "이미 인덱싱됨"으로 보고됐다(아무 일도 안 하고 done — 조용한 데이터 유실).
# 그 구멍을 접수 관문에서 다른 작품을 통째로 거절하는 방식으로 막고 있었는데, 이제 쿼리 자체가
# 테넌트로 좁으므로 그 관문이 필요 없어졌다.
_INDEXED_MARKER_CYPHER = """
MATCH (c:Chapter {tenant_id: $tenant_id})-[:IN_STORY]->(:Story {id: 'main', tenant_id: $tenant_id})
WHERE c.number IN $chapters
RETURN c.number AS chapter
"""


def _already_indexed(tenant: Tenant, episode_nos: list[int]) -> set[int]:
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
                {"chapters": episode_nos, **tenant.params()},
                database_=LOREKEEPER_DATABASE,
            )
        finally:
            driver.close()
    except Exception:  # noqa: BLE001 — 마커 확인 실패는 작업 거절 사유가 아니다
        logger.warning("완료 마커 확인 실패 — 전부 인덱싱 대상으로 취급한다 | episodeNos=%s", episode_nos)
        return set()
    return {r["chapter"] for r in records}


# ---------- 접수 게이트: 큐가 얼마나 밀렸는가 ----------
#
# 예전에는 여기서 TPM(분당 토큰)을 봤다. 접수 시점에 원고 글자수로 "이 요청이 몇 토큰을
# 쓸지"를 추정해 60초 창의 여유와 비교하는 방식이었는데, 실측해보니 두 가지가 틀렸다.
#
# 1. **모델이 실제와 다르다.** OpenAI의 한도는 60초 슬라이딩 창이 아니라 **연속 충전
#    버킷**이다(응답 헤더의 reset이 3ms로 오는 것이 그 증거 — "만수위 복귀까지"의 뜻이라
#    조금만 쓰면 즉시 찬다). 창 모델은 그걸 흉내 낸 근사였다.
#
# 2. **지키려는 자원이 이미 다른 층에서 지켜진다.** 게이트웨이 세마포어(모델당 4)와 단일
#    워커 때문에 인덱싱은 구조적으로 TPM을 넘길 수 없다 — 실측 소비율 1,600 토큰/초로
#    충전율 3,333/초의 48%다.
#
# 정작 부족한 자원은 **워커 처리량**인데 그건 아무도 안 봤다. 접수는 분당 7화까지 받고
# 처리는 분당 0.5화라 14배 어긋난 채로 큐가 자랐다.
#
# 그래서 게이트가 재는 것을 토큰에서 **대기 시간**으로 바꾼다. 큐에 밀린 화 수는 정확히
# 셀 수 있고(추정이 아니다), 화당 처리 시간은 스펙이 명시한 값이다.
INDEX_EPISODE_SECONDS = int(os.environ.get("INDEX_EPISODE_SECONDS", "120"))
INDEX_MAX_WAIT_SECONDS = int(os.environ.get("INDEX_MAX_WAIT_SECONDS", "2400"))  # 40분 = 20화


def _queued_episode_count() -> int:
    """아직 처리되지 않은 화 수.

    **테넌트를 가리지 않고 전부 센다.** 워커가 하나뿐이라 큐도 전역이고, 소설 A의 대기열
    뒤에 소설 B가 서면 B도 그만큼 기다린다 — 대기 시간을 예측하려면 전체를 봐야 한다.
    """
    return sum(
        1
        for job in _index_jobs.values()
        for episode in job["episodes"]
        if episode["status"] in ("QUEUED", "RUNNING")
    )


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
    tenant = kg_scope(req.user_id, req.work_id)

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
    indexed = await asyncio.to_thread(
        _already_indexed, tenant, [e.episode_no for e in req.episodes]
    )
    pending = [e for e in req.episodes if e.episode_no not in indexed]

    # 여기부터 큐에 넣기까지는 await가 없다 — 그래야 동시에 들어온 두 요청이 같은 워터마크를
    # 읽고 둘 다 통과하는 일이 없다(같은 화가 두 번 인덱싱되는 경로).
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
    watermark = _max_queued_episode_no.get(tenant.id, 0)
    fresh = [e for e in pending if e.episode_no not in _active_episode_nos(tenant)]
    if fresh and fresh[0].episode_no <= watermark:
        raise HTTPException(
            status_code=400,
            detail=(
                f"episodeNo {fresh[0].episode_no} was already queued behind "
                f"{watermark}; episodes must be submitted in ascending order "
                f"across requests, not just within one"
            ),
        )

    # 이 묶음 하나만으로 상한을 넘으면 큐가 아무리 비어도 통과할 수 없다. 그런데도 429를 주면
    # 호출자는 Retry-After만큼 기다렸다 똑같은 묶음을 영원히 다시 보낸다 — 끝나지 않는 재시도
    # 루프다. "기다려라"가 아니라 "쪼개서 다시 보내라"고 말해야 한다.
    own_wait = len(pending) * INDEX_EPISODE_SECONDS
    if own_wait > INDEX_MAX_WAIT_SECONDS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{len(pending)} episodes need about {own_wait}s to index, which exceeds "
                f"the maximum queue wait ({INDEX_MAX_WAIT_SECONDS}s) on its own — this "
                f"bundle can never be accepted. Split it into smaller requests."
            ),
        )

    # 큐가 얼마나 밀렸는가. 이미 인덱싱된 화(pending에서 빠진 것)는 일하지 않으므로 세지
    # 않는다 — 재제출뿐인 요청이 429로 거절당하면 "안전한 재제출"이라는 스펙의 전제가 깨진다.
    queued = _queued_episode_count()
    estimated_wait = (queued + len(pending)) * INDEX_EPISODE_SECONDS
    if estimated_wait > INDEX_MAX_WAIT_SECONDS:
        logger.warning(
            "큐 혼잡으로 인덱싱 요청 거절 | userId=%s workId=%s 대기=%d화 예상=%d초",
            req.user_id,
            req.work_id,
            queued,
            estimated_wait,
        )
        return JSONResponse(
            status_code=429,
            headers={"Retry-After": str(estimated_wait - INDEX_MAX_WAIT_SECONDS)},
            content={
                "detail": "Indexing queue is full. Retry after the Retry-After period.",
                "queuedEpisodes": queued,
                "estimatedWaitSeconds": estimated_wait,
            },
        )

    # 마지막 안전망 — 탐지가 같은 모델 버킷을 비웠는지 본다(둘 다 EXTRACTION_MODEL을 쓴다).
    # 큐가 한가해도 OpenAI 쪽이 바닥이면 시작해봐야 429만 맞는다.
    retry_after = admission.budget_retry_after(EXTRACTION_MODEL)
    if retry_after is not None:
        logger.warning(
            "모델 한도 부족으로 인덱싱 요청 거절 | userId=%s workId=%s 재시도=%d초",
            req.user_id,
            req.work_id,
            retry_after,
        )
        return JSONResponse(
            status_code=429,
            headers={"Retry-After": str(retry_after)},
            content={
                "detail": "Model rate limit is nearly exhausted. Retry after the Retry-After period.",
                "remainingTpm": llm_limit.remaining(EXTRACTION_MODEL).remaining or 0,
            },
        )

    job_id = str(uuid.uuid4())
    requested_at = _now_rfc3339()
    _index_jobs[job_id] = {
        "user_id": req.user_id,
        "work_id": req.work_id,
        "tenant_id": tenant.id,
        "requested_at": requested_at,
        "episodes": [
            {
                "episode_id": e.episode_id,
                "episode_no": e.episode_no,
                "text": e.text or "",
                # 마커가 이미 있는 화는 큐에 들어가기 전부터 DONE이다.
                "status": "DONE" if e.episode_no in indexed else "QUEUED",
                "error": None,
            }
            for e in req.episodes
        ],
    }
    # 워터마크는 실제로 큐에 넣는 요청만 올린다(429/400으로 거절한 요청은 없던 일이어야 하므로).
    _max_queued_episode_no[tenant.id] = max(watermark, req.episodes[-1].episode_no)
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
        # 미터가 헤더로 관측한 **실제** 잔량이다. 예전에는 글자수 추정에서 뺀 값이라
        # 이름과 달리 TPM이 아니었다.
        remaining_tpm=llm_limit.remaining(EXTRACTION_MODEL).remaining or 0,
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
