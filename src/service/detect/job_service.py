"""설정 오류 탐지 작업의 접수·실행·상태를 관리한다.

파이프라인은 세 단계다: 원고에서 claim 추출 → claim마다 근거 검색 → 문서고와 대조해 판정.
단계별 구현은 extract_service·retrieve_service·judge_service에 있고 여기는 그것을 잇는다.

인덱싱과 달리 회차 순서를 지킬 필요가 없다(검사 대상 회차 하나를 그 시점의 그래프와
대조할 뿐, 검사끼리 서로 의존하지 않는다). 그래서 큐 없이 요청마다 asyncio.create_task로
바로 백그라운드에 띄운다.

진행 상태는 이 프로세스 메모리에 있고, 완료된 결과는 Spring의 테이블에 쓴다. 재시작하면
메모리가 비어 조회가 404가 되는데, Spring은 자기 테이블을 보거나 같은 jobId로 다시
보내면 된다(같은 jobId 재실행은 결과를 덮어쓴다).
"""

from __future__ import annotations

import asyncio
import logging
import os

from src.common import admission, llm_limit
from src.common.exceptions import NotFound, RateLimited
from src.config import EXTRACTION_MODEL
from src.dto.detect_dto import DetectRequest, DetectStatus, JobAck
from src.repository.postgres import detection
from src.service.detect import extract_service, judge_service, retrieve_service
from src.service.kg_scope import kg_scope

logger = logging.getLogger("detect")

# 동시에 돌릴 검사 수. 게이트웨이의 LLM 세마포어와 같은 값으로 잡는다 — 그보다 많이 받아도
# 어차피 그 앞에서 줄을 서므로 지연만 늘고 메모리만 더 쓴다.
#
# 검사 하나가 판정 단계에서 27k 토큰짜리 호출을 한 번 하는데, 그게 여럿 겹치면 버킷을
# 빠르게 먹는다. 동시 검사 수를 묶는 것이 그 겹침을 제한하는 가장 직접적인 수단이다.
MAX_CONCURRENT_DETECTS = int(os.environ.get("MAX_CONCURRENT_DETECTS", "4"))

# 429의 Retry-After로 쓸 검사 1건의 대략적 소요 시간. 슬롯이 하나 나기까지의 추정치다.
# 실제 소요는 회차 길이에 따라 달라지므로 관측으로 조정한다.
DETECT_JOB_SECONDS = int(os.environ.get("DETECT_JOB_SECONDS", "90"))

# job_id -> {"status", "phase", "claim_count", "contradiction_count", "findings", "detail"}
_detect_jobs: dict[str, dict] = {}

# 진행 중인 검사 task. 이벤트 루프가 task를 약한 참조로만 들고 있어서, 여기 담아두지
# 않으면 GC가 실행 도중에 가져갈 수 있다 — 그러면 그 job은 예외 한 줄 없이 QUEUED에
# 영원히 머문다. 끝나면 콜백이 스스로 빼낸다.
_running: set[asyncio.Task] = set()


def _running_detect_count() -> int:
    """아직 끝나지 않은 검사 수.

    `_running`(task 집합)이 아니라 상태를 세는 이유: task는 완료 콜백이 도는 시점에
    빠지는데, 그 사이 잠깐 실제보다 크게 보인다. 상태는 _run_detect가 끝나면서 DONE/ERROR로
    확정되므로 더 정확하다.
    """
    return sum(1 for s in _detect_jobs.values() if s["status"] in ("QUEUED", "RUNNING"))


async def _run_detect(
    job_id: str, user_id: int, work_id: int, episode_number: int, text: str
) -> None:
    tenant = kg_scope(user_id, work_id)
    state = _detect_jobs[job_id]
    state.update({"status": "RUNNING", "phase": "EXTRACT"})
    await asyncio.to_thread(detection.mark_running, job_id)

    try:
        # lines는 판정 결과의 줄 번호를 원문으로 되짚는 데 쓴다(judge에 그대로 넘긴다).
        claims, lines, _tokens = await extract_service.extract(text, tenant, episode_number)
        # claim 수는 추출이 끝나야 알 수 있다. 여기서부터 조회가 진행률을 보여줄 수 있다.
        state.update({"phase": "RETRIEVE", "claim_count": len(claims)})

        evidence = await retrieve_service.retrieve(claims, tenant, episode_number)
        state["phase"] = "JUDGE"

        findings = await judge_service.judge(claims, evidence, lines)

        # 결과와 상태를 한 번의 update로 쓴다. 조회는 다른 스레드에서 도는데 두 줄로 나눠
        # 쓰면 그 사이의 폴링이 status=DONE + findings=null을 보고, 호출자는 폴링을 멈춘 뒤
        # "오류 0건"인 빈 리포트를 확정 저장한다.
        state.update(
            {
                "status": "DONE",
                "phase": None,
                "findings": findings,
                "contradiction_count": len(findings),
            }
        )
        await asyncio.to_thread(detection.save_result, job_id, len(claims), findings)
        logger.info(
            "설정 오류 탐지 완료 | job_id=%s claim=%d 오류=%d",
            job_id,
            len(claims),
            len(findings),
        )
    except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 호출자가 보여줄 수 있다
        logger.exception("설정 오류 탐지 실패 | job_id=%s episode=%s", job_id, episode_number)
        # 같은 이유로 사유와 상태를 함께 쓴다.
        state.update({"status": "ERROR", "phase": None, "detail": str(exc)})
        await asyncio.to_thread(detection.mark_error, job_id, str(exc))


async def submit(req: DetectRequest) -> JobAck:
    """검사를 백그라운드로 시작하고 즉시 응답한다. 여유가 없으면 RateLimited(→429)로
    거절한다 — HTTP 변환은 error_handlers가 한다."""
    known = _detect_jobs.get(req.job_id)
    if known is not None:
        # 중복 제출 방어. 회차 하나 검사에 LLM을 수십 번 부르므로 중복 실행 비용이 크다.
        #
        # **게이트보다 먼저 본다.** 이미 접수한 검사를 다시 물어보는 것은 자원을 쓰지
        # 않으므로 거절할 이유가 없다. 뒤에 두면 서버가 바쁠 때 진행 중인 검사의 상태
        # 조회조차 429가 되어, 호출자가 결과를 영영 못 받는다.
        return JobAck(job_id=req.job_id, status=known["status"])

    # 진행 중인 검사가 너무 많으면 받지 않는다. 인덱싱과 달리 탐지는 큐가 없고 전부
    # 동시에 돌아서(create_task) 밀리는 게 아니라 한꺼번에 터진다 — 그래서 "대기 시간"이
    # 아니라 "동시 실행 수"로 잰다.
    running = _running_detect_count()
    if running >= MAX_CONCURRENT_DETECTS:
        logger.warning("동시 검사 상한으로 탐지 요청 거절 | job_id=%s 진행중=%d", req.job_id, running)
        raise RateLimited(
            "Too many detections in progress. Retry after the Retry-After period.",
            retry_after=DETECT_JOB_SECONDS,
            type="/errors/too-many-detections",
            title="Too Many Detections",
            # 확장 멤버는 응답 top-level 에 이 키 그대로 실린다(스펙의 camelCase).
            extensions={"runningDetections": running},
        )

    # 마지막 안전망 — 인덱싱이 같은 모델 버킷을 비웠는지 본다(둘 다 EXTRACTION_MODEL).
    retry_after = admission.budget_retry_after(EXTRACTION_MODEL)
    if retry_after is not None:
        logger.warning("모델 한도 부족으로 탐지 요청 거절 | job_id=%s 재시도=%d초", req.job_id, retry_after)
        raise RateLimited(
            "Model rate limit is nearly exhausted. Retry after the Retry-After period.",
            retry_after=retry_after,
            type="/errors/model-rate-limit",
            title="Model Rate Limit Exhausted",
            extensions={"remainingTpm": llm_limit.remaining(EXTRACTION_MODEL).remaining or 0},
        )

    _detect_jobs[req.job_id] = {
        "status": "QUEUED",
        "phase": None,
        "claim_count": None,
        "contradiction_count": 0,
        "findings": None,
        "detail": None,
    }
    task = asyncio.create_task(
        _run_detect(req.job_id, req.user_id, req.work_id, req.episode_number, req.text)
    )
    _running.add(task)
    task.add_done_callback(_running.discard)
    return JobAck(job_id=req.job_id, status="QUEUED")


def get_status(job_id: str) -> DetectStatus:
    """검사 하나의 진행 상태와 결과. 모르는 job_id는 404다."""
    state = _detect_jobs.get(job_id)
    if state is None:
        raise NotFound(f"detection job '{job_id}' not found")
    # 검사는 이벤트 루프에서, 이 조회는 FastAPI의 스레드풀에서 돈다. 필드를 하나씩 읽으면
    # 읽는 도중 상태가 바뀌어 "status는 새 값, findings는 옛 값" 같은 조합을 볼 수 있다.
    snapshot = dict(state)
    return DetectStatus(
        job_id=job_id,
        status=snapshot["status"],
        phase=snapshot.get("phase"),
        claim_count=snapshot.get("claim_count"),
        contradiction_count=snapshot.get("contradiction_count") or 0,
        findings=snapshot.get("findings"),
        detail=snapshot.get("detail"),
    )
