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

from fastapi import HTTPException

from src.dto.detect_dto import DetectRequest, DetectStatus, JobAck
from src.repository.postgres import detection
from src.service.detect import extract_service, judge_service, retrieve_service
from src.service.kg_scope import kg_scope

logger = logging.getLogger("detect")

# job_id -> {"status", "phase", "claim_count", "contradiction_count", "findings", "detail"}
_detect_jobs: dict[str, dict] = {}


async def _run_detect(
    job_id: str, user_id: int, work_id: int, episode_number: int, text: str
) -> None:
    tenant = kg_scope(user_id, work_id)
    state = _detect_jobs[job_id]
    state.update({"status": "RUNNING", "phase": "EXTRACT"})
    await asyncio.to_thread(detection.mark_running, job_id)

    try:
        claims, _lines, _tokens = await extract_service.extract(text, tenant, episode_number)
        # claim 수는 추출이 끝나야 알 수 있다. 여기서부터 조회가 진행률을 보여줄 수 있다.
        state.update({"phase": "RETRIEVE", "claim_count": len(claims)})

        evidence = await retrieve_service.retrieve(claims, tenant, episode_number)
        state["phase"] = "JUDGE"

        findings = await judge_service.judge(claims, evidence)

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
    """검사를 백그라운드로 시작하고 즉시 응답한다."""
    known = _detect_jobs.get(req.job_id)
    if known is not None:
        # 중복 제출 방어. 회차 하나 검사에 LLM을 수십 번 부르므로 중복 실행 비용이 크다.
        return JobAck(job_id=req.job_id, status=known["status"])

    _detect_jobs[req.job_id] = {
        "status": "QUEUED",
        "phase": None,
        "claim_count": None,
        "contradiction_count": 0,
        "findings": None,
        "detail": None,
    }
    asyncio.create_task(
        _run_detect(req.job_id, req.user_id, req.work_id, req.episode_number, req.text)
    )
    return JobAck(job_id=req.job_id, status="QUEUED")


def get_status(job_id: str) -> DetectStatus:
    """검사 하나의 진행 상태와 결과. 모르는 job_id는 404다."""
    state = _detect_jobs.get(job_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"'{job_id}' 탐지 작업 기록이 없습니다.")
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
