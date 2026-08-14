"""설정 오류 탐지 작업의 접수·실행·상태를 관리한다.

인덱싱과 달리 회차 순서를 지킬 필요가 없다(검사 대상 회차 하나를 그 시점의 그래프와
대조할 뿐, 검사끼리 서로 의존하지 않는다). 그래서 큐 없이 요청마다
asyncio.create_task로 바로 백그라운드에 띄운다 — 요청 커넥션과 무관하게 계속 진행되는
성질은 인덱싱 워커와 동일하다.
"""

import asyncio
import logging

from fastapi import HTTPException

from src.contradiction import save_report_files
from src.contradiction.pipeline import check_new_episode_streaming
from src.dto.detect_dto import DetectRequest, DetectStatus, JobAck
from src.service.kg_scope import kg_scope

logger = logging.getLogger("detect")

_detect_jobs: dict[str, dict] = {}  # job_id -> {"status": ..., "claims": [...], "findings": [...]}


async def _run_detect(
    job_id: str, user_id: int, work_id: int, episode_number: int, text: str
) -> None:
    def on_claims_extracted(claims: list[dict]) -> None:
        # claim 추출 직후 시점 — 호출자가 이 시점부터 claim별 진행 목록을 그릴 수 있게 한다.
        # claim은 LLM이 만든 JSON이라 키가 있어도 값이 null일 수 있다. 기본값을 or로 씌워
        # 조회 API가 응답 검증에서 500을 내지 않게 한다(진행 조회는 무슨 일이 있어도 살아야 한다).
        _detect_jobs[job_id]["claims"] = [
            {
                "index": i,
                "quote": c.get("quote") or "",
                "category": c.get("category") or "기타",
                "status": "RUNNING",
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
                "status": "DONE",
                "label": result.get("label"),
                "established_fact": result.get("established_fact"),
                "source_episode": result.get("source_episode"),
                "explanation": result.get("explanation"),
            }
        )

    _detect_jobs[job_id]["status"] = "RUNNING"
    try:
        # 인덱싱 워커와 같은 이유로 여기서도 kg_scope를 통과시킨다 — 요청을 KG 테넌트로
        # 바꾸는 지점은 이 프로젝트에서 kg_scope 하나뿐이어야 한다.
        tenant = kg_scope(user_id, work_id)
        # episode_number를 파이프라인에 넘긴다. 이게 없으면 검사 대상 회차를 **자기 자신을 포함한
        # 그래프 전체**와 대조하게 된다 — 5화를 5화가 만든 사실과 비교해 "일치"라고 자평하고,
        # 6~10화가 나중에 밝힌 반전을 5화에 심어둔 모순으로 읽는다.
        findings = await check_new_episode_streaming(
            text, tenant, episode_number, on_claims_extracted, on_claim_done
        )
        # 리포트는 파일 두 개(md+json)를 쓴다 — 이벤트 루프에서 직접 쓰지 않는다.
        await asyncio.to_thread(
            save_report_files, findings, job_id, display_label=f"{episode_number}화"
        )
        # 결과와 상태를 한 번의 update로 쓴다. 조회 API(get_status)는 다른 스레드에서 도는데,
        # 두 줄로 나눠 쓰면 그 사이에 들어온 폴링이 status="done" + findings=null을 보고,
        # 호출자는 폴링을 멈춘 뒤 "오류 0건"인 빈 리포트를 확정 저장한다.
        _detect_jobs[job_id].update({"findings": findings, "status": "DONE"})
    except Exception as exc:  # noqa: BLE001 — 실패 사유를 상태로 노출해야 호출자가 보여줄 수 있다
        logger.exception("설정 오류 탐지 실패 | job_id=%s episode=%s", job_id, episode_number)
        # 같은 이유로 사유와 상태를 함께 쓴다(status="error" + detail=null을 보이지 않게).
        _detect_jobs[job_id].update({"detail": str(exc), "status": "ERROR"})


async def submit(req: DetectRequest) -> JobAck:
    """설정 오류 탐지를 백그라운드로 시작하고 즉시 응답한다. 실제 검사(claim 추출 → claim별
    병렬 검증 → 판정 집계)는 이 요청의 커넥션과 무관하게 진행된다."""
    known = _detect_jobs.get(req.job_id)
    if known is not None:
        # 인덱싱과 같은 이유(재시도 방어). 여기선 그래프가 더러워지진 않지만 회차 하나 검사에
        # 수십 번의 LLM 호출이 들어가므로 중복 실행 비용이 특히 크다.
        return JobAck(job_id=req.job_id, status=known["status"])

    _detect_jobs[req.job_id] = {"status": "QUEUED", "claims": []}
    asyncio.create_task(
        _run_detect(req.job_id, req.user_id, req.work_id, req.episode_number, req.text)
    )
    return JobAck(job_id=req.job_id, status="QUEUED")


def get_status(job_id: str) -> DetectStatus:
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
