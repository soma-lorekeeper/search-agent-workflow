"""설정 오류 탐지 API.

인덱싱과 달리 jobId를 Spring이 발급한다 — 검사 요청 하나가 회차 하나라 호출자가 부여한
id를 그대로 쓰는 편이 단순하다. 조회 경로는 인덱싱과 같은 모양(`/jobs/{id}`)으로 맞춘다.
"""

from fastapi import APIRouter

from src.dto.detect_dto import DetectRequest, DetectStatus, JobAck
from src.service.detect import job_service

router = APIRouter()


@router.post("/api/detect", response_model=JobAck, status_code=202)
async def start_detect(req: DetectRequest) -> JobAck:
    """설정 오류 탐지를 백그라운드로 시작하고 즉시 응답한다.

    여유가 없으면 429로 거절한다(동시 검사 상한 또는 모델 한도 소진) — service가
    RateLimited를 던지고 error_handlers가 응답으로 바꾸므로, 이 함수는 항상 JobAck만
    반환한다.
    """
    return await job_service.submit(req)


@router.get("/api/detect/jobs/{job_id}", response_model=DetectStatus)
def get_detect_job(job_id: str) -> DetectStatus:
    """검사 하나의 진행 상태와 결과. 모르는 job_id는 404다."""
    return job_service.get_status(job_id)
