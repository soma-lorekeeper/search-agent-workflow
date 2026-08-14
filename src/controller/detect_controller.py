"""설정 오류 탐지 API. 인덱싱과 달리 job_id를 Spring이 발급하고 필드도 snake_case다."""

from fastapi import APIRouter

from src.dto.detect_dto import DetectRequest, DetectStatus, JobAck
from src.service.detect import job_service

router = APIRouter()


@router.post("/api/detect", response_model=JobAck, status_code=202)
async def start_detect(req: DetectRequest) -> JobAck:
    """설정 오류 탐지를 백그라운드로 시작하고 즉시 응답한다."""
    return await job_service.submit(req)


@router.get("/api/detect/{job_id}", response_model=DetectStatus)
def get_detect_job(job_id: str) -> DetectStatus:
    """검사 하나의 진행 상태와 결과. 모르는 job_id는 404다."""
    return job_service.get_status(job_id)
