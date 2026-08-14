"""인덱싱 API. 요청·응답 필드는 스펙대로 camelCase다(dto/common.CamelModel)."""

from fastapi import APIRouter

from src.dto.index_dto import IndexAccepted, IndexJobStatus, IndexRequest
from src.service.index import job_service

router = APIRouter()


@router.post("/api/index", response_model=IndexAccepted, status_code=201)
async def index_episodes(req: IndexRequest):
    """여러 화를 한 작업으로 접수하고 즉시 응답한다(실제 처리는 백그라운드 워커)."""
    return await job_service.submit(req)


@router.get("/api/index/jobs/{job_id}", response_model=IndexJobStatus)
def get_index_job(job_id: str) -> IndexJobStatus:
    """작업 하나의 화별 진행 상태. 모르는 jobId는 404다."""
    return job_service.get_status(job_id)
