from fastapi import APIRouter

from src.service.health_service import collect as collect_health

router = APIRouter()


@router.get("/api/health")
def health() -> dict:
    """이 서버가 두 DB 에 실제로 닿는지 점검한다.

    API 서버(Spring)가 이걸 호출해 자기 점검 결과와 합쳐 프론트에 내려준다.
    프로덕션에서 이 서버는 127.0.0.1 에만 떠 있어 외부에서 직접 부를 수 없다.

    DB 가 죽어도 HTTP 200 을 준다 — 상태는 본문의 status 로 구분한다. 여기서 5xx 를
    내면 "에이전트가 죽음"과 "에이전트는 살아있고 DB 만 죽음"을 호출자가 구분하지 못한다.
    """
    return collect_health()
