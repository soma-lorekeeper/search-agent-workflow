"""에러 응답 계약(RFC 9457 Problem Details) 테스트.

모든 에러 응답이 같은 모양인지를 고정한다:
  {"type": ..., "title": ..., "status": ..., "detail": ...} (+ 사유별 확장 멤버)
  + Content-Type: application/problem+json

개별 API 테스트(test_index_api, test_detect_api)는 각 에러의 **내용**(어떤 조건에서 어떤
detail이 나오는가)을 검증하고, 여기는 **모양**(어느 경로로 나가든 4필드와 Content-Type이
같은가)을 검증한다. 모양이 갈라지면 Spring 쪽 파싱이 경로마다 달라져야 하므로, 이 파일이
깨지면 스펙 문서(docs/*-api-spec.md)와 Spring 대응을 함께 봐야 한다.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from src import app as app_module
from src.common import llm_limit
from src.service.detect import job_service as detect_job_service
from src.service.index import job_service as index_job_service


@pytest.fixture
def client(monkeypatch):
    """깨끗한 서버 상태의 TestClient. 큐를 새로 만드는 이유는 test_index_api와 같다
    (asyncio.Queue가 처음 쓰인 이벤트 루프에 묶이므로 TestClient마다 새로 필요하다)."""
    index_job_service._index_jobs.clear()
    detect_job_service._detect_jobs.clear()
    monkeypatch.setattr(llm_limit, "_buckets", {})
    monkeypatch.setattr(index_job_service, "_index_queue", asyncio.Queue())
    with TestClient(app_module.app) as c:
        yield c


@pytest.fixture
def tolerant_client(monkeypatch):
    """500 검증용 TestClient.

    Starlette의 ServerErrorMiddleware는 커스텀 500 핸들러의 응답을 보낸 **뒤에도** 예외를
    다시 던진다. TestClient 기본값(raise_server_exceptions=True)은 그걸 그대로 올려
    응답을 볼 수 없으므로, 500 응답 본문을 검증하려면 이 플래그를 꺼야 한다.
    """
    index_job_service._index_jobs.clear()
    monkeypatch.setattr(llm_limit, "_buckets", {})
    monkeypatch.setattr(index_job_service, "_index_queue", asyncio.Queue())
    with TestClient(app_module.app, raise_server_exceptions=False) as c:
        yield c


def assert_problem(res, status: int) -> dict:
    """응답이 RFC 9457 모양인지 확인하고 본문을 돌려준다."""
    assert res.status_code == status
    assert res.headers["content-type"].startswith("application/problem+json")
    body = res.json()
    # 4필드가 모두 있고 status는 HTTP 상태코드와 일치해야 한다.
    assert set(body) >= {"type", "title", "status", "detail"}
    assert body["status"] == status
    assert isinstance(body["type"], str) and isinstance(body["title"], str)
    return body


# ---------- 400: 도메인 규칙 위반 (InvalidRequest) ----------


def test_400_is_problem_shaped(client):
    # 빈 episodes — 완료 마커 조회(Neo4j) 전에 걸리는 검증이라 스텁 없이 안전하다.
    res = client.post("/api/index", json={"userId": 1, "workId": 1, "episodes": []})
    body = assert_problem(res, 400)
    assert body["type"] == "/errors/invalid-request"
    assert body["detail"] == "episodes must not be empty"
    # 400 은 재시도 안내가 아니다 — Retry-After 가 붙으면 호출자가 같은 요청을 영원히 반복한다.
    assert "retry-after" not in res.headers


# ---------- 404: 모르는 jobId (NotFound) ----------


def test_404_is_problem_shaped(client):
    res = client.get("/api/detect/jobs/no-such-job")
    body = assert_problem(res, 404)
    assert body["type"] == "/errors/not-found"
    assert "no-such-job" in body["detail"]


# ---------- 429: 접수 거절 (RateLimited) ----------


def test_429_is_problem_shaped_with_retry_after(client, monkeypatch):
    # 동시 검사 상한을 0으로 낮춰 첫 요청부터 거절되게 한다 — Neo4j·LLM 없이 429 경로를 탄다.
    monkeypatch.setattr(detect_job_service, "MAX_CONCURRENT_DETECTS", 0)
    res = client.post(
        "/api/detect",
        json={"job_id": "j-1", "user_id": 1, "work_id": 1, "episode_number": 1, "text": "본문"},
    )
    body = assert_problem(res, 429)
    assert body["type"] == "/errors/too-many-detections"
    # Retry-After 헤더는 Spring 재시도 흐름이 의존하는 계약이다.
    assert int(res.headers["Retry-After"]) > 0
    # 확장 멤버는 top-level 에 그대로 실린다(스펙의 camelCase).
    assert body["runningDetections"] == 0


# ---------- 프레임워크 404/405 (StarletteHTTPException 편입) ----------


def test_unknown_path_404_is_problem_shaped(client):
    body = assert_problem(client.get("/no/such/path"), 404)
    # 상태코드 이상의 의미가 없는 프레임워크 예외라 type 은 RFC 기본값이다.
    assert body["type"] == "about:blank"


def test_method_not_allowed_405_is_problem_shaped(client):
    # /api/index 는 POST 전용이다.
    assert_problem(client.get("/api/index"), 405)


# ---------- 422: pydantic 검증 실패 (프레임워크 예외 편입) ----------


def test_422_is_problem_shaped(client):
    # episodes가 배열이 아니다 — 컨트롤러에 도달하기 전에 pydantic이 거른다.
    res = client.post("/api/index", json={"userId": 1, "workId": 1, "episodes": "not-a-list"})
    body = assert_problem(res, 422)
    assert body["type"] == "/errors/validation"
    # pydantic 오류 배열이 errors 확장 멤버로 보존된다 — 어떤 필드가 왜 틀렸는지가 단서다.
    assert isinstance(body["errors"], list) and body["errors"]
    assert body["errors"][0]["loc"]


# ---------- 500: 잡히지 않은 예외 (catch-all) ----------


def test_500_is_problem_shaped_and_hides_internals(tolerant_client, monkeypatch):
    # 조회 경로의 서비스를 강제로 터뜨린다 — 어떤 미처리 예외든 같은 500이 나가야 한다.
    def _boom(job_id: str):
        raise RuntimeError("내부 사정: DB 비밀번호 틀림 같은 민감한 메시지")

    monkeypatch.setattr(index_job_service, "get_status", _boom)
    res = tolerant_client.get("/api/index/jobs/any")
    body = assert_problem(res, 500)
    # 내부 예외 메시지는 응답에 노출되지 않는다 — 고정 문구여야 한다(스택은 로그로만).
    assert body["detail"] == "Internal server error"
    assert "내부 사정" not in res.text
