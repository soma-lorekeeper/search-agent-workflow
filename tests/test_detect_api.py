"""설정 오류 탐지 API 계약 테스트.

파이프라인(check_new_episode_streaming)과 리포트 저장을 스텁으로 갈아끼워 LLM·Neo4j 비용
없이 계약만 검증한다. 확인하는 것: 요청의 (userId, workId)가 KG 테넌트로 파이프라인까지
내려가는지, 검사 대상 회차 번호가 전달되는지, 검사 중 진행 목록(claims)의 모양, 모르는
jobId의 404, 그리고 "status는 끝났는데 결과는 아직 없는" 순간이 존재하지 않는지.

와이어 포맷은 camelCase다(jobId/userId/workId/episodeNumber, claims의 establishedFact·
sourceEpisode). status 어휘는 DB와 같은 대문자(QUEUED|RUNNING|DONE|ERROR)다.
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest
from conftest import RecordingDict  # tests/에 __init__.py가 없어 pytest가 경로에 넣어준다
from fastapi.testclient import TestClient

from src import app as app_module
from src.common.tenant import Tenant
from src.service.detect import job_service as detect_job_service

# 기본 요청이 쓰는 테넌트. userId × workId 조합이 소설 한 편(KG 테넌트)을 가리킨다.
USER_ID = 42
WORK_ID = 1

# 파이프라인이 추출했다고 가정할 claim 두 개.
CLAIMS = [
    {"quote": "카엘은 검을 들었다.", "category": "소유물", "entities": ["카엘"]},
    {"quote": "레아는 북부 출신이다.", "category": "소속", "entities": ["레아"]},
]


@pytest.fixture
def detect_client(monkeypatch):
    """매 테스트마다 깨끗한 탐지 작업 상태로 시작한다."""
    detect_job_service._detect_jobs.clear()
    # 리포트 파일은 이 테스트의 관심사가 아니다(실제 reports/를 건드리지 않게 막는다).
    monkeypatch.setattr(detect_job_service, "save_report_files", lambda *args, **kwargs: {})
    with TestClient(app_module.app) as c:
        yield c


def _start(
    client, job_id="job-1", user_id=USER_ID, work_id=WORK_ID, episode_number=5, text="5화 원고"
):
    return client.post(
        "/api/detect",
        json={
            "jobId": job_id,
            "userId": user_id,
            "workId": work_id,
            "episodeNumber": episode_number,
            "text": text,
        },
    )


def _wait_until(client, job_id, predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    body = None
    while time.monotonic() < deadline:
        body = client.get(f"/api/detect/jobs/{job_id}").json()
        if predicate(body):
            return body
        time.sleep(0.02)
    raise AssertionError(f"조건을 만족하지 못한 채 타임아웃: {body}")


# ---------- 요청이 파이프라인까지 가는가 ----------


def test_episode_number_is_passed_to_the_pipeline(detect_client, monkeypatch):
    """episode_number가 리포트 제목에만 쓰이고 사라지면, 5화를 5화 자신과 대조하게 된다."""
    captured: dict = {}

    async def _stub(text, tenant, up_to_chapter=None, on_claims_extracted=None, on_claim_done=None):
        captured["text"] = text
        captured["up_to_chapter"] = up_to_chapter
        return []

    monkeypatch.setattr(detect_job_service, "check_new_episode_streaming", _stub)
    assert _start(detect_client, episode_number=5, text="5화 원고").status_code == 202
    _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")

    assert captured["text"] == "5화 원고"
    assert captured["up_to_chapter"] == 5


def test_tenant_is_passed_to_the_pipeline(detect_client, monkeypatch):
    """검사도 그래프 전체를 "기존 설정"으로 읽는다 — 테넌트가 빠지면 남의 작품과 대조하게 된다.

    답이 그럴듯해 보이는 게 특히 나쁘다: 작가는 자기 작품의 판정으로 알고 원고를 고친다.
    """
    captured: dict = {}

    async def _stub(text, tenant, up_to_chapter=None, on_claims_extracted=None, on_claim_done=None):
        captured["tenant"] = tenant
        return []

    monkeypatch.setattr(detect_job_service, "check_new_episode_streaming", _stub)
    assert _start(detect_client, user_id=7, work_id=3).status_code == 202
    _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")

    assert captured["tenant"] == Tenant.of(7, 3)


# ---------- 검사 중 진행 목록(claims) ----------


def test_claims_progress_is_visible_while_running(detect_client, monkeypatch):
    """UI가 검사가 끝나기 전에 claim별 진행 상황을 그릴 수 있어야 한다."""
    gate = threading.Event()

    async def _stub(text, tenant, up_to_chapter=None, on_claims_extracted=None, on_claim_done=None):
        on_claims_extracted(CLAIMS)
        # 첫 claim만 먼저 끝난다 — 병렬 검증이라 index 순서대로 끝나지 않는다는 사실을 반영.
        on_claim_done(
            0,
            {
                "label": "contradiction",
                "established_fact": "카엘의 검은 3화에서 부러졌다",
                # 모델이 숫자가 아니라 "3화"로 답하는 경우가 있다 — 조회가 500이 나면 안 된다.
                "source_episode": "3화",
                "explanation": "부러진 검을 다시 들 수 없다",
            },
        )
        while not gate.is_set():
            await asyncio.sleep(0.01)
        return [{"quote": CLAIMS[0]["quote"], "label": "contradiction"}]

    monkeypatch.setattr(detect_job_service, "check_new_episode_streaming", _stub)
    assert _start(detect_client).status_code == 202

    try:
        body = _wait_until(detect_client, "job-1", lambda b: b["claims"])
        assert body["status"] == "RUNNING"
        assert body["findings"] is None  # 아직 끝나지 않았다

        first, second = body["claims"]
        assert first["index"] == 0
        assert first["quote"] == "카엘은 검을 들었다."
        assert first["category"] == "소유물"
        assert first["status"] == "DONE"
        assert first["label"] == "contradiction"
        assert first["establishedFact"] == "카엘의 검은 3화에서 부러졌다"
        assert first["sourceEpisode"] == "3화"

        # 아직 검증 중인 claim도 같은 모양으로 보이되 판정 필드만 비어 있다.
        assert second["index"] == 1
        assert second["quote"] == "레아는 북부 출신이다."
        assert second["category"] == "소속"
        assert second["status"] == "RUNNING"
        assert second["label"] is None
        assert second["establishedFact"] is None
        assert second["explanation"] is None
    finally:
        gate.set()

    body = _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")
    assert body["findings"] == [{"quote": "카엘은 검을 들었다.", "label": "contradiction"}]
    assert body["claims"][0]["index"] == 0  # 끝난 뒤에도 진행 목록은 그대로 남는다


def test_claims_is_an_empty_list_before_extraction(detect_client, monkeypatch):
    """접수 직후엔 claim이 아직 없다 — null이 아니라 빈 배열이어야 프론트가 분기 없이 그린다."""
    gate = threading.Event()

    async def _stub(text, tenant, up_to_chapter=None, on_claims_extracted=None, on_claim_done=None):
        while not gate.is_set():
            await asyncio.sleep(0.01)
        return []

    monkeypatch.setattr(detect_job_service, "check_new_episode_streaming", _stub)
    assert _start(detect_client).status_code == 202
    try:
        body = detect_client.get("/api/detect/jobs/job-1").json()
        assert body["claims"] == []
        assert body["findings"] is None
    finally:
        gate.set()


def test_claim_without_category_still_renders(detect_client, monkeypatch):
    """claim은 LLM이 만든 JSON이라 값이 null일 수 있다 — 진행 조회가 그걸로 500이 나면 안 된다."""

    async def _stub(text, tenant, up_to_chapter=None, on_claims_extracted=None, on_claim_done=None):
        on_claims_extracted([{"quote": None, "category": None}])
        return []

    monkeypatch.setattr(detect_job_service, "check_new_episode_streaming", _stub)
    assert _start(detect_client).status_code == 202
    body = _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")
    assert body["claims"] == [
        {
            "index": 0,
            "quote": "",
            "category": "기타",
            "status": "RUNNING",
            "label": None,
            "establishedFact": None,
            "sourceEpisode": None,
            "explanation": None,
        }
    ]


# ---------- 조회(404) ----------


def test_unknown_job_is_404(detect_client):
    res = detect_client.get("/api/detect/jobs/모르는-작업")
    assert res.status_code == 404
    assert "detail" in res.json()


# ---------- 터진 읽기(status와 결과가 따로 쓰이는 순간) ----------


def test_done_never_appears_without_findings(monkeypatch):
    """status="DONE" + findings=null을 본 폴링은 폴링을 멈추고 "오류 0건" 리포트를 확정 저장한다.

    조회는 FastAPI 스레드풀에서, 검사는 이벤트 루프에서 도니까 그 한 순간은 실제로 관측된다.
    그래서 그런 순간이 애초에 존재하지 않아야 한다.
    """
    findings = [{"quote": "카엘은 검을 들었다.", "label": "contradiction"}]

    async def _stub(text, tenant, up_to_chapter=None, on_claims_extracted=None, on_claim_done=None):
        return findings

    monkeypatch.setattr(detect_job_service, "check_new_episode_streaming", _stub)
    monkeypatch.setattr(detect_job_service, "save_report_files", lambda *args, **kwargs: {})
    state = RecordingDict({"status": "QUEUED", "claims": []})
    detect_job_service._detect_jobs["job-torn"] = state
    try:
        asyncio.run(
            detect_job_service._run_detect("job-torn", USER_ID, WORK_ID, 5, "5화 원고")
        )
    finally:
        detect_job_service._detect_jobs.pop("job-torn", None)

    assert state["status"] == "DONE" and state["findings"] == findings
    assert [s for s in state.snapshots if s["status"] == "DONE" and not s.get("findings")] == []


def test_error_never_appears_without_a_reason(monkeypatch):
    """실패도 마찬가지다 — status="ERROR" + detail=null은 "사유 없는 실패"로 저장된다."""

    async def _stub(text, tenant, up_to_chapter=None, on_claims_extracted=None, on_claim_done=None):
        raise RuntimeError("그래프 접속 실패")

    monkeypatch.setattr(detect_job_service, "check_new_episode_streaming", _stub)
    state = RecordingDict({"status": "QUEUED", "claims": []})
    detect_job_service._detect_jobs["job-torn"] = state
    try:
        asyncio.run(
            detect_job_service._run_detect("job-torn", USER_ID, WORK_ID, 5, "5화 원고")
        )
    finally:
        detect_job_service._detect_jobs.pop("job-torn", None)

    assert state["status"] == "ERROR" and state["detail"] == "그래프 접속 실패"
    assert [s for s in state.snapshots if s["status"] == "ERROR" and not s.get("detail")] == []
