"""설정 오류 탐지 API 계약 테스트.

3단계 파이프라인(extract → retrieve → judge)을 통째로 스텁으로 갈아끼워 LLM·Neo4j·
PostgreSQL 비용 없이 **HTTP 계약만** 검증한다. 단계 안쪽의 규칙(줄 번호 전역성, 라우팅표,
τ 컷 같은 것)은 tests/test_detect_pipeline.py가 따로 본다.

스텁을 `create_completion`이 아니라 세 단계 함수에 거는 이유: 이 파일이 확인하려는 것은
"작업 상태와 응답 필드가 계약대로인가"이지 "LLM 응답을 어떻게 푸는가"가 아니다. 가짜 LLM
응답까지 만들어 넣으면 계약 검증이 프롬프트 형식 변화에 같이 깨진다.

확인하는 것:
  - 접수(202)와 중복 제출 방어
  - 진행 중 phase 전이(EXTRACT→RETRIEVE→JUDGE)와 그때 findings가 null인 것
  - 완료 응답의 findings 계약 전수 — 특히 score가 응답에 새어 나오지 않는 것
  - 실패 시 status=ERROR + detail
  - 모르는 jobId의 404
  - "status는 끝났는데 결과는 아직 없는" 찢어진 순간이 존재하지 않는 것
  - detection 테이블 기록(mark_running / save_result / mark_error)이 실제로 불리는 것

와이어 포맷은 camelCase(jobId/userId/workId/episodeNumber/claimCount/contradictionCount/
lineIds/isError)이고, status 어휘는 DB와 같은 대문자(QUEUED|RUNNING|DONE|ERROR)다.
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest
from conftest import RecordingDict  # tests/에 __init__.py가 없어 pytest가 경로에 넣어준다
from fastapi.testclient import TestClient

from src import app as app_module
from src.common import llm_limit
from src.common.tenant import Tenant
from src.repository.postgres import detection
from src.service.detect import extract_service, judge_service, retrieve_service
from src.service.detect import job_service as detect_job_service

# 기본 요청이 쓰는 테넌트. userId × workId 조합이 소설 한 편(KG 테넌트)을 가리킨다.
USER_ID = 42
WORK_ID = 1

# 추출이 뽑았다고 가정할 claim 두 개. lines는 원고 전역 줄 번호(L12 등)의 숫자 부분이다.
CLAIMS = [
    {"quote": "카엘은 검을 뽑아 들었다.", "axis": "카엘의 검 상태", "value": "온전함", "lines": [12, 13]},
    {"quote": "레아는 북부 출신이다.", "axis": "레아의 출신", "value": "북부", "lines": [40]},
]


def _fresh_claims() -> list[dict]:
    """매번 새 dict를 준다 — assign_claim_ids가 제자리에서 id를 박으므로 공유하면 오염된다."""
    claims = [dict(c) for c in CLAIMS]
    extract_service.assign_claim_ids(claims)  # P1, P2를 실제 코드가 붙인다
    return claims


def _finding_for(claim: dict) -> dict:
    """judge가 내는 finding 한 건의 실제 모양.

    `score`를 일부러 넣는다 — judge는 내부 판단용으로 점수를 들고 있고, 응답 DTO가 그걸
    떨어뜨리는지가 이 파일이 지켜야 할 계약이다(점수가 새면 작가 화면에 노출된다).
    """
    return {
        "claimId": claim["id"],
        "quote": claim["quote"],
        "axis": claim["axis"],
        "value": claim["value"],
        "lines": [{"lineNo": n, "text": f"{n}번째 줄."} for n in claim["lines"]],
        "isError": True,
        "score": 9,
        "reason": "3화에서 부러진 검을 다시 뽑아 들 수 없다.",
        "cited": [{"episodeNo": 3, "chunkIndex": 1, "text": "카엘의 검은 부러졌다."}],
    }


@pytest.fixture
def detect_client(monkeypatch):
    """깨끗한 작업 상태 + DB 쓰기를 막은 클라이언트.

    detection.* 는 실제 PostgreSQL에 붙으므로 전부 호출 기록만 남기는 가짜로 바꾼다.
    기록 리스트를 클라이언트에 달아 두어 테스트가 "무엇이 불렸는지"를 볼 수 있게 한다.
    """
    detect_job_service._detect_jobs.clear()
    # 접수 게이트의 마지막 안전망이 모델 버킷 잔량을 보므로 미터도 비운다.
    monkeypatch.setattr(llm_limit, "_buckets", {})

    calls: list[tuple] = []
    monkeypatch.setattr(detection, "mark_running", lambda job_id: calls.append(("mark_running", job_id)))
    monkeypatch.setattr(
        detection,
        "save_result",
        lambda job_id, claim_count, findings: calls.append(("save_result", job_id, claim_count, findings)),
    )
    monkeypatch.setattr(
        detection, "mark_error", lambda job_id, detail: calls.append(("mark_error", job_id, detail))
    )

    with TestClient(app_module.app) as c:
        c.db_calls = calls  # 테스트가 DB 호출 기록을 꺼내 볼 통로
        yield c


def _stub_pipeline(monkeypatch, *, claims=None, findings=None, evidence=None):
    """세 단계를 전부 가짜로 바꾸고, 각 단계가 받은 인자를 담은 dict를 돌려준다."""
    captured: dict = {}
    claims = _fresh_claims() if claims is None else claims
    evidence = {"records": []} if evidence is None else evidence
    findings = [] if findings is None else findings

    async def _extract(text, tenant, up_to_chapter=None):
        captured["extract"] = {"text": text, "tenant": tenant, "up_to_chapter": up_to_chapter}
        return claims, ["첫 줄", "둘째 줄"], {"calls": 1}

    async def _retrieve(cs, tenant, up_to_chapter=None):
        captured["retrieve"] = {"claims": cs, "tenant": tenant, "up_to_chapter": up_to_chapter}
        return evidence

    async def _judge(cs, ev, lines):
        captured["judge"] = {"claims": cs, "evidence": ev, "lines": lines}
        return findings

    monkeypatch.setattr(extract_service, "extract", _extract)
    monkeypatch.setattr(retrieve_service, "retrieve", _retrieve)
    monkeypatch.setattr(judge_service, "judge", _judge)
    return captured


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
    """조회를 반복해 조건을 만족하는 응답을 돌려준다. 못 만족하면 마지막 응답을 붙여 실패시킨다."""
    deadline = time.monotonic() + timeout
    body = None
    while time.monotonic() < deadline:
        body = client.get(f"/api/detect/jobs/{job_id}").json()
        if predicate(body):
            return body
        time.sleep(0.02)
    raise AssertionError(f"조건을 만족하지 못한 채 타임아웃: {body}")


# ---------- 접수와 중복 제출 ----------


def test_submit_returns_202_and_queued(detect_client, monkeypatch):
    """접수는 즉시 끝난다 — 회차 하나 검사에 LLM을 여러 번 부르므로 동기로 기다릴 수 없다."""
    _stub_pipeline(monkeypatch)
    res = _start(detect_client)

    assert res.status_code == 202
    assert res.json() == {"jobId": "job-1", "status": "QUEUED"}


def test_resubmitting_the_same_job_id_does_not_rerun(detect_client, monkeypatch):
    """중복 제출은 재실행 없이 **현재 상태만** 돌려준다.

    Spring이 재시도로 같은 jobId를 두 번 보내는 일이 실제로 있는데, 그때 검사가 두 번 돌면
    LLM 비용이 두 배가 되고 두 실행이 같은 DB 행을 서로 덮어쓴다.
    """
    gate = threading.Event()
    calls: list[int] = []

    async def _extract(text, tenant, up_to_chapter=None):
        calls.append(1)
        while not gate.is_set():  # 첫 실행을 RUNNING에 붙잡아 둔다
            await asyncio.sleep(0.01)
        return [], [], {}

    monkeypatch.setattr(extract_service, "extract", _extract)
    monkeypatch.setattr(retrieve_service, "retrieve", lambda *a, **k: {"records": []})

    assert _start(detect_client).status_code == 202
    try:
        _wait_until(detect_client, "job-1", lambda b: b["status"] == "RUNNING")
        again = _start(detect_client)
        assert again.status_code == 202
        assert again.json() == {"jobId": "job-1", "status": "RUNNING"}  # 이미 돌고 있는 그 작업
        assert calls == [1]  # 추출이 다시 시작되지 않았다
    finally:
        gate.set()


# ---------- 요청이 파이프라인까지 가는가 ----------


def test_request_reaches_the_pipeline_with_tenant_and_episode(detect_client, monkeypatch):
    """테넌트와 회차 상한이 둘 다 단계로 내려가야 한다.

    테넌트가 빠지면 남의 작품과 대조하고, 회차 상한이 빠지면 5화를 5화 자신이 만든 사실과
    대조해 "일치"라고 자평한다. 둘 다 답이 그럴듯해 보여서 특히 나쁘다 — 작가는 그 판정을
    자기 작품의 것으로 믿고 원고를 고친다.
    """
    captured = _stub_pipeline(monkeypatch)
    assert _start(detect_client, user_id=7, work_id=3, episode_number=5, text="5화 원고").status_code == 202
    _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")

    assert captured["extract"]["text"] == "5화 원고"
    assert captured["extract"]["tenant"] == Tenant.of(7, 3)
    assert captured["extract"]["up_to_chapter"] == 5
    assert captured["retrieve"]["tenant"] == Tenant.of(7, 3)
    assert captured["retrieve"]["up_to_chapter"] == 5
    # 판정은 추출이 뽑은 claim과 검색이 모은 근거를 그대로 받는다(중간에 갈아치우지 않는다).
    assert captured["judge"]["claims"] is captured["retrieve"]["claims"]
    # 추출이 만든 원고 줄 목록도 판정까지 간다 — 판정이 고른 줄 번호를 원문으로 되짚는
    # 통로다. 여기서 끊기면 findings의 lines에 text가 빈 채로 나간다.
    assert captured["judge"]["lines"] == ["첫 줄", "둘째 줄"]


# ---------- 완료 응답의 findings 계약 ----------


def test_done_response_carries_the_full_finding_contract(detect_client, monkeypatch):
    """완료 응답 전수 검사. 여기 있는 필드가 작가 화면이 그리는 전부다."""
    claims = _fresh_claims()
    findings = [_finding_for(claims[0])]
    _stub_pipeline(monkeypatch, claims=claims, findings=findings)

    assert _start(detect_client).status_code == 202
    body = _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")

    assert body["jobId"] == "job-1"
    assert body["status"] == "DONE"
    assert body["phase"] is None  # 진행 중일 때만 값이 있다
    assert body["detail"] is None
    assert body["claimCount"] == 2  # 검사한 총량 — findings는 그중 오류만이라 수가 다르다
    assert body["contradictionCount"] == len(body["findings"]) == 1

    (finding,) = body["findings"]
    # 필드 집합을 통째로 못박는다. score가 응답에 끼면 여기서 걸린다 — 점수는 임계값을
    # 정하려고 만든 내부 값이고, 작가 화면에 "9점짜리 모순"을 보여줄 계약이 아니다.
    assert set(finding) == {
        "claimId", "quote", "axis", "value", "lines", "isError", "reason", "cited",
    }
    assert finding["claimId"] == "P1"  # 추출 순서 = 원고 등장 순서라 화면 정렬에 그대로 쓴다
    assert finding["quote"] == CLAIMS[0]["quote"]
    assert finding["axis"] == CLAIMS[0]["axis"]
    assert finding["value"] == CLAIMS[0]["value"]
    # 줄 번호에 원문이 딸려 온다 — 번호만으로는 받는 쪽이 어느 문장인지 알 수 없다.
    assert finding["lines"] == [{"lineNo": n, "text": f"{n}번째 줄."} for n in CLAIMS[0]["lines"]]
    assert finding["isError"] is True
    assert finding["reason"]
    # cited도 좌표에 근거 원문이 함께 실린다(같은 이유).
    assert finding["cited"] == [{"episodeNo": 3, "chunkIndex": 1, "text": "카엘의 검은 부러졌다."}]


def test_no_error_found_is_an_empty_list_not_null(detect_client, monkeypatch):
    """오류 0건은 빈 배열이다 — null은 "아직 안 끝났다"는 다른 뜻이라 섞으면 안 된다."""
    _stub_pipeline(monkeypatch, findings=[])

    assert _start(detect_client).status_code == 202
    body = _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")

    assert body["findings"] == []
    assert body["contradictionCount"] == 0
    assert body["claimCount"] == 2  # 검사는 했다


# ---------- 진행 중 phase ----------


def test_phase_advances_through_the_three_stages(detect_client, monkeypatch):
    """UI는 claim 단위 진행률 대신 "지금 어느 단계인가"로 진행을 그린다.

    판정은 한 번의 배치 호출이라 claim별 진행이라는 게 없다. 대신 세 단계가 순서대로
    보여야 하고, 그동안 findings는 계속 null이어야 한다(폴링을 멈추면 안 되므로).
    """
    gates = {"EXTRACT": threading.Event(), "RETRIEVE": threading.Event(), "JUDGE": threading.Event()}

    async def _hold(name):
        # 이벤트 루프를 막지 않고 기다린다 — 막으면 같은 루프의 다른 작업이 굶는다.
        while not gates[name].is_set():
            await asyncio.sleep(0.01)

    claims = _fresh_claims()

    async def _extract(text, tenant, up_to_chapter=None):
        await _hold("EXTRACT")
        return claims, [], {}

    async def _retrieve(cs, tenant, up_to_chapter=None):
        await _hold("RETRIEVE")
        return {"records": []}

    async def _judge(cs, ev, lines):
        await _hold("JUDGE")
        return []

    monkeypatch.setattr(extract_service, "extract", _extract)
    monkeypatch.setattr(retrieve_service, "retrieve", _retrieve)
    monkeypatch.setattr(judge_service, "judge", _judge)

    assert _start(detect_client).status_code == 202
    try:
        body = _wait_until(detect_client, "job-1", lambda b: b["phase"] == "EXTRACT")
        assert body["status"] == "RUNNING"
        assert body["findings"] is None
        assert body["claimCount"] is None  # 추출이 끝나야 알 수 있는 값이다
        gates["EXTRACT"].set()

        body = _wait_until(detect_client, "job-1", lambda b: b["phase"] == "RETRIEVE")
        assert body["status"] == "RUNNING"
        assert body["findings"] is None
        assert body["claimCount"] == len(claims)  # 여기서부터 진행률을 그릴 수 있다
        gates["RETRIEVE"].set()

        body = _wait_until(detect_client, "job-1", lambda b: b["phase"] == "JUDGE")
        assert body["status"] == "RUNNING"
        assert body["findings"] is None
    finally:
        for g in gates.values():
            g.set()

    body = _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")
    assert body["phase"] is None


# ---------- 실패 ----------


def test_failure_reports_error_status_with_detail(detect_client, monkeypatch):
    """실패 사유가 응답에 실려야 호출자가 "왜 안 됐는지"를 보여줄 수 있다."""

    async def _extract(text, tenant, up_to_chapter=None):
        raise RuntimeError("그래프 접속 실패")

    monkeypatch.setattr(extract_service, "extract", _extract)

    assert _start(detect_client).status_code == 202
    body = _wait_until(detect_client, "job-1", lambda b: b["status"] == "ERROR")

    assert body["detail"] == "그래프 접속 실패"
    assert body["phase"] is None
    assert body["findings"] is None  # 실패는 "오류 0건"이 아니다
    assert body["contradictionCount"] == 0


# ---------- 조회(404) ----------


def test_unknown_job_is_404(detect_client):
    res = detect_client.get("/api/detect/jobs/모르는-작업")
    assert res.status_code == 404
    assert "detail" in res.json()


# ---------- DB 기록 ----------


def test_success_path_writes_running_then_result(detect_client, monkeypatch):
    """메모리 상태와 별개로 Spring 테이블에도 시작·결과가 남아야 한다.

    이 서버가 재시작하면 메모리 상태는 사라진다 — 그때 "무엇이 어디까지 갔는지"는 그
    테이블에만 남는다.
    """
    claims = _fresh_claims()
    findings = [_finding_for(claims[0])]
    _stub_pipeline(monkeypatch, claims=claims, findings=findings)

    assert _start(detect_client).status_code == 202
    _wait_until(detect_client, "job-1", lambda b: b["status"] == "DONE")

    assert detect_client.db_calls == [
        ("mark_running", "job-1"),
        ("save_result", "job-1", 2, findings),  # claim 총수와 오류 목록을 함께 넣는다
    ]


def test_failure_path_writes_running_then_error(detect_client, monkeypatch):
    """실패도 테이블에 남아야 한다 — 안 남기면 그 행이 영원히 RUNNING으로 굳는다."""

    async def _extract(text, tenant, up_to_chapter=None):
        raise RuntimeError("그래프 접속 실패")

    monkeypatch.setattr(extract_service, "extract", _extract)

    assert _start(detect_client).status_code == 202
    _wait_until(detect_client, "job-1", lambda b: b["status"] == "ERROR")

    assert detect_client.db_calls == [
        ("mark_running", "job-1"),
        ("mark_error", "job-1", "그래프 접속 실패"),
    ]


# ---------- 찢어진 읽기(status와 결과가 따로 쓰이는 순간) ----------


def _blank_state() -> RecordingDict:
    """submit()이 만드는 것과 같은 초기 상태. 쓰기가 일어날 때마다 스냅샷이 쌓인다."""
    return RecordingDict(
        {
            "status": "QUEUED",
            "phase": None,
            "claim_count": None,
            "contradiction_count": 0,
            "findings": None,
            "detail": None,
        }
    )


def test_done_never_appears_without_findings(monkeypatch):
    """status="DONE" + findings=null을 본 폴링은 폴링을 멈추고 "오류 0건"으로 확정한다.

    조회는 FastAPI 스레드풀에서, 검사는 이벤트 루프에서 도니까 두 줄로 나눠 쓰면 그 사이의
    한 순간이 실제로 관측된다. 그래서 그런 순간이 애초에 존재하지 않아야 한다.
    """
    claims = _fresh_claims()
    findings = [_finding_for(claims[0])]
    _stub_pipeline(monkeypatch, claims=claims, findings=findings)
    monkeypatch.setattr(detection, "mark_running", lambda job_id: None)
    monkeypatch.setattr(detection, "save_result", lambda job_id, claim_count, fs: None)

    state = _blank_state()
    detect_job_service._detect_jobs["job-torn"] = state
    try:
        asyncio.run(detect_job_service._run_detect("job-torn", USER_ID, WORK_ID, 5, "5화 원고"))
    finally:
        detect_job_service._detect_jobs.pop("job-torn", None)

    assert state["status"] == "DONE" and state["findings"] == findings
    assert [s for s in state.snapshots if s["status"] == "DONE" and not s.get("findings")] == []
    # 카운트도 같은 이유로 함께 쓰여야 한다 — DONE인데 0건으로 보이는 순간이 없어야 한다.
    assert [
        s for s in state.snapshots if s["status"] == "DONE" and s.get("contradiction_count") == 0
    ] == []


def test_error_never_appears_without_a_reason(monkeypatch):
    """실패도 마찬가지다 — status="ERROR" + detail=null은 "사유 없는 실패"로 저장된다."""

    async def _extract(text, tenant, up_to_chapter=None):
        raise RuntimeError("그래프 접속 실패")

    monkeypatch.setattr(extract_service, "extract", _extract)
    monkeypatch.setattr(detection, "mark_running", lambda job_id: None)
    monkeypatch.setattr(detection, "mark_error", lambda job_id, detail: None)

    state = _blank_state()
    detect_job_service._detect_jobs["job-torn"] = state
    try:
        asyncio.run(detect_job_service._run_detect("job-torn", USER_ID, WORK_ID, 5, "5화 원고"))
    finally:
        detect_job_service._detect_jobs.pop("job-torn", None)

    assert state["status"] == "ERROR" and state["detail"] == "그래프 접속 실패"
    assert [s for s in state.snapshots if s["status"] == "ERROR" and not s.get("detail")] == []


# ---------- 접수 게이트(429) ----------


def test_동시_검사_상한을_넘으면_429(detect_client, monkeypatch):
    """탐지는 큐 없이 전부 동시에 돈다(create_task) — 밀리는 게 아니라 한꺼번에 터진다.

    그래서 인덱싱처럼 "대기 시간"이 아니라 **동시 실행 수**로 잰다. 상한이 없으면 검사
    10건이 겹쳐 LLM 수십 콜 + 임베딩 수천 콜이 한꺼번에 나간다.
    """
    monkeypatch.setattr(detect_job_service, "MAX_CONCURRENT_DETECTS", 2)

    gate = threading.Event()

    async def _blocking_extract(text, tenant, up_to_chapter):
        await asyncio.to_thread(gate.wait, 5)
        return [], [], {}

    monkeypatch.setattr(extract_service, "extract", _blocking_extract)

    for n in range(2):
        assert _start(detect_client, job_id=f"job-{n}").status_code == 202

    res = _start(detect_client, job_id="job-overflow")
    assert res.status_code == 429
    body = res.json()
    assert body["detail"] == "Too many detections in progress. Retry after the Retry-After period."
    assert body["runningDetections"] == 2
    assert int(res.headers["Retry-After"]) > 0
    # 거절된 검사는 상태에 남지 않는다 — 남으면 폴링이 영원히 QUEUED를 본다.
    assert "job-overflow" not in detect_job_service._detect_jobs

    gate.set()


def test_이미_접수한_검사의_재제출은_게이트를_통과한다(detect_client, monkeypatch):
    """중복 제출 방어가 게이트보다 **먼저** 와야 한다.

    뒤에 두면 서버가 바쁠 때 진행 중인 검사의 상태 조회조차 429가 되어, 호출자가 결과를
    영영 못 받는다. 재제출은 자원을 쓰지 않으므로 거절할 이유가 없다.
    """
    monkeypatch.setattr(detect_job_service, "MAX_CONCURRENT_DETECTS", 1)

    gate = threading.Event()

    async def _blocking_extract(text, tenant, up_to_chapter):
        await asyncio.to_thread(gate.wait, 5)
        return [], [], {}

    monkeypatch.setattr(extract_service, "extract", _blocking_extract)

    assert _start(detect_client, job_id="job-1").status_code == 202
    # 상한이 1이라 새 검사는 429지만, 같은 jobId 재제출은 통과해야 한다.
    assert _start(detect_client, job_id="job-2").status_code == 429
    again = _start(detect_client, job_id="job-1")
    assert again.status_code == 202
    assert again.json()["jobId"] == "job-1"

    gate.set()


def test_모델_한도가_바닥이면_탐지도_거절한다(detect_client, monkeypatch):
    """인덱싱이 같은 EXTRACTION_MODEL 버킷을 비웠을 수 있다 — 공통 안전망이 그걸 본다."""
    from src.config import EXTRACTION_MODEL

    llm_limit.observe(
        EXTRACTION_MODEL,
        {
            "x-ratelimit-limit-tokens": "200000",
            "x-ratelimit-remaining-tokens": "100",
            "x-ratelimit-reset-tokens": "30s",
        },
    )

    res = _start(detect_client, job_id="job-budget")
    assert res.status_code == 429
    assert "rate limit" in res.json()["detail"].lower()
    assert "job-budget" not in detect_job_service._detect_jobs
