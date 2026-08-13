"""인덱싱 API 스모크 테스트.

실제 인덱싱(run_indexing)과 Neo4j 완료 마커 조회(_already_indexed)를 전부 스텁으로 갈아끼워
LLM·DB 비용 없이 계약만 검증한다. 확인하는 것: 201 응답 모양, 400 검증, 429(TPM) 경로,
화별 상태 전이(waiting→running→done), 실패 시 뒤 화 연쇄 스킵, 모르는 jobId의 404,
그리고 이미 인덱싱된 화의 빠른 경로.

TestClient는 앱을 별도 스레드의 이벤트 루프에서 돌리므로, 백그라운드 워커가 진행하는 동안
테스트 스레드가 조회 API를 폴링할 수 있다(_wait_until).
"""

from __future__ import annotations

import asyncio
import re
import threading
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from src import webapp

RFC3339_UTC = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"


@pytest.fixture
def stub_indexing(monkeypatch):
    """run_indexing을 "바로 성공하고 호출 기록만 남기는" 스텁으로 바꾼다."""
    calls: list[int] = []

    async def _stub(episode_no: int, text: str) -> dict:
        calls.append(episode_no)
        return {"chapter": episode_no}

    monkeypatch.setattr(webapp, "run_indexing", _stub)
    return calls


@pytest.fixture
def client(monkeypatch, tmp_path):
    """매 테스트마다 깨끗한 서버 상태로 시작한다.

    _index_queue를 새로 만드는 건 asyncio.Queue가 처음 쓰인 이벤트 루프에 묶이기 때문이다 —
    TestClient는 인스턴스마다 새 루프를 만들어서, 모듈 전역 큐를 그대로 쓰면 두 번째
    테스트에서 "다른 루프에 묶인 큐" 오류가 난다.
    DATA_DIR도 tmp_path로 돌린다(테스트가 실제 data/episode*.txt를 덮어쓰지 않게).
    """
    webapp._index_jobs.clear()
    webapp._tpm_window.clear()
    monkeypatch.setattr(webapp, "_index_queue", asyncio.Queue())
    monkeypatch.setattr(webapp, "DATA_DIR", tmp_path)
    # 기본값은 "아직 인덱싱 안 됨" — 마커가 있는 경우는 해당 테스트에서 따로 뒤집는다.
    monkeypatch.setattr(webapp, "_already_indexed", lambda episode_nos: set())
    with TestClient(webapp.app) as c:
        c.data_dir = tmp_path
        yield c


def _submit(client, episodes, user_id=42, work_id=7):
    return client.post(
        "/api/index",
        json={"userId": user_id, "workId": work_id, "episodes": episodes},
    )


def _wait_until(client, job_id, predicate, timeout=5.0):
    """조회 API를 폴링하며 predicate(응답 본문)가 참이 될 때까지 기다린다."""
    deadline = time.monotonic() + timeout
    body = None
    while time.monotonic() < deadline:
        body = client.get(f"/api/index/jobs/{job_id}").json()
        if predicate(body):
            return body
        time.sleep(0.02)
    raise AssertionError(f"조건을 만족하지 못한 채 타임아웃: {body}")


def _terminal(body):
    return all(e["status"] in ("done", "error") for e in body["episodes"])


# ---------- 접수(201) ----------


def test_submit_returns_201_contract(client, stub_indexing):
    res = _submit(
        client,
        [
            {"episodeId": 101, "episodeNo": 6, "text": "6화 원고"},
            {"episodeId": 102, "episodeNo": 7, "text": "7화 원고"},
        ],
    )
    assert res.status_code == 201
    body = res.json()
    assert uuid.UUID(body["jobId"])  # jobId는 이 서버가 발급한 UUID다
    assert body["userId"] == 42
    assert body["workId"] == 7
    assert body["episodeIds"] == [101, 102]
    assert body["remainingTpm"] < webapp.INDEX_TPM_LIMIT  # 추정치만큼 창에서 깎였다
    assert re.match(RFC3339_UTC, body["requestedAt"])
    # 원문은 뷰어용으로 화 번호 기준 파일에 저장된다.
    assert (client.data_dir / "episode6.txt").read_text(encoding="utf-8") == "6화 원고"


# ---------- 검증(400) ----------


def test_empty_episodes_is_400(client, stub_indexing):
    res = _submit(client, [])
    assert res.status_code == 400
    assert res.json() == {"detail": "episodes must not be empty"}


def test_missing_text_is_400(client, stub_indexing):
    res = _submit(client, [{"episodeId": 101, "episodeNo": 6}])
    assert res.status_code == 400
    assert "text" in res.json()["detail"]
    assert _index_job_count() == 0


def test_descending_episode_no_is_400(client, stub_indexing):
    res = _submit(
        client,
        [
            {"episodeId": 102, "episodeNo": 7, "text": "7화"},
            {"episodeId": 101, "episodeNo": 6, "text": "6화"},
        ],
    )
    assert res.status_code == 400
    assert "ascending" in res.json()["detail"]
    assert _index_job_count() == 0


def _index_job_count() -> int:
    return len(webapp._index_jobs)


# ---------- TPM(429) ----------


def test_tpm_exhausted_returns_429_and_stores_nothing(client, stub_indexing, monkeypatch):
    # 한도를 아주 낮춰서 화 하나만으로도 여유를 넘기게 만든다(고정 컨텍스트 비용만으로 초과).
    monkeypatch.setattr(webapp, "INDEX_TPM_LIMIT", 1000)
    res = _submit(client, [{"episodeId": 101, "episodeNo": 6, "text": "6화 원고"}])
    assert res.status_code == 429
    assert res.headers["Retry-After"] == "60"  # 창이 비어 있으면 창 길이 그대로
    body = res.json()
    assert body["detail"] == "TPM limit exceeded. Retry after the Retry-After period."
    assert body["remainingTpm"] == 1000
    # 거절된 요청은 어디에도 남지 않는다 — 작업 기록도, 원고 파일도.
    assert _index_job_count() == 0
    assert not list(client.data_dir.iterdir())
    assert stub_indexing == []


# ---------- 화별 상태 전이 ----------


def test_episode_status_transitions(client, monkeypatch):
    """앞 화가 도는 동안 뒤 화는 waiting이고, 풀어주면 순서대로 done이 된다."""
    gate = threading.Event()
    started: list[int] = []

    async def _blocking(episode_no: int, text: str) -> dict:
        started.append(episode_no)
        while not gate.is_set():
            await asyncio.sleep(0.01)
        return {}

    monkeypatch.setattr(webapp, "run_indexing", _blocking)
    job_id = _submit(
        client,
        [
            {"episodeId": 101, "episodeNo": 6, "text": "6화"},
            {"episodeId": 102, "episodeNo": 7, "text": "7화"},
        ],
    ).json()["jobId"]

    try:
        body = _wait_until(client, job_id, lambda b: b["episodes"][0]["status"] == "running")
        assert body["jobId"] == job_id
        assert body["userId"] == 42 and body["workId"] == 7
        # 워커가 하나뿐이라 뒤 화는 아직 시작조차 안 한다(누적 컨텍스트 때문에 순차 처리 필수).
        assert body["episodes"][1] == {"episodeId": 102, "status": "waiting", "error": None}
        assert started == [6]
    finally:
        gate.set()

    body = _wait_until(client, job_id, _terminal)
    assert [e["status"] for e in body["episodes"]] == ["done", "done"]
    assert started == [6, 7]


# ---------- 실패 연쇄 스킵 ----------


def test_failure_skips_following_episodes(client, monkeypatch):
    async def _fail_on_seven(episode_no: int, text: str) -> dict:
        if episode_no == 7:
            raise RuntimeError("추출 실패")
        return {}

    monkeypatch.setattr(webapp, "run_indexing", _fail_on_seven)
    job_id = _submit(
        client,
        [
            {"episodeId": 101, "episodeNo": 6, "text": "6화"},
            {"episodeId": 102, "episodeNo": 7, "text": "7화"},
            {"episodeId": 103, "episodeNo": 8, "text": "8화"},
        ],
    ).json()["jobId"]

    body = _wait_until(client, job_id, _terminal)
    assert [e["status"] for e in body["episodes"]] == ["done", "error", "error"]
    assert body["episodes"][1]["error"] == "추출 실패"
    # 뒤 화는 시도조차 하지 않았다는 사실이 error 문구에 드러나야 한다.
    assert body["episodes"][2]["error"] == "Skipped due to preceding episode (7) failure"


# ---------- 조회(404) ----------


def test_unknown_job_is_404(client, stub_indexing):
    res = client.get(f"/api/index/jobs/{uuid.uuid4()}")
    assert res.status_code == 404
    assert "detail" in res.json()


# ---------- 이미 인덱싱된 화 ----------


def test_already_indexed_episodes_skip_work(client, stub_indexing, monkeypatch):
    """완료 마커가 있는 화는 즉시 done이고, 인덱싱도 TPM 소비도 하지 않는다."""
    monkeypatch.setattr(webapp, "_already_indexed", lambda episode_nos: {6})
    res = _submit(
        client,
        [
            {"episodeId": 101, "episodeNo": 6, "text": "이미 인덱싱된 6화"},
            {"episodeId": 102, "episodeNo": 7, "text": "새 7화"},
        ],
    )
    assert res.status_code == 201
    job_id = res.json()["jobId"]

    # 접수 직후부터 6화는 done이다(워커가 손도 대기 전에).
    assert client.get(f"/api/index/jobs/{job_id}").json()["episodes"][0]["status"] == "done"
    body = _wait_until(client, job_id, _terminal)
    assert [e["status"] for e in body["episodes"]] == ["done", "done"]
    assert stub_indexing == [7]  # 6화는 실제로 인덱싱되지 않았다


def test_fully_indexed_resubmit_costs_no_tpm(client, stub_indexing, monkeypatch):
    """전부 이미 인덱싱된 재제출은 TPM을 전혀 쓰지 않는다 — 안 그러면 재제출이 429로 막힌다."""
    monkeypatch.setattr(webapp, "_already_indexed", lambda episode_nos: set(episode_nos))
    monkeypatch.setattr(webapp, "INDEX_TPM_LIMIT", 1000)
    res = _submit(client, [{"episodeId": 101, "episodeNo": 6, "text": "6화" * 5000}])
    assert res.status_code == 201
    assert res.json()["remainingTpm"] == 1000
    body = _wait_until(client, res.json()["jobId"], _terminal)
    assert body["episodes"][0]["status"] == "done"
    assert stub_indexing == []
