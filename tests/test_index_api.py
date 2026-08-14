"""인덱싱 API 스모크 테스트.

실제 인덱싱(run_indexing)과 Neo4j 완료 마커 조회(_already_indexed)를 전부 스텁으로 갈아끼워
LLM·DB 비용 없이 계약만 검증한다. 확인하는 것: 201 응답 모양, 400 검증, 429(TPM) 경로,
화별 상태 전이(waiting→running→done), 실패 시 뒤 화 연쇄 스킵, 모르는 jobId의 404,
이미 인덱싱된 화의 빠른 경로, 인덱싱된 작품 외의 요청 거절, 요청을 가로지르는 오름차순 강제,
그리고 한도 자체를 넘는 묶음의 400.

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

from src import app as app_module
from src.service.index import job_service as index_job_service
from src.service import kg_scope
from conftest import RecordingDict  # tests/에 __init__.py가 없어 pytest가 경로에 넣어준다

RFC3339_UTC = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

# KG에 인덱싱돼 있다고 보는 작품. 실제 값은 환경변수(KG_INDEXED_WORK_ID)라서 테스트에서
# 고정한다 — 테스트가 배포 환경 설정에 따라 통과했다 말았다 하면 안 된다.
WORK_ID = 1
OTHER_WORK_ID = WORK_ID + 1


@pytest.fixture
def stub_indexing(monkeypatch):
    """run_indexing을 "바로 성공하고 호출 기록만 남기는" 스텁으로 바꾼다."""
    calls: list[int] = []

    async def _stub(episode_no: int, text: str) -> dict:
        calls.append(episode_no)
        return {"chapter": episode_no}

    monkeypatch.setattr(index_job_service, "run_indexing", _stub)
    return calls


@pytest.fixture
def client(monkeypatch, tmp_path):
    """매 테스트마다 깨끗한 서버 상태로 시작한다.

    _index_queue를 새로 만드는 건 asyncio.Queue가 처음 쓰인 이벤트 루프에 묶이기 때문이다 —
    TestClient는 인스턴스마다 새 루프를 만들어서, 모듈 전역 큐를 그대로 쓰면 두 번째
    테스트에서 "다른 루프에 묶인 큐" 오류가 난다.
    큐 오름차순 워터마크(_max_queued_episode_no)도 모듈 전역이라 매번 0으로 되돌린다.
    """
    index_job_service._index_jobs.clear()
    index_job_service._tpm_window.clear()
    monkeypatch.setattr(index_job_service, "_index_queue", asyncio.Queue())
    monkeypatch.setattr(index_job_service, "_max_queued_episode_no", 0)
    monkeypatch.setattr(kg_scope, "KG_INDEXED_WORK_ID", WORK_ID)
    # 기본값은 "아직 인덱싱 안 됨" — 마커가 있는 경우는 해당 테스트에서 따로 뒤집는다.
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda episode_nos: set())
    with TestClient(app_module.app) as c:
        yield c


def _submit(client, episodes, user_id=42, work_id=WORK_ID):
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
    assert body["workId"] == WORK_ID
    assert body["episodeIds"] == [101, 102]
    assert body["remainingTpm"] < index_job_service.INDEX_TPM_LIMIT  # 추정치만큼 창에서 깎였다
    assert re.match(RFC3339_UTC, body["requestedAt"])


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
    return len(index_job_service._index_jobs)


# ---------- 인덱싱된 작품 외의 요청(400) ----------


def test_other_work_is_400(client, stub_indexing):
    """KG에 작품 격리가 없어서, 다른 작품의 화를 받으면 인덱싱된 작품 위에 덮어쓴다.

    특히 위험한 건 완료 마커다: 작품 A에 6화가 이미 있으면 작품 B의 6화가 마커에 걸려
    **아무 일도 하지 않은 채 done**으로 보고되고, 호출자는 성공으로 알고 다시 보내지 않는다.
    그래서 조용한 성공 대신 시끄러운 실패를 준다.
    """
    res = _submit(
        client, [{"episodeId": 101, "episodeNo": 6, "text": "다른 작품 6화"}], work_id=OTHER_WORK_ID
    )
    assert res.status_code == 400
    detail = res.json()["detail"]
    assert str(OTHER_WORK_ID) in detail and str(WORK_ID) in detail
    # 거절된 요청은 작업 기록에 남지 않는다.
    assert _index_job_count() == 0
    assert stub_indexing == []


def test_other_work_is_400_even_when_marker_says_done(client, stub_indexing, monkeypatch):
    """마커 조회는 화 번호만 보므로 다른 작품의 6화도 "이미 인덱싱됨"으로 답한다.

    이게 이 버그의 심장이다 — 관문이 없으면 여기서 201 + 전부 done이 나가고, 그 화는 영영
    인덱싱되지 않는다. 관문은 마커를 보기도 전에 막아야 한다.
    """
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda episode_nos: set(episode_nos))
    res = _submit(
        client, [{"episodeId": 101, "episodeNo": 6, "text": "다른 작품 6화"}], work_id=OTHER_WORK_ID
    )
    assert res.status_code == 400
    assert _index_job_count() == 0


# ---------- 요청을 가로지르는 오름차순(400) ----------


def test_lower_episode_after_higher_request_is_400(client, stub_indexing):
    """POST [5,6] 다음의 POST [3,4]는 400이다.

    큐는 FIFO라 받아주면 [5,6,3,4] 순으로 실행되고, 3화를 추출할 때 그래프와 Story.summary에는
    이미 5·6화가 들어 있다 — 추출기가 미래를 "지금까지의 줄거리"로 읽어 그래프가 조용히 오염된다.
    """
    first = _submit(
        client,
        [
            {"episodeId": 105, "episodeNo": 5, "text": "5화"},
            {"episodeId": 106, "episodeNo": 6, "text": "6화"},
        ],
    )
    assert first.status_code == 201

    second = _submit(
        client,
        [
            {"episodeId": 103, "episodeNo": 3, "text": "3화"},
            {"episodeId": 104, "episodeNo": 4, "text": "4화"},
        ],
    )
    assert second.status_code == 400
    assert "ascending" in second.json()["detail"]
    assert _index_job_count() == 1  # 첫 요청만 남았다


def test_same_episode_in_two_requests_is_400(client, stub_indexing):
    """같은 화가 두 요청에 겹쳐 들어오면 400 — 안 막으면 같은 화를 두 번 인덱싱한다(LLM 비용 2배)."""
    assert _submit(client, [{"episodeId": 106, "episodeNo": 6, "text": "6화"}]).status_code == 201
    res = _submit(client, [{"episodeId": 106, "episodeNo": 6, "text": "6화 다시"}])
    assert res.status_code == 400
    assert "ascending" in res.json()["detail"]


def test_ascending_across_requests_is_accepted(client, stub_indexing):
    """규칙은 "요청마다 처음부터"가 아니라 "이어서 오름차순"이다 — 정상 흐름을 막으면 안 된다."""
    assert _submit(client, [{"episodeId": 106, "episodeNo": 6, "text": "6화"}]).status_code == 201
    assert _submit(client, [{"episodeId": 107, "episodeNo": 7, "text": "7화"}]).status_code == 201
    body = _wait_until(
        client,
        _submit(client, [{"episodeId": 108, "episodeNo": 8, "text": "8화"}]).json()["jobId"],
        _terminal,
    )
    assert body["episodes"][0]["status"] == "done"
    assert stub_indexing == [6, 7, 8]


def test_already_indexed_resubmit_is_not_blocked_by_watermark(client, stub_indexing, monkeypatch):
    """완료 마커가 있는 화의 재제출은 워터마크 비교에서 빠진다.

    재제출은 아무 일도 하지 않으므로 순서를 깨지 않는다. 여기서 400을 주면 "재시작 후 404를
    보면 다시 POST한다"는 스펙의 복구 경로가 막힌다.
    """
    assert _submit(client, [{"episodeId": 107, "episodeNo": 7, "text": "7화"}]).status_code == 201
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda episode_nos: {3, 4})
    res = _submit(
        client,
        [
            {"episodeId": 103, "episodeNo": 3, "text": "3화"},
            {"episodeId": 104, "episodeNo": 4, "text": "4화"},
        ],
    )
    assert res.status_code == 201
    body = _wait_until(client, res.json()["jobId"], _terminal)
    assert [e["status"] for e in body["episodes"]] == ["done", "done"]
    assert stub_indexing == [7]  # 3·4화는 실제로 인덱싱되지 않았다


def test_rejected_request_does_not_raise_the_watermark(client, stub_indexing, monkeypatch):
    """429로 거절한 요청은 없던 일이어야 한다 — 워터마크만 올려두면 재시도가 400으로 막힌다."""
    monkeypatch.setattr(index_job_service, "INDEX_TPM_LIMIT", 20000)
    index_job_service._tpm_window.append((time.monotonic(), 19000))  # 여유를 거의 없애 429를 유도
    assert _submit(client, [{"episodeId": 106, "episodeNo": 6, "text": "6화"}]).status_code == 429
    index_job_service._tpm_window.clear()
    assert _submit(client, [{"episodeId": 106, "episodeNo": 6, "text": "6화"}]).status_code == 201


# ---------- TPM(429) ----------


def test_tpm_exhausted_returns_429_and_stores_nothing(client, stub_indexing, monkeypatch):
    """한도 안에는 들어가지만 "지금" 여유가 없는 요청 — 기다리면 통과하므로 429가 맞다.

    (한도 자체를 넘어 영영 통과할 수 없는 요청은 429가 아니라 400이다. 위 테스트 참고.)
    """
    monkeypatch.setattr(index_job_service, "INDEX_TPM_LIMIT", 20000)
    index_job_service._tpm_window.append((time.monotonic(), 19000))  # 방금 19,000을 썼다고 가정
    res = _submit(client, [{"episodeId": 101, "episodeNo": 6, "text": "6화 원고"}])
    assert res.status_code == 429
    assert res.headers["Retry-After"] == "60"  # 방금 기록이라 창이 다 흐르려면 60초
    body = res.json()
    assert body["detail"] == "TPM limit exceeded. Retry after the Retry-After period."
    assert body["remainingTpm"] == 1000
    # 거절된 요청은 작업 기록에 남지 않는다.
    assert _index_job_count() == 0
    assert stub_indexing == []


def test_bundle_larger_than_the_whole_limit_is_400_not_429(client, stub_indexing, monkeypatch):
    """한도 자체를 넘는 묶음은 기다린다고 통과하지 못한다 — 429는 끝나지 않는 재시도 루프다."""
    monkeypatch.setattr(index_job_service, "INDEX_TPM_LIMIT", 20000)
    # 회차당 고정 비용 15,000 × 2 = 30,000 > 20,000. 창이 텅 비어 있어도 절대 못 들어간다.
    res = _submit(
        client,
        [
            {"episodeId": 106, "episodeNo": 6, "text": "6화"},
            {"episodeId": 107, "episodeNo": 7, "text": "7화"},
        ],
    )
    assert res.status_code == 400
    detail = res.json()["detail"]
    assert "never be accepted" in detail and "Split" in detail
    assert "Retry-After" not in res.headers
    assert _index_job_count() == 0


def test_many_small_episodes_are_400_too(client, stub_indexing):
    """길이와 무관하게 화 수만으로 한도를 넘는 경우(고정 비용 15,000/화 × 14화 > 200,000)."""
    res = _submit(
        client,
        [{"episodeId": 100 + n, "episodeNo": n, "text": "짧은 원고"} for n in range(1, 15)],
    )
    assert res.status_code == 400
    assert "never be accepted" in res.json()["detail"]


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

    monkeypatch.setattr(index_job_service, "run_indexing", _blocking)
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
        assert body["userId"] == 42 and body["workId"] == WORK_ID
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

    monkeypatch.setattr(index_job_service, "run_indexing", _fail_on_seven)
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


# ---------- 터진 읽기(status와 사유가 따로 쓰이는 순간) ----------


def test_error_status_never_appears_without_its_reason(monkeypatch):
    """조회는 스레드풀에서, 워커는 이벤트 루프에서 돈다 — status="error"인데 error=None인
    순간이 있으면 그 순간의 폴링이 사유 없는 실패를 확정 저장한다."""

    async def _fail(episode_no: int, text: str) -> dict:
        raise RuntimeError("추출 실패")

    monkeypatch.setattr(index_job_service, "run_indexing", _fail)
    episodes = [
        RecordingDict(
            {"episode_id": 101, "episode_no": 6, "text": "6화", "status": "waiting", "error": None}
        ),
        RecordingDict(
            {"episode_id": 102, "episode_no": 7, "text": "7화", "status": "waiting", "error": None}
        ),
    ]
    index_job_service._index_jobs["job-torn"] = {
        "user_id": 42,
        "work_id": WORK_ID,
        "requested_at": "2026-08-14T00:00:00Z",
        "episodes": episodes,
    }
    try:
        asyncio.run(index_job_service._run_index_job("job-torn"))
    finally:
        index_job_service._index_jobs.pop("job-torn", None)

    # 첫 화는 실패, 둘째 화는 연쇄 스킵 — 둘 다 error다.
    assert [e["status"] for e in episodes] == ["error", "error"]
    for episode in episodes:
        assert [s for s in episode.snapshots if s["status"] == "error" and not s["error"]] == []


# ---------- 이미 인덱싱된 화 ----------


def test_already_indexed_episodes_skip_work(client, stub_indexing, monkeypatch):
    """완료 마커가 있는 화는 즉시 done이고, 인덱싱도 TPM 소비도 하지 않는다."""
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda episode_nos: {6})
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
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda episode_nos: set(episode_nos))
    monkeypatch.setattr(index_job_service, "INDEX_TPM_LIMIT", 1000)
    res = _submit(client, [{"episodeId": 101, "episodeNo": 6, "text": "6화" * 5000}])
    assert res.status_code == 201
    assert res.json()["remainingTpm"] == 1000
    body = _wait_until(client, res.json()["jobId"], _terminal)
    assert body["episodes"][0]["status"] == "done"
    assert stub_indexing == []


def test_inflight_resubmit_is_not_blocked_by_watermark(client, monkeypatch):
    """아직 처리 중인 화의 재제출은 워터마크에 걸리지 않는다.

    완료 마커는 인덱싱이 끝나야 찍힌다. 그래서 큐에 들어가 처리 중인 화는 마커가 없고,
    계약대로(타임아웃·404) 재제출하면 마커로 걸러지지 않는다. 이걸 워터마크가 막으면
    정상 재제출이 영구 실패가 된다 — 실제로 이미 인덱싱이 끝난 회차가 화면에 "반영 실패"로
    표시된 회귀가 있었다.
    """
    started = threading.Event()
    release = threading.Event()

    async def blocking_indexing(chapter: int, text: str) -> dict:
        started.set()
        await asyncio.to_thread(release.wait, 5)
        return {"chapter": chapter}

    monkeypatch.setattr(index_job_service, "run_indexing", blocking_indexing)
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda episode_nos: set())

    episodes = [
        {"episodeId": 102, "episodeNo": 2, "text": "2화"},
        {"episodeId": 103, "episodeNo": 3, "text": "3화"},
    ]
    assert _submit(client, episodes).status_code == 201
    assert started.wait(5), "워커가 첫 화를 시작하지 못했다"

    # 마커는 아직 없고 2·3화는 waiting/running 이다. 여기서 같은 묶음을 다시 보낸다.
    try:
        assert _submit(client, episodes).status_code == 201
    finally:
        release.set()
