"""인덱싱 API 스모크 테스트.

실제 인덱싱(run_indexing)과 Neo4j 완료 마커 조회(_already_indexed)를 전부 스텁으로 갈아끼워
LLM·DB 비용 없이 계약만 검증한다. 확인하는 것: 201 응답 모양, 400 검증, 429(TPM) 경로,
화별 상태 전이(QUEUED→RUNNING→DONE), 실패 시 뒤 화 연쇄 스킵, 모르는 jobId의 404,
이미 인덱싱된 화의 빠른 경로, 요청을 가로지르는 오름차순 강제, 그 오름차순이 테넌트를
넘나들지 않는다는 것, 그리고 한도 자체를 넘는 묶음의 400.

TestClient는 앱을 별도 스레드의 이벤트 루프에서 돌리므로, 백그라운드 워커가 진행하는 동안
테스트 스레드가 조회 API를 폴링할 수 있다(_wait_until).
"""

from __future__ import annotations

import asyncio
import re
import threading
import time
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient

from src import app as app_module
from src.common import llm_limit
from src.common.tenant import Tenant
from src.service.index import job_service as index_job_service
from conftest import RecordingDict  # tests/에 __init__.py가 없어 pytest가 경로에 넣어준다

RFC3339_UTC = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

# 기본 요청이 쓰는 테넌트. userId × workId 조합이 소설 한 편(KG 테넌트)을 가리킨다.
USER_ID = 42
WORK_ID = 1


@pytest.fixture
def stub_indexing(monkeypatch):
    """run_indexing을 "바로 성공하고 호출 기록만 남기는" 스텁으로 바꾼다.

    첫 인자로 Tenant를 받는다 — 인덱싱은 어느 소설의 그래프에 쓸지를 이 값으로만 안다.
    """
    calls: list[int] = []

    async def _stub(tenant: Tenant, episode_no: int, text: str) -> dict:
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
    큐 오름차순 워터마크(_max_queued_episode_no)도 모듈 전역이라 매번 비운다 — 테넌트 id를
    키로 하는 dict이므로 빈 dict가 "아무 화도 큐에 넣은 적 없음"이다.
    """
    index_job_service._index_jobs.clear()
    # 미터도 비운다 — 접수 게이트의 마지막 안전망이 모델 버킷 잔량을 보므로, 앞 테스트가
    # 남긴 값이 남아 있으면 엉뚱한 429가 난다.
    monkeypatch.setattr(llm_limit, "_buckets", {})
    monkeypatch.setattr(index_job_service, "_index_queue", asyncio.Queue())
    monkeypatch.setattr(index_job_service, "_max_queued_episode_no", {})
    # 기본값은 "아직 인덱싱 안 됨" — 마커가 있는 경우는 해당 테스트에서 따로 뒤집는다.
    # 완료 마커 조회는 테넌트 안에서만 유효하므로 첫 인자로 Tenant를 받는다.
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda tenant, episode_nos: set())
    with TestClient(app_module.app) as c:
        yield c


def _submit(client, episodes, user_id=USER_ID, work_id=WORK_ID):
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
    return all(e["status"] in ("DONE", "ERROR") for e in body["episodes"])


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
    assert body["userId"] == USER_ID
    assert body["workId"] == WORK_ID
    assert body["episodeIds"] == [101, 102]
    # remainingTpm 은 이제 미터가 헤더로 관측한 실제 잔량이다. 테스트에서는 호출이 없어
    # 콜드 스타트 가정값이 그대로 나온다 — 음수나 None 이 아니면 계약은 지켜진 것이다.
    assert body["remainingTpm"] >= 0
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


def test_watermark_does_not_cross_tenants(client, stub_indexing):
    """워터마크는 테넌트(userId × workId)마다 따로 센다.

    전역 워터마크 하나였다면 작품 1의 7화가 올려둔 값에 작품 2의 3화가 걸려 400을 받는다 —
    서로 아무 상관 없는 두 소설이 서로의 진도에 발이 묶인다. 오름차순이 필요한 이유는
    "누적 컨텍스트가 오염된다"이고, 그 누적은 테넌트 안에서만 일어난다.
    """
    assert (
        _submit(
            client,
            [{"episodeId": 107, "episodeNo": 7, "text": "1번 작품 7화"}],
            user_id=1,
            work_id=1,
        ).status_code
        == 201
    )

    res = _submit(
        client,
        [{"episodeId": 203, "episodeNo": 3, "text": "2번 작품 3화"}],
        user_id=1,
        work_id=2,
    )
    assert res.status_code == 201
    body = _wait_until(client, res.json()["jobId"], _terminal)
    assert body["episodes"][0]["status"] == "DONE"
    assert stub_indexing == [7, 3]  # 3화가 실제로 인덱싱까지 갔다


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
    assert body["episodes"][0]["status"] == "DONE"
    assert stub_indexing == [6, 7, 8]


def test_already_indexed_resubmit_is_not_blocked_by_watermark(client, stub_indexing, monkeypatch):
    """완료 마커가 있는 화의 재제출은 워터마크 비교에서 빠진다.

    재제출은 아무 일도 하지 않으므로 순서를 깨지 않는다. 여기서 400을 주면 "재시작 후 404를
    보면 다시 POST한다"는 스펙의 복구 경로가 막힌다.
    """
    assert _submit(client, [{"episodeId": 107, "episodeNo": 7, "text": "7화"}]).status_code == 201
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda tenant, episode_nos: {3, 4})
    res = _submit(
        client,
        [
            {"episodeId": 103, "episodeNo": 3, "text": "3화"},
            {"episodeId": 104, "episodeNo": 4, "text": "4화"},
        ],
    )
    assert res.status_code == 201
    body = _wait_until(client, res.json()["jobId"], _terminal)
    assert [e["status"] for e in body["episodes"]] == ["DONE", "DONE"]
    assert stub_indexing == [7]  # 3·4화는 실제로 인덱싱되지 않았다


def test_rejected_request_does_not_raise_the_watermark(client, monkeypatch):
    """429로 거절한 요청은 없던 일이어야 한다 — 워터마크만 올려두면 재시도가 400으로 막힌다."""
    monkeypatch.setattr(index_job_service, "INDEX_MAX_WAIT_SECONDS", 120)  # = 1화
    release = threading.Event()

    async def _blocking(tenant, episode_no, text):
        await asyncio.to_thread(release.wait, 5)
        return {"chapter": episode_no}

    monkeypatch.setattr(index_job_service, "run_indexing", _blocking)

    # 1화를 넣어 큐를 채운다 → 다음 요청은 큐 혼잡으로 429
    assert _submit(client, [{"episodeId": 105, "episodeNo": 5, "text": "5화"}]).status_code == 201
    assert _submit(client, [{"episodeId": 106, "episodeNo": 6, "text": "6화"}]).status_code == 429

    # 큐가 빠지면 같은 화가 다시 통과해야 한다(워터마크가 올라가 있으면 400이 된다).
    release.set()
    _wait_until(client, _job_ids()[0], _terminal)
    assert _submit(client, [{"episodeId": 106, "episodeNo": 6, "text": "6화"}]).status_code == 201


def _job_ids() -> list[str]:
    return list(index_job_service._index_jobs)


# ---------- 접수 게이트(429) ----------


def test_queue_backlog_returns_429_and_stores_nothing(client, stub_indexing, monkeypatch):
    """큐가 밀리면 429 — 기다리면 빠지므로 400이 아니라 429가 맞다.

    예전에는 이 자리에서 TPM(글자수 추정 토큰)을 봤다. 지금은 **대기 화 수**를 본다:
    지키려던 자원이 OpenAI 한도가 아니라 워커 처리량이었기 때문이다(접수 7화/분 vs
    처리 0.5화/분으로 14배 어긋나 있었다).
    """
    # 화당 120초, 상한 240초 = 2화. 이미 2화가 큐에 있으면 다음 요청은 거절된다.
    monkeypatch.setattr(index_job_service, "INDEX_MAX_WAIT_SECONDS", 240)

    # 워커를 붙들어 큐에 남겨둔다 — 스텁이 즉시 끝나면 접수하자마자 큐가 비어버린다.
    release = threading.Event()

    async def _blocking(tenant, episode_no, text):
        await asyncio.to_thread(release.wait, 5)
        return {"chapter": episode_no}

    monkeypatch.setattr(index_job_service, "run_indexing", _blocking)

    assert _submit(
        client,
        [
            {"episodeId": 101, "episodeNo": 1, "text": "1화"},
            {"episodeId": 102, "episodeNo": 2, "text": "2화"},
        ],
    ).status_code == 201

    res = _submit(client, [{"episodeId": 103, "episodeNo": 3, "text": "3화"}])
    assert res.status_code == 429
    body = res.json()
    assert body["detail"] == "Indexing queue is full. Retry after the Retry-After period."
    assert body["queuedEpisodes"] >= 1
    assert body["estimatedWaitSeconds"] > 240
    assert int(res.headers["Retry-After"]) > 0
    # 거절된 요청은 작업 기록에 남지 않는다.
    assert _index_job_count() == 1  # 첫 요청만
    release.set()


def test_bundle_larger_than_the_whole_limit_is_400_not_429(client, stub_indexing, monkeypatch):
    """묶음 하나가 대기 상한을 넘으면 기다린다고 통과하지 못한다 — 429는 끝나지 않는 재시도 루프다."""
    monkeypatch.setattr(index_job_service, "INDEX_MAX_WAIT_SECONDS", 240)  # = 2화
    res = _submit(
        client,
        [{"episodeId": 100 + n, "episodeNo": n, "text": f"{n}화"} for n in range(1, 4)],  # 3화
    )
    assert res.status_code == 400
    detail = res.json()["detail"]
    assert "never be accepted" in detail and "Split" in detail
    assert "Retry-After" not in res.headers
    assert _index_job_count() == 0


def test_many_small_episodes_are_400_too(client, stub_indexing):
    """원고 길이와 무관하게 **화 수**만으로 상한을 넘는 경우(기본 상한 2400초 = 20화)."""
    res = _submit(
        client,
        [{"episodeId": 100 + n, "episodeNo": n, "text": "짧은 원고"} for n in range(1, 22)],
    )
    assert res.status_code == 400
    assert "never be accepted" in res.json()["detail"]


# ---------- 화별 상태 전이 ----------


def test_episode_status_transitions(client, monkeypatch):
    """앞 화가 도는 동안 뒤 화는 QUEUED이고, 풀어주면 순서대로 DONE이 된다."""
    gate = threading.Event()
    started: list[int] = []

    async def _blocking(tenant: Tenant, episode_no: int, text: str) -> dict:
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
        body = _wait_until(client, job_id, lambda b: b["episodes"][0]["status"] == "RUNNING")
        assert body["jobId"] == job_id
        assert body["userId"] == USER_ID and body["workId"] == WORK_ID
        # 워커가 하나뿐이라 뒤 화는 아직 시작조차 안 한다(누적 컨텍스트 때문에 순차 처리 필수).
        assert body["episodes"][1] == {"episodeId": 102, "status": "QUEUED", "error": None}
        assert started == [6]
    finally:
        gate.set()

    body = _wait_until(client, job_id, _terminal)
    assert [e["status"] for e in body["episodes"]] == ["DONE", "DONE"]
    assert started == [6, 7]


# ---------- 실패 연쇄 스킵 ----------


def test_failure_skips_following_episodes(client, monkeypatch):
    async def _fail_on_seven(tenant: Tenant, episode_no: int, text: str) -> dict:
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
    assert [e["status"] for e in body["episodes"]] == ["DONE", "ERROR", "ERROR"]
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
    """조회는 스레드풀에서, 워커는 이벤트 루프에서 돈다 — status="ERROR"인데 error=None인
    순간이 있으면 그 순간의 폴링이 사유 없는 실패를 확정 저장한다."""

    async def _fail(tenant: Tenant, episode_no: int, text: str) -> dict:
        raise RuntimeError("추출 실패")

    monkeypatch.setattr(index_job_service, "run_indexing", _fail)
    episodes = [
        RecordingDict(
            {"episode_id": 101, "episode_no": 6, "text": "6화", "status": "QUEUED", "error": None}
        ),
        RecordingDict(
            {"episode_id": 102, "episode_no": 7, "text": "7화", "status": "QUEUED", "error": None}
        ),
    ]
    # 워커가 실제로 만드는 작업과 같은 모양으로 넣는다(tenant_id 포함 — 워터마크 판정이 읽는다).
    index_job_service._index_jobs["job-torn"] = {
        "user_id": USER_ID,
        "work_id": WORK_ID,
        "tenant_id": Tenant.of(USER_ID, WORK_ID).id,
        "requested_at": "2026-08-14T00:00:00Z",
        "episodes": episodes,
    }
    try:
        asyncio.run(index_job_service._run_index_job("job-torn"))
    finally:
        index_job_service._index_jobs.pop("job-torn", None)

    # 첫 화는 실패, 둘째 화는 연쇄 스킵 — 둘 다 ERROR다.
    assert [e["status"] for e in episodes] == ["ERROR", "ERROR"]
    for episode in episodes:
        assert [s for s in episode.snapshots if s["status"] == "ERROR" and not s["error"]] == []


# ---------- 이미 인덱싱된 화 ----------


def test_already_indexed_episodes_skip_work(client, stub_indexing, monkeypatch):
    """완료 마커가 있는 화는 즉시 DONE이고, 인덱싱도 TPM 소비도 하지 않는다."""
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda tenant, episode_nos: {6})
    res = _submit(
        client,
        [
            {"episodeId": 101, "episodeNo": 6, "text": "이미 인덱싱된 6화"},
            {"episodeId": 102, "episodeNo": 7, "text": "새 7화"},
        ],
    )
    assert res.status_code == 201
    job_id = res.json()["jobId"]

    # 접수 직후부터 6화는 DONE이다(워커가 손도 대기 전에).
    assert client.get(f"/api/index/jobs/{job_id}").json()["episodes"][0]["status"] == "DONE"
    body = _wait_until(client, job_id, _terminal)
    assert [e["status"] for e in body["episodes"]] == ["DONE", "DONE"]
    assert stub_indexing == [7]  # 6화는 실제로 인덱싱되지 않았다


def test_marker_is_looked_up_within_the_requesting_tenant(client, stub_indexing, monkeypatch):
    """완료 마커 조회는 요청의 테넌트로 좁혀서 물어봐야 한다.

    화 번호만 보고 판정하면 작품 B의 6화가 작품 A의 6화 마커에 걸려 **아무 일도 하지 않은 채**
    DONE으로 보고된다 — 호출자는 성공으로 알고 다시 보내지 않고, 그 화는 영영 인덱싱되지 않는다.
    """
    seen: list[str] = []

    def _fake_marker(tenant: Tenant, episode_nos: list[int]) -> set[int]:
        seen.append(tenant.id)
        return set()

    monkeypatch.setattr(index_job_service, "_already_indexed", _fake_marker)
    assert (
        _submit(
            client,
            [{"episodeId": 101, "episodeNo": 6, "text": "6화"}],
            user_id=7,
            work_id=3,
        ).status_code
        == 201
    )
    assert seen == ["7:3"]


def test_fully_indexed_resubmit_is_not_gated(client, stub_indexing, monkeypatch):
    """전부 이미 인덱싱된 재제출은 큐를 쓰지 않으므로 게이트에 걸리지 않는다.

    걸리면 "안전한 재제출"이라는 스펙의 전제가 깨진다 — 아무 일도 안 하는 요청이
    429를 받고 영원히 재시도한다.
    """
    monkeypatch.setattr(
        index_job_service, "_already_indexed", lambda tenant, episode_nos: set(episode_nos)
    )
    monkeypatch.setattr(index_job_service, "INDEX_MAX_WAIT_SECONDS", 0)  # 어떤 대기도 불허
    res = _submit(client, [{"episodeId": 101, "episodeNo": 6, "text": "6화" * 5000}])
    assert res.status_code == 201
    body = _wait_until(client, res.json()["jobId"], _terminal)
    assert body["episodes"][0]["status"] == "DONE"
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

    async def blocking_indexing(tenant: Tenant, chapter: int, text: str) -> dict:
        started.set()
        await asyncio.to_thread(release.wait, 5)
        return {"chapter": chapter}

    monkeypatch.setattr(index_job_service, "run_indexing", blocking_indexing)
    monkeypatch.setattr(index_job_service, "_already_indexed", lambda tenant, episode_nos: set())

    episodes = [
        {"episodeId": 102, "episodeNo": 2, "text": "2화"},
        {"episodeId": 103, "episodeNo": 3, "text": "3화"},
    ]
    assert _submit(client, episodes).status_code == 201
    assert started.wait(5), "워커가 첫 화를 시작하지 못했다"

    # 마커는 아직 없고 2·3화는 QUEUED/RUNNING 이다. 여기서 같은 묶음을 다시 보낸다.
    try:
        assert _submit(client, episodes).status_code == 201
    finally:
        release.set()


def test_인덱싱은_429를_자체_재시도하지_않고_즉시_실패한다(client, monkeypatch):
    """429 재시도는 관문(src/common/openai_client.py)이 호출 단위로만 한다.

    예전에는 이 자리에 회차 단위 재시도가 있었다(3회, 20/40/80초). 지운 이유는 재시도가
    회차 전체를 다시 돌리는데, 라이브러리 노드 쓰기가 upsert가 아니라 CREATE이고
    Event·CharacterState는 resolver가 일부러 병합하지 않아서 **재시도할 때마다 사건과
    인물 상태가 한 벌씩 더 쌓이기** 때문이다. 예외도 안 나고 검색 결과에만 조용히 중복으로
    잡힌다.

    그래서 여기서는 "빨리 실패하는 것"이 올바른 동작이다. 누가 편의로 재시도를 다시
    넣으면 이 테스트가 깨진다 — 그때 위 이유를 다시 읽어야 한다.
    """
    from openai import RateLimitError

    호출 = []

    async def _429(tenant, episode_no, text):
        호출.append(episode_no)
        response = httpx.Response(
            429, request=httpx.Request("POST", "https://api.openai.com/v1")
        )
        raise RateLimitError("rate limited", response=response, body={})

    monkeypatch.setattr(index_job_service, "run_indexing", _429)

    시작 = time.monotonic()
    body = _submit(client, [{"episodeId": 101, "episodeNo": 1, "text": "1화 원고"}]).json()
    상태 = _wait_until(client, body["jobId"], _terminal)
    걸린 = time.monotonic() - 시작

    assert 상태["episodes"][0]["status"] == "ERROR"
    assert len(호출) == 1, f"회차를 다시 돌렸다({len(호출)}회) — 그래프에 중복이 쌓인다"
    assert 걸린 < 5, f"{걸린:.1f}초 걸렸다 — 백오프 대기가 남아 있다"


def test_모델_한도가_바닥이면_큐가_한가해도_거절한다(client, stub_indexing, monkeypatch):
    """마지막 안전망 — 탐지가 같은 모델 버킷을 비웠을 수 있다.

    인덱싱과 탐지는 같은 EXTRACTION_MODEL 을 쓴다(6번 커밋 이후). 큐 깊이만 보면 "우리는
    한가하다"고 판단해 접수하는데, 정작 OpenAI 쪽이 바닥이면 시작해봐야 429만 맞는다.
    """
    from src.config import EXTRACTION_MODEL

    llm_limit.observe(
        EXTRACTION_MODEL,
        {
            "x-ratelimit-limit-tokens": "200000",
            "x-ratelimit-remaining-tokens": "100",  # 한도의 0.05% — 임계(10%) 아래
            "x-ratelimit-reset-tokens": "30s",
        },
    )

    res = _submit(client, [{"episodeId": 101, "episodeNo": 1, "text": "1화"}])
    assert res.status_code == 429
    assert "rate limit" in res.json()["detail"].lower()
    assert int(res.headers["Retry-After"]) > 0
    assert _index_job_count() == 0
    assert stub_indexing == []


def test_여유가_충분하면_안전망은_통과시킨다(client, stub_indexing):
    """임계 위면 막지 않는다 — 안전망이 정상 흐름을 막으면 안 된다."""
    from src.config import EXTRACTION_MODEL

    llm_limit.observe(
        EXTRACTION_MODEL,
        {"x-ratelimit-limit-tokens": "200000", "x-ratelimit-remaining-tokens": "190000"},
    )
    assert _submit(client, [{"episodeId": 101, "episodeNo": 1, "text": "1화"}]).status_code == 201
