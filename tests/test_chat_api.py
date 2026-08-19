"""채팅 회차 컨텍스트 계약 테스트.

검증 대상은 "회차에 관한 세 개념이 서로 섞이지 않는가"다:
  1. 인덱싱된 회차 — 요청이 아니라 Neo4j에서 온다(요청에 실려도 무시된다).
  2. 집필 중인 회차 — 원고 **전문**이 시스템 프롬프트에 그대로 들어간다.
  3. 보고 있는 회차 — 번호만 오고, 기본 주제가 아니라 힌트로 제시된다.

LLM·Neo4j·PostgreSQL은 전부 스텁이라 실제 비용이 들지 않는다.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src import app as app_module
from src.service.chat import agent as chat_agent

# ---------- /api/chat 요청 계약 ----------
# 와이어 포맷은 전 API 공통으로 camelCase다. 아래 captured[...] assert들은 wire가 아니라
# 컨트롤러가 서비스에 넘기는 파이썬 kwargs(내부 계약)라 snake_case가 맞다.


@pytest.fixture
def captured_chat(monkeypatch):
    """run_chat을 "인자만 기록하는" 스텁으로 바꾼다 — 엔드포인트가 넘기는 모양만 본다."""
    captured: dict = {}

    async def _stub(**kwargs):
        captured.update(kwargs)
        return {"content": "답", "tool_calls": [], "suggested_title": None}

    # 컨트롤러가 agent 모듈을 통해 부르므로(agent.run_chat), 이름을 심을 곳도 agent 모듈이다.
    monkeypatch.setattr(chat_agent, "run_chat", _stub)
    with TestClient(app_module.app) as client:
        yield client, captured


def test_chat_받은_회차_컨텍스트를_그대로_에이전트에_넘긴다(captured_chat):
    """user_id × work_id는 KG 테넌트라 반드시 함께 에이전트로 내려가야 한다 —
    둘 중 하나만 가면 남의 작품 그래프를 읽는다."""
    client, captured = captured_chat

    response = client.post(
        "/api/chat",
        json={
            "userId": 42,
            "workId": 1,
            "sessionId": 7,
            "messages": [{"role": "user", "content": "이번 화 어때?"}],
            "context": {
                "editingEpisode": {"number": 5, "title": "결전", "text": "전문 원고"},
                "viewingEpisodeNumber": 3,
            },
        },
    )

    assert response.status_code == 200
    assert captured["user_id"] == 42
    assert captured["work_id"] == 1
    assert captured["session_id"] == 7
    assert captured["context"] == {
        "editing_episode": {"number": 5, "title": "결전", "text": "전문 원고"},
        "viewing_episode_number": 3,
    }


def test_chat_컨텍스트가_없어도_받는다(captured_chat):
    """편집기를 열지 않고 대화만 하는 경우 — 세 개념 다 없이도 200이어야 한다."""
    client, captured = captured_chat

    response = client.post(
        "/api/chat",
        json={
            "userId": 42,
            "workId": 1,
            "sessionId": 7,
            "messages": [{"role": "user", "content": "안녕"}],
        },
    )

    assert response.status_code == 200
    assert captured["context"] == {"editing_episode": None, "viewing_episode_number": None}


def test_chat_집필_중인_회차의_화수는_없을_수_있다(captured_chat):
    """DRAFT는 화수가 확정되기 전이라 번호가 null로 온다(Episode 도메인 규칙)."""
    client, captured = captured_chat

    response = client.post(
        "/api/chat",
        json={
            "userId": 42,
            "workId": 1,
            "sessionId": 7,
            "messages": [{"role": "user", "content": "안녕"}],
            "context": {"editingEpisode": {"number": None, "title": "새 회차", "text": ""}},
        },
    )

    assert response.status_code == 200
    assert captured["context"]["editing_episode"]["number"] is None
    assert captured["context"]["viewing_episode_number"] is None


def test_chat_인덱싱된_회차는_요청으로_받지_않는다(captured_chat):
    """요청이 indexed_episodes를 보내도 계약에 없는 필드라 컨텍스트에 섞이지 않는다."""
    client, captured = captured_chat

    response = client.post(
        "/api/chat",
        json={
            "userId": 42,
            "workId": 1,
            "sessionId": 7,
            "messages": [{"role": "user", "content": "안녕"}],
            "context": {"indexedEpisodes": [1, 2, 3], "viewingEpisodeNumber": 2},
        },
    )

    assert response.status_code == 200
    # 계약에 없는 필드는 어느 표기로도 컨텍스트에 실리지 않는다.
    assert "indexed_episodes" not in captured["context"]
    assert "indexedEpisodes" not in captured["context"]


# ---------- 와이어 포맷: 응답 키가 전부 camelCase인가 ----------


def _snake_keys(value) -> list[str]:
    """응답 JSON을 재귀 순회해 `_`가 들어간 키를 모은다 — 비면 camelCase 계약 준수다."""
    found: list[str] = []
    if isinstance(value, dict):
        for key, inner in value.items():
            if "_" in key:
                found.append(key)
            found.extend(_snake_keys(inner))
    elif isinstance(value, list):
        for inner in value:
            found.extend(_snake_keys(inner))
    return found


def test_chat_응답_키는_camelCase다(monkeypatch):
    """서비스가 내부 계약(snake_case dict)으로 결과를 줘도 wire에는 camelCase로 나가야 한다.
    tool_calls가 비어 있으면 중첩 키 직렬화를 못 보므로 한 건 채워서 확인한다."""

    async def _stub(**kwargs):
        return {
            "content": "답",
            "tool_calls": [{"name": "hybrid_search", "summary": "검색함", "status": "DONE"}],
            "suggested_title": "제목",
        }

    monkeypatch.setattr(chat_agent, "run_chat", _stub)
    with TestClient(app_module.app) as client:
        response = client.post(
            "/api/chat",
            json={
                "userId": 42,
                "workId": 1,
                "sessionId": 7,
                "messages": [{"role": "user", "content": "안녕"}],
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["toolCalls"] == [{"name": "hybrid_search", "summary": "검색함", "status": "DONE"}]
    assert body["suggestedTitle"] == "제목"
    assert _snake_keys(body) == []


def test_health_점검_결과_키는_camelCase다():
    """health는 DTO 없이 dict를 그대로 내보내므로 alias 변환이 없다 — 점검 결과를 만드는
    _timed가 키를 직접 camelCase로 지켜야 한다(실제 DB 없이 그 지점만 검증)."""
    from src.service import health_service

    result = health_service._timed(lambda: {"uri": "bolt://x"})

    assert "latencyMs" in result
    assert _snake_keys(result) == []


# ---------- 시스템 프롬프트: 세 개념이 각각 다르게 제시되는가 ----------


def test_프롬프트_세_개념이_각각_따로_제시된다():
    prompt = chat_agent._system_prompt(
        {
            "editing_episode": {"number": 5, "title": "결전", "text": "카엘은 검을 들었다."},
            "viewing_episode_number": 3,
        },
        [1, 2, 3],
    )

    # 1) 인덱싱된 회차 = 조회 가능한 범위
    assert "1화, 2화, 3화" in prompt
    # 2) 집필 중인 회차 = 전문이 그대로
    assert "5화 「결전」" in prompt
    assert "카엘은 검을 들었다." in prompt
    # 3) 보고 있는 회차 = 힌트
    assert "3화를 화면에 열어 두고 있다" in prompt
    # 세 개념이 섞이지 않도록 하는 지시가 살아 있다
    assert "episode_manuscript" in prompt
    assert "뭉뚱그리지 마라" in prompt


def test_프롬프트_집필_중인_회차_원고는_잘리지_않는다():
    """전문을 넣기로 한 결정이 코드에 남아 있는지 — 도구(4000자 컷)와 다른 점이다."""
    long_text = "가" * 12000
    prompt = chat_agent._system_prompt(
        {"editing_episode": {"number": 5, "title": "결전", "text": long_text}}, [1]
    )

    assert long_text in prompt
    assert "12000자" in prompt


def test_프롬프트_원고에_중괄호가_있어도_치환이_깨지지_않는다():
    """원고는 작가가 쓴 임의의 텍스트다. 플레이스홀더처럼 생긴 문자열이 있어도 그대로 남아야 한다."""
    prompt = chat_agent._system_prompt(
        {"editing_episode": {"number": 2, "title": "t", "text": "그는 {tool_guide}라고 적었다."}},
        [1, 2],
    )

    assert "그는 {tool_guide}라고 적었다." in prompt
    assert "hybrid_search" in prompt  # 진짜 도구 가이드는 제대로 채워졌다


def test_프롬프트_인덱싱_안_된_회차는_조회_불가라고_못_박는다():
    prompt = chat_agent._system_prompt(None, [1, 2])

    assert "조회할 수 없" in prompt
    assert "인덱싱되지 않았다고 분명히 답해라" in prompt


def test_프롬프트_인덱싱된_회차가_없으면_그렇게_말한다():
    prompt = chat_agent._system_prompt(None, [])

    assert "하나도 없다" in prompt


def test_프롬프트_중간에_빠진_회차를_짚어준다():
    """1~5화 중 3화만 인덱싱에 실패한 상황 — "1~5화 조회 가능"이라고 뭉개면 안 된다."""
    prompt = chat_agent._system_prompt(None, [1, 2, 4, 5])

    assert "중간의 3화는 인덱싱되지 않아 조회할 수 없다" in prompt


def test_프롬프트_컨텍스트가_비면_모른다고_적는다():
    prompt = chat_agent._system_prompt({}, [1])

    assert "집필 중인 회차를 알 수 없다" in prompt
    assert "어느 회차를 열어 두고 있는지는 알 수 없다" in prompt


def test_프롬프트_화수_없는_DRAFT를_0화로_지어내지_않는다():
    prompt = chat_agent._system_prompt(
        {"editing_episode": {"number": None, "title": "새 회차", "text": "초고"}}, [1]
    )

    assert "화수가 아직 배정되지 않은 새 회차" in prompt
    assert "0화" not in prompt


def test_프롬프트_보고_있는_회차가_집필_중인_회차와_같으면_그렇게_알린다():
    prompt = chat_agent._system_prompt(
        {
            "editing_episode": {"number": 4, "title": "t", "text": "본문"},
            "viewing_episode_number": 4,
        },
        [1, 2, 3],
    )

    assert "집필 중인 회차와 같다" in prompt


# ---------- run_chat: 인덱싱된 회차를 매 턴 그래프에서 읽는가 ----------


@pytest.fixture
def stub_llm(monkeypatch):
    """LLM 호출을 가로채 요청 kwargs를 기록한다(실제 API 호출 없음).

    채팅은 responses API를 쓰므로 가짜 응답도 그 형상(id/output/output_text)을 흉내낸다 —
    output이 비면 도구 호출이 없다는 뜻이고 output_text가 최종 답변이다.
    """
    seen: list[dict] = []

    async def _stub(**kwargs):
        seen.append(kwargs)
        return SimpleNamespace(id="resp_test", output=[], output_text="답변")

    # 채팅이 자기 클라이언트를 들고 있던 시절에는 `_create_with_retry`를 막았다. 지금은
    # 공용 관문(src/common/openai_client.create_response)을 쓰므로 그 이름을 막는다 —
    # agent 모듈에 바인딩된 이름이라 다른 서비스(extract_service 등)와 방식이 같다.
    monkeypatch.setattr(chat_agent, "create_response", _stub)
    return seen


def test_run_chat_인덱싱된_회차를_요청이_아니라_그래프에서_읽는다(monkeypatch, stub_llm):
    calls: list[tuple[int, int]] = []

    def _fake_fetch(user_id: int, work_id: int) -> list[int]:
        calls.append((user_id, work_id))
        return [1, 2]

    monkeypatch.setattr(chat_agent, "fetch_indexed_episodes", _fake_fetch)

    result = asyncio.run(
        chat_agent.run_chat(
            user_id=4,
            work_id=9,
            session_id=1,
            messages=[
                {"role": "user", "content": "안녕"},
                {"role": "assistant", "content": "네"},
                {"role": "user", "content": "2화 요약"},
            ],
            context={
                "editing_episode": {"number": 3, "title": "추적", "text": "원고 전문입니다"},
                "viewing_episode_number": 1,
            },
        )
    )

    assert result["content"] == "답변"
    # 요청이 아니라 그래프에 물었고, 그 대상은 요청의 테넌트(user_id × work_id)다.
    assert calls == [(4, 9)]

    system_prompt = stub_llm[0]["input"][0]["content"]
    assert "1화, 2화" in system_prompt
    assert "원고 전문입니다" in system_prompt
    assert "1화를 화면에 열어 두고 있다" in system_prompt


def test_run_chat_그래프_조회가_실패해도_대화는_계속된다(monkeypatch, stub_llm):
    """fetch가 빈 리스트를 돌려주는 상황(그래프 다운) — 죽지 않고 "조회 불가"로 답하게 만든다."""
    monkeypatch.setattr(chat_agent, "fetch_indexed_episodes", lambda user_id, work_id: [])

    result = asyncio.run(
        chat_agent.run_chat(
            user_id=42,
            work_id=1,
            session_id=1,
            messages=[
                {"role": "user", "content": "안녕"},
                {"role": "assistant", "content": "네"},
                {"role": "user", "content": "3화 내용"},
            ],
            context=None,
        )
    )

    assert result["content"] == "답변"
    assert "하나도 없다" in stub_llm[0]["input"][0]["content"]


def test_run_chat_요청이_responses_계약을_지킨다(monkeypatch, stub_llm):
    """luna 500 회귀(버그②) 방지의 핵심 계약 — 채팅은 responses API로 나가며
    추론 강도 high를 명시하고, tools는 responses의 평탄형 스키마여야 한다."""
    monkeypatch.setattr(chat_agent, "fetch_indexed_episodes", lambda user_id, work_id: [1])

    asyncio.run(
        chat_agent.run_chat(
            user_id=1,
            work_id=1,
            session_id=1,
            messages=[
                {"role": "user", "content": "안녕"},
                {"role": "assistant", "content": "네"},
                {"role": "user", "content": "1화 요약"},
            ],
            context=None,
        )
    )

    kwargs = stub_llm[0]
    assert kwargs["reasoning"] == {"effort": "high"}
    assert "messages" not in kwargs and "input" in kwargs
    # 평탄형: chat.completions의 중첩({"function": {...}})이 아니어야 한다.
    first_tool = kwargs["tools"][0]
    assert "function" not in first_tool
    assert first_tool["name"] in {"hybrid_search", "fact_search", "entity_search",
                                  "episode_manuscript", "work_settings"}
