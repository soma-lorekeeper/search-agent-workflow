"""채팅 회차 컨텍스트 계약 테스트.

검증 대상은 "회차에 관한 세 개념이 서로 섞이지 않는가"다:
  1. 인덱싱된 회차 — 요청이 아니라 Neo4j에서 온다(요청에 실려도 무시된다).
  2. 집필 중인 회차 — 원고 **전문**이 시스템 프롬프트에 그대로 들어간다.
  3. 보고 있는 회차 — 번호만 오고, 기본 주제가 아니라 힌트로 제시된다.

LLM·Neo4j·PostgreSQL은 전부 스텁이라 실제 비용이 들지 않는다.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src import webapp
from src.chat import agent as chat_agent
from src.chat import kg_scope
from src.chat.tools import ChatTool, build_chat_tools

# KG에 인덱싱돼 있다고 보는 작품. 실제 값은 환경변수라서 테스트에서 고정한다.
WORK_ID = 1
OTHER_WORK_ID = WORK_ID + 1

# ---------- /api/chat 요청 계약 ----------


@pytest.fixture
def captured_chat(monkeypatch):
    """run_chat을 "인자만 기록하는" 스텁으로 바꾼다 — 엔드포인트가 넘기는 모양만 본다."""
    captured: dict = {}

    async def _stub(**kwargs):
        captured.update(kwargs)
        return {"content": "답", "tool_calls": [], "suggested_title": None}

    monkeypatch.setattr(webapp, "run_chat", _stub)
    monkeypatch.setattr(kg_scope, "KG_INDEXED_WORK_ID", WORK_ID)
    with TestClient(webapp.app) as client:
        yield client, captured


def test_chat_다른_작품의_질문은_400이다(captured_chat):
    """KG에 작품 격리가 없어서, 다른 작품으로 물으면 남의 작품 그래프로 답하게 된다.

    답이 그럴듯해 보이는 게 특히 나쁘다 — 작가는 자기 작품 설정으로 알고 그걸 근거로 글을 쓴다.
    """
    client, captured = captured_chat

    response = client.post(
        "/api/chat",
        json={
            "work_id": OTHER_WORK_ID,
            "session_id": 7,
            "messages": [{"role": "user", "content": "주인공이 누구야?"}],
        },
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert str(OTHER_WORK_ID) in detail and str(WORK_ID) in detail
    assert captured == {}  # 에이전트를 부르지도 않는다(LLM 비용 0)


def test_chat_받은_회차_컨텍스트를_그대로_에이전트에_넘긴다(captured_chat):
    client, captured = captured_chat

    response = client.post(
        "/api/chat",
        json={
            "work_id": 1,
            "session_id": 7,
            "messages": [{"role": "user", "content": "이번 화 어때?"}],
            "context": {
                "editing_episode": {"number": 5, "title": "결전", "text": "전문 원고"},
                "viewing_episode_number": 3,
            },
        },
    )

    assert response.status_code == 200
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
        json={"work_id": 1, "session_id": 7, "messages": [{"role": "user", "content": "안녕"}]},
    )

    assert response.status_code == 200
    assert captured["context"] == {"editing_episode": None, "viewing_episode_number": None}


def test_chat_집필_중인_회차의_화수는_없을_수_있다(captured_chat):
    """DRAFT는 화수가 확정되기 전이라 번호가 null로 온다(Episode 도메인 규칙)."""
    client, captured = captured_chat

    response = client.post(
        "/api/chat",
        json={
            "work_id": 1,
            "session_id": 7,
            "messages": [{"role": "user", "content": "안녕"}],
            "context": {"editing_episode": {"number": None, "title": "새 회차", "text": ""}},
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
            "work_id": 1,
            "session_id": 7,
            "messages": [{"role": "user", "content": "안녕"}],
            "context": {"indexed_episodes": [1, 2, 3], "viewing_episode_number": 2},
        },
    )

    assert response.status_code == 200
    assert "indexed_episodes" not in captured["context"]


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
    assert "kg_vector_search" in prompt  # 진짜 도구 가이드는 제대로 채워졌다


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
    """LLM 호출을 가로채 시스템 프롬프트를 기록한다(실제 API 호출 없음)."""
    seen: list[list[dict]] = []

    async def _stub(**kwargs):
        seen.append(kwargs["messages"])
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="답변", tool_calls=None))]
        )

    monkeypatch.setattr(chat_agent, "_create_with_retry", _stub)
    return seen


def test_run_chat_인덱싱된_회차를_요청이_아니라_그래프에서_읽는다(monkeypatch, stub_llm):
    calls: list[int] = []

    def _fake_fetch(work_id: int) -> list[int]:
        calls.append(work_id)
        return [1, 2]

    monkeypatch.setattr(chat_agent, "fetch_indexed_episodes", _fake_fetch)

    result = asyncio.run(
        chat_agent.run_chat(
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
    # 요청이 아니라 그래프에 물었고, 그 대상은 요청의 work_id다.
    assert calls == [9]

    system_prompt = stub_llm[0][0]["content"]
    assert "1화, 2화" in system_prompt
    assert "원고 전문입니다" in system_prompt
    assert "1화를 화면에 열어 두고 있다" in system_prompt


def test_run_chat_그래프_조회가_실패해도_대화는_계속된다(monkeypatch, stub_llm):
    """fetch가 빈 리스트를 돌려주는 상황(그래프 다운) — 죽지 않고 "조회 불가"로 답하게 만든다."""
    monkeypatch.setattr(chat_agent, "fetch_indexed_episodes", lambda work_id: [])

    result = asyncio.run(
        chat_agent.run_chat(
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
    assert "하나도 없다" in stub_llm[0][0]["content"]


# ---------- 에이전트 루프(LangGraph)의 통제 장치 ----------
# 여기부터는 "루프가 무엇을 절대 하지 않는가"를 검사한다. 구현이 손수 루프에서 그래프로
# 바뀌어도 아래 성질은 그대로여야 한다 — 이건 구조가 아니라 안전장치에 대한 계약이다.


def _tool_call(name: str, arguments: dict, call_id: str = "call_1") -> SimpleNamespace:
    """OpenAI 응답의 tool_call 한 건(모델이 도구를 부르겠다고 말한 모양)."""
    return SimpleNamespace(
        id=call_id,
        function=SimpleNamespace(name=name, arguments=json.dumps(arguments, ensure_ascii=False)),
    )


def _response(content: str | None = None, tool_calls: list | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content, tool_calls=tool_calls))]
    )


@pytest.fixture
def loop_harness(monkeypatch):
    """도구 1종짜리 가짜 레지스트리 + LLM 스텁을 끼운다.

    반환: (seen_kwargs, tool_runs, set_replies) — 모델에 실제로 넘어간 요청 kwargs 목록,
    도구가 실행된 인자 목록, 그리고 턴별 응답을 지정하는 함수.
    """
    seen: list[dict] = []
    runs: list[tuple] = []
    replies: list[SimpleNamespace] = []

    def _run(work_id: int, query_text: str = "") -> tuple[str, str]:
        runs.append((work_id, query_text))
        return f"결과({query_text})", f"가짜 조회 · {query_text}"

    fake_tool = ChatTool(
        name="fake_search",
        description="테스트용 도구",
        parameters={
            "type": "object",
            "properties": {"query_text": {"type": "string"}},
            "required": ["query_text"],
        },
        run=_run,
    )
    schemas = [
        {
            "type": "function",
            "function": {
                "name": fake_tool.name,
                "description": fake_tool.description,
                "parameters": fake_tool.parameters,
            },
        }
    ]

    async def _stub(**kwargs):
        seen.append(kwargs)
        # 지정된 응답이 떨어지면 마지막 응답을 계속 반복한다(예산·턴 상한 검사용).
        return replies[len(seen) - 1] if len(seen) <= len(replies) else replies[-1]

    monkeypatch.setattr(chat_agent, "build_chat_tools", lambda: (schemas, {fake_tool.name: fake_tool}))
    monkeypatch.setattr(chat_agent, "fetch_indexed_episodes", lambda work_id: [1, 2])
    monkeypatch.setattr(chat_agent, "_create_with_retry", _stub)

    def set_replies(*responses: SimpleNamespace) -> None:
        replies.extend(responses)

    return seen, runs, set_replies


def _run_one_turn(work_id: int = 1) -> dict:
    return asyncio.run(
        chat_agent.run_chat(
            work_id=work_id,
            session_id=1,
            # 첫 턴이 아니게 만들어 제목 생성 호출이 끼어들지 않게 한다(루프만 검사한다).
            messages=[
                {"role": "user", "content": "안녕"},
                {"role": "assistant", "content": "네"},
                {"role": "user", "content": "카엘 상태 알려줘"},
            ],
            context=None,
        )
    )


def test_예산을_다_쓰면_도구_목록_자체를_보내지_않는다(loop_harness):
    """예산 소진은 "부르지 마라"는 부탁이 아니라 **부를 수단을 없애는 것**이어야 한다.

    프롬프트로만 말리면 모델은 태연히 계속 부른다. tools 파라미터가 요청에서 빠져야
    부를 방법 자체가 사라진다.
    """
    seen, runs, set_replies = loop_harness
    # 모델이 매 턴 도구를 부르려 든다(예산이 끝나도 멈추지 않는 최악의 경우).
    set_replies(_response(tool_calls=[_tool_call("fake_search", {"query_text": "카엘"})]))

    result = _run_one_turn()

    # 1) 예산(5회)만큼만 tools가 실려 나가고, 그 뒤로는 아예 빠진다.
    with_tools = [i for i, kwargs in enumerate(seen) if "tools" in kwargs]
    assert with_tools == [0, 1, 2, 3, 4]
    assert all("tool_choice" not in kwargs for kwargs in seen[chat_agent.MAX_TOOL_CALLS :])
    # 2) 도구는 예산 횟수만큼만 실제로 실행된다.
    assert len(runs) == chat_agent.MAX_TOOL_CALLS
    assert len(result["tool_calls"]) == chat_agent.MAX_TOOL_CALLS
    # 3) 턴 상한은 예산과 별개의 안전판이다 — 예산이 0이 된 뒤로도 모델이 계속 도구를
    #    부르려 하지만 MAX_TURNS에서 끊긴다.
    assert len(seen) == chat_agent.MAX_TURNS
    assert "시간이 너무 오래 걸렸" in result["content"]
    # 4) 예산 초과 호출도 tool 메시지로 답을 돌려준다(안 돌려주면 다음 요청이 400이 된다).
    over_budget = [
        m
        for kwargs in seen
        for m in kwargs["messages"]
        if m.get("role") == "tool" and "예산" in m["content"]
    ]
    assert over_budget


def test_병렬_도구_호출은_꺼둔다(loop_harness):
    """예산 회계를 정확히 유지하려고 한 번에 하나씩만 부르게 한다."""
    seen, _runs, set_replies = loop_harness
    set_replies(_response(content="답변"))

    _run_one_turn()

    assert seen[0]["parallel_tool_calls"] is False


def test_도구가_실패해도_대화는_끝나지_않는다(loop_harness, monkeypatch):
    """도구 실패는 "근거 부족"이지 대화의 끝이 아니다.

    오류 문자열을 도구 결과로 돌려줘야 모델이 "확인하지 못했다"고 답할 수 있다. 예외를 그대로
    올리면 작가에게는 답변 대신 에러 말풍선만 남는다.
    """
    seen, _runs, set_replies = loop_harness

    def _boom(work_id: int, query_text: str = "") -> tuple[str, str]:
        raise RuntimeError("Neo4j 연결 실패")

    schemas, _ = chat_agent.build_chat_tools()  # loop_harness가 끼운 가짜 스키마
    monkeypatch.setattr(
        chat_agent,
        "build_chat_tools",
        lambda: (schemas, {"fake_search": ChatTool("fake_search", "d", {}, _boom)}),
    )
    set_replies(
        _response(tool_calls=[_tool_call("fake_search", {"query_text": "카엘"})]),
        _response(content="확인하지 못했습니다."),
    )

    result = _run_one_turn()

    # 1) 대화는 정상 종료되고 모델은 한 번 더 답할 기회를 얻는다.
    assert result["content"] == "확인하지 못했습니다."
    # 2) 실패는 UI에 FAILED로 남는다(무엇을 찾다 실패했는지 작가가 봐야 한다).
    assert result["tool_calls"] == [
        {"name": "fake_search", "summary": "fake_search 조회 실패", "status": "FAILED"}
    ]
    # 3) 오류 문자열이 도구 결과로 모델에게 전달된다.
    tool_messages = [m for m in seen[1]["messages"] if m.get("role") == "tool"]
    assert "도구 실행 오류: Neo4j 연결 실패" in tool_messages[0]["content"]


def test_work_id는_스키마에_없지만_도구에는_전달된다(loop_harness):
    """조회 대상 작품은 서버가 정하는 값이지 모델이 고를 값이 아니다.

    스키마에 넣으면 모델이 작품 번호를 지어내 남의 작품을 읽으려 드는 경로가 생긴다.
    그래서 모델에게는 감추고, 실행 시점에 요청의 값을 주입한다.
    """
    _seen, runs, set_replies = loop_harness
    set_replies(
        _response(tool_calls=[_tool_call("fake_search", {"query_text": "카엘"})]),
        _response(content="답변"),
    )

    result = _run_one_turn(work_id=42)

    assert result["tool_calls"][0]["status"] == "DONE"
    assert runs == [(42, "카엘")]  # 모델이 준 인자에는 없던 work_id가 실행 시점에 주입됐다


def test_실제_도구_스키마에는_work_id가_없다():
    """가짜 도구가 아니라 진짜 6종 스키마에 대한 검사 — 모델이 볼 수 있는 전체 표면을 훑는다."""
    schemas, tools = build_chat_tools()

    assert "work_id" not in json.dumps(schemas, ensure_ascii=False)
    # 반대로 실행기는 전부 work_id를 첫 인자로 받는다(작품 격리 전제를 인터페이스에 박아둔 것).
    for tool in tools.values():
        assert tool.run.__code__.co_varnames[0] == "work_id"


def test_모델이_work_id를_지어내도_주입값을_덮어쓰지_못한다(loop_harness):
    """스키마에 없어도 모델은 없는 인자를 만들어 낼 수 있다 — 그때 조용히 먹히면 안 된다."""
    _seen, runs, set_replies = loop_harness
    set_replies(
        _response(tool_calls=[_tool_call("fake_search", {"query_text": "카엘", "work_id": 999})]),
        _response(content="확인하지 못했습니다."),
    )

    result = _run_one_turn(work_id=1)

    # 위조된 work_id로 실행되는 대신 호출이 실패하고, 대화는 계속된다.
    assert runs == []
    assert result["tool_calls"][0]["status"] == "FAILED"
