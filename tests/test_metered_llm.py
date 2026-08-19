"""인덱싱 LLM 어댑터가 라이브러리 계약을 지키는지 고정한다.

라이브러리의 `OpenAILLM`을 상속하지 않고 직접 구현했기 때문에, 라이브러리가 기대하는
호출 규약을 **우리가** 지켜야 한다. 여기서 틀리면 조용하다 — 스키마 변환이 어긋나면
추출이 빈 그래프를 만들고, v1/v2 분기를 놓치면 요약이 엉뚱한 메시지로 나간다. 둘 다
예외가 아니라 "결과가 이상함"으로 나타난다.

LLM을 부르지 않는다. 관문(create_completion)을 가짜로 바꿔 무엇이 전달되는지만 본다.
"""

from __future__ import annotations

import asyncio
import weakref
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from src.common import graphrag, llm_limit

MODEL = "gpt-test-luna"


class _예시스키마(BaseModel):
    """response_format으로 넘기는 Pydantic 모델(실제로는 Neo4jGraph)."""

    이름: str


@pytest.fixture(autouse=True)
def 상태_초기화(monkeypatch):
    monkeypatch.setattr(llm_limit, "_buckets", {})
    monkeypatch.setattr(llm_limit, "_async_slots", weakref.WeakKeyDictionary())
    monkeypatch.setattr(llm_limit, "_thread_slots", {})


def _응답(content: str = "결과", usage=None):
    message = SimpleNamespace(content=content)
    return SimpleNamespace(choices=[SimpleNamespace(message=message)], usage=usage)


@pytest.fixture
def 전달된_인자(monkeypatch):
    """관문에 무엇이 넘어가는지 기록한다."""
    seen: list[dict] = []

    async def _stub(**kwargs):
        seen.append(kwargs)
        return _응답()

    monkeypatch.setattr(graphrag, "create_completion", _stub)
    return seen


def _llm(**params) -> graphrag.MeteredLLM:
    return graphrag.MeteredLLM(model_name=MODEL, model_params=params)


# ---------- v1 / v2 분기 ----------


def test_문자열_입력은_v1_규약으로_조립된다(전달된_인자):
    """회차 요약·전역 요약·description 병합이 이 경로를 쓴다.

    라이브러리가 해주던 system/user 조립을 우리가 대신한다 — 빠뜨리면 system 지시가
    통째로 사라져 요약 형식이 무너진다.
    """
    asyncio.run(_llm().ainvoke("원고 본문", system_instruction="요약해라"))

    assert 전달된_인자[0]["messages"] == [
        {"role": "system", "content": "요약해라"},
        {"role": "user", "content": "원고 본문"},
    ]
    assert "response_format" not in 전달된_인자[0], "v1에는 구조화 출력이 없다"


def test_메시지_배열_입력은_v2_규약으로_그대로_넘어간다(전달된_인자):
    """회차 KG 추출이 이 경로를 쓴다. 배열 조립은 호출자(라이브러리 extractor) 몫이다."""
    messages = [{"role": "user", "content": "추출할 원고"}]
    asyncio.run(_llm().ainvoke(messages, response_format=_예시스키마))

    assert 전달된_인자[0]["messages"] == messages


def test_알_수_없는_입력_타입은_즉시_거부한다():
    """조용히 넘기면 관문이 이상한 messages를 그대로 OpenAI에 보낸다."""
    with pytest.raises(ValueError):
        asyncio.run(_llm().ainvoke(123))


# ---------- 스키마 변환 ----------


def test_Pydantic_모델을_라이브러리와_같은_json_schema로_바꾼다(전달된_인자):
    """`neo4j_graphrag/llm/openai_llm.py`의 __ainvoke_v2에서 자구 그대로 옮긴 변환이다.

    strict=True와 name이 클래스명이라는 것까지 같아야 한다 — 여기가 어긋나면 추출이
    예외 없이 빈 그래프를 돌려주고, 그 회차는 조용히 인덱싱된 것으로 기록된다.
    """
    asyncio.run(_llm().ainvoke([{"role": "user", "content": "x"}], response_format=_예시스키마))

    assert 전달된_인자[0]["response_format"] == {
        "type": "json_schema",
        "json_schema": {
            "name": "_예시스키마",
            "strict": True,
            "schema": _예시스키마.model_json_schema(),
        },
    }


def test_dict_형식_response_format은_그대로_전달된다(전달된_인자):
    """{"type": "json_object"} 같은 형태는 변환 없이 넘어가야 한다."""
    asyncio.run(
        _llm().ainvoke([{"role": "user", "content": "x"}], response_format={"type": "json_object"})
    )
    assert 전달된_인자[0]["response_format"] == {"type": "json_object"}


def test_model_params의_response_format은_v2에서_빠진다(전달된_인자):
    """생성자와 호출 인자 양쪽에 있으면 충돌한다(라이브러리도 같은 처리를 한다)."""
    llm = _llm(prompt_cache_key="키", response_format={"type": "json_object"})
    asyncio.run(llm.ainvoke([{"role": "user", "content": "x"}], response_format=_예시스키마))

    assert 전달된_인자[0]["response_format"]["type"] == "json_schema"


# ---------- 전달 ----------


def test_model_params가_호출에_실린다(전달된_인자):
    """prompt_cache_key는 프롬프트 캐시 라우팅을, reasoning_effort는 추론 강도를 정한다.
    빠지면 비용과 품질이 둘 다 조용히 달라진다."""
    asyncio.run(_llm(prompt_cache_key="lorekeeper-extract", reasoning_effort="high").ainvoke("x"))

    assert 전달된_인자[0]["model"] == MODEL
    assert 전달된_인자[0]["prompt_cache_key"] == "lorekeeper-extract"
    assert 전달된_인자[0]["reasoning_effort"] == "high"


def test_usage를_라이브러리_형식으로_옮긴다(monkeypatch):
    """extractor가 LLMResponse.usage를 읽는다. None이어도 죽지 않아야 한다."""

    async def _stub(**kwargs):
        return _응답(usage=SimpleNamespace(prompt_tokens=10, completion_tokens=3, total_tokens=13))

    monkeypatch.setattr(graphrag, "create_completion", _stub)
    응답 = asyncio.run(_llm().ainvoke("x"))

    assert 응답.content == "결과"
    assert 응답.usage.request_tokens == 10
    assert 응답.usage.response_tokens == 3
    assert 응답.usage.total_tokens == 13


def test_usage가_없어도_응답을_돌려준다(전달된_인자):
    응답 = asyncio.run(_llm().ainvoke("x"))
    assert 응답.content == "결과"
    assert 응답.usage is None


# ---------- 라이브러리 계약 ----------


def test_구조화_출력을_지원한다고_선언한다():
    """이 플래그가 없으면 LLMEntityRelationExtractor가 __init__에서 거부한다 —
    서버 기동 시점이 아니라 인덱싱을 시작할 때 터진다."""
    assert graphrag.MeteredLLM.supports_structured_output is True


def test_동기_invoke는_쓰지_않는다():
    """동기 경로가 생기면 이벤트 루프를 막는다. 조용히 도는 것보다 막는 편이 낫다."""
    with pytest.raises(NotImplementedError):
        _llm().invoke("x")


def test_모든_인스턴스의_토큰이_한_버킷에_모인다(monkeypatch):
    """build_llm은 호출마다 새 인스턴스를 만든다(추출·회차요약·전역요약·병합이 각자 부른다).

    예전에는 인스턴스별 카운터라 추출 것만 읽혔고 나머지 셋은 아무도 안 셌다. 지금은
    모델 버킷에 적립되므로 인스턴스가 몇 개든 합산된다.
    """
    async def _stub(**kwargs):
        llm_limit.observe(kwargs["model"], {"x-ratelimit-remaining-tokens": "777"})
        return _응답()

    monkeypatch.setattr(graphrag, "create_completion", _stub)

    async def 네_인스턴스():
        for _ in range(4):
            await _llm().ainvoke("x")

    asyncio.run(네_인스턴스())
    assert llm_limit.snapshot()[MODEL].updated_at is not None
    assert llm_limit.remaining(MODEL).remaining == 777
