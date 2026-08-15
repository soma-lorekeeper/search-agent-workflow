"""서비스가 평가 하네스와 **같은 프롬프트**를 만드는지 대조한다.

탐지 파이프라인의 성능 수치(검출 22/25, 클린 회차 오탐 0)는 하네스에서 잰 것이다. 서비스가
그 수치를 물려받는 근거는 "같은 입력에 같은 프롬프트를 만든다"는 것뿐이라, 문안이 한 글자만
달라져도 측정치는 무효가 된다.

사람이 옮겨 적은 문자열은 언젠가 어긋난다 — 그때 조용히 성능만 떨어지는 대신 이 테스트가
깨지게 한다. LLM이나 DB를 부르지 않는다(문자열 조립만 비교한다).
"""

from __future__ import annotations

import pytest

# 하네스는 scripts/ 아래 있고 PYTHONPATH 없이도 임포트되도록 conftest가 루트를 잡아준다.
eval_claims = pytest.importorskip(
    "scripts.eval_claims", reason="평가 하네스가 없으면 대조할 기준도 없다"
)

from src.service.detect import docstore, judge_service, prompts  # noqa: E402
from src.service.detect.extract_service import CHUNK_SIZE, _cap_for  # noqa: E402

# 검색 결과 한 건이 든 최소 evidence. 두 구현에 같은 입력을 준다.
_EVIDENCE = {
    # run·draft는 하네스가 파일 이름을 짓는 데 쓰던 필드다. 서비스는 안 쓰지만
    # 하네스 쪽 build_docstore가 요구하므로 대조 입력에는 넣어 둔다.
    "run": "parity",
    "draft": "parity",
    "records": [
        {
            "claim": {"id": "P1", "quote": "인용", "axis": "축", "value": "값", "lines": [3]},
            "channels": [
                {
                    "tool": "hybrid_search",
                    "args": {},
                    "content": "채널 텍스트",
                    "items": [
                        {
                            "metadata": {
                                "kind": "chunk",
                                "eid": "e1",
                                "chapter": 3,
                                "chunk_index": 12,
                                "text": "원문 조각",
                            }
                        }
                    ],
                }
            ],
        }
    ],
}
_CLAIMS = [_EVIDENCE["records"][0]["claim"]]


def test_추출_프롬프트_문안이_하네스와_같다():
    """qav2 확정 문안 그대로여야 한다. 하네스는 치환 체인으로 만들고 서비스는 결과를 굳혀 뒀다."""
    assert prompts.EXTRACT_CRITERIA == eval_claims._CRITERIA_QAV2
    assert prompts.EXTRACT_FEWSHOT == eval_claims._FEWSHOT_QAV2
    assert prompts.ENTITY_NODE_HEADER == eval_claims._NODE_HEADER


def test_추출_범위_문단의_상한이_하네스와_같다():
    """상한은 조각 크기에서 유도된다 — 조각 크기를 바꾸면 이 숫자도 함께 움직여야 한다."""
    expected = eval_claims._SCOPE_CHUNK_NO_DRAFT.replace(
        "{max_claims}", str(eval_claims.cap_for(CHUNK_SIZE))
    )
    assert prompts.EXTRACT_SCOPE.replace("{max_claims}", str(_cap_for(CHUNK_SIZE))) == expected


def test_판정_프롬프트_문안이_하네스_p4와_같다():
    criteria, fewshot = eval_claims._JUDGE_PROMPTS["p4"]
    assert prompts.JUDGE_CRITERIA == criteria
    assert prompts.JUDGE_FEWSHOT == fewshot


def test_문서고_렌더가_하네스와_같다():
    """별칭 부여와 렌더가 같아야 판정기가 보는 근거 문자열이 같다."""
    harness = eval_claims.build_docstore(_EVIDENCE)
    service = docstore.build_docstore(_EVIDENCE)
    assert docstore.render_docstore(service) == eval_claims.render_docstore(harness)
    assert docstore.render_claim_refs(service, _CLAIMS) == eval_claims.render_claim_refs(
        harness, _CLAIMS
    )


def test_판정_system_전체가_하네스와_같다():
    """기준+예시+문서고를 잇는 순서와 구분자까지 같은지 본다."""
    harness_store = eval_claims.build_docstore(_EVIDENCE)
    service_store = docstore.build_docstore(_EVIDENCE)
    eval_claims.JUDGE_PROMPT_VERSION = "p4"
    assert judge_service.build_system(service_store) == eval_claims.build_judge_system(
        harness_store
    )


def test_확정_구성값이_하네스와_같다():
    """조각 크기·상한·top_k·임계값이 하네스가 잰 그 구성인지 본다.

    이 다섯 숫자가 성능 수치(검출 22/25, 오탐 0)의 조건이다. 하나라도 다르면 서비스는
    측정한 적 없는 구성으로 도는 것이고, 그 사실이 어디에도 드러나지 않는다.
    """
    from src.service.detect import judge_service, retrieve_service
    from src.service.detect.extract_service import CHUNK_SIZE, _cap_for

    # 하네스가 확정한 변형: line-3000 = (조각 3000자, 원고 미주입)
    harness_chunk, harness_include_draft = eval_claims.VARIANTS["line-3000"]
    assert CHUNK_SIZE == harness_chunk
    assert harness_include_draft is False, "원고를 판정에 넣는 구성은 검출이 21→16으로 떨어졌다"

    assert _cap_for(CHUNK_SIZE) == eval_claims.cap_for(harness_chunk)
    assert retrieve_service.TOP_K == 3
    assert judge_service.ERROR_THRESHOLD == 7


def test_탐지가_하네스와_같은_모델로_돈다():
    """프롬프트가 같아도 모델이 다르면 잰 수치를 물려받지 못한다.

    실제로 서비스는 한동안 OPENAI_MODEL(채팅용)로 탐지를 돌렸다. 파리티 테스트가
    프롬프트·조각 크기·임계값만 대조하고 **모델은 안 봤기 때문에** 그 어긋남이
    드러나지 않았다. 이 구멍을 막는 자리다.
    """
    from src.config import EXTRACTION_MODEL

    assert EXTRACTION_MODEL == eval_claims.EXTRACT_MODEL


def test_탐지가_추론_강도를_넘기지_않는다(monkeypatch):
    """하네스 기본은 reasoning_effort **미지정**이다(scripts/eval_claims.py:53).

    값을 넘기면 다른 구성이 되는데, 응답은 멀쩡히 오고 결과도 그럴듯해서 눈에 띄지 않는다.
    추출·판정 두 경로 모두 kwargs에 그 키가 없어야 한다.
    """
    import asyncio
    import json

    from src.service.detect import extract_service, judge_service

    seen: list[dict] = []

    class _FakeResponse:
        def __init__(self, content):
            message = type("_M", (), {"content": content})()
            self.choices = [type("_C", (), {"message": message})()]

    async def _fake(**kwargs):
        seen.append(kwargs)
        return _FakeResponse(json.dumps({"claims": [], "verdicts": []}))

    monkeypatch.setattr(extract_service, "create_completion", _fake)
    monkeypatch.setattr(judge_service, "create_completion", _fake)
    monkeypatch.setattr(extract_service, "_build_system", lambda tenant, ch: "시스템")
    monkeypatch.setattr(extract_service, "split_lines", lambda text: [text])
    monkeypatch.setattr(extract_service, "number_lines", lambda lines: "L1: " + lines[0])

    asyncio.run(extract_service.extract("원고 한 줄", tenant=None, up_to_chapter=5))
    with pytest.raises(RuntimeError):
        # 판정 응답이 비면 "오류 0건"이 아니라 실패다 — 여기서는 kwargs만 보면 된다.
        asyncio.run(judge_service.judge([{"id": "P1", "quote": "q"}], {"records": []}))

    assert seen, "두 경로 모두 호출돼야 한다"
    for kwargs in seen:
        assert "reasoning_effort" not in kwargs, kwargs


def test_라우팅이_하네스와_같은_채널을_연다():
    """어떤 질의를 어느 채널에 던지는지가 검색 도달률을 정한다."""
    from src.service.detect.routing import route_qav

    for claim in (
        {"axis": "축", "value": "값", "quote": "인용"},
        {"axis": "축", "value": "", "quote": "인용"},
        {"axis": "", "value": "", "quote": "인용"},
        {},
    ):
        assert route_qav(claim) == eval_claims.route_qav(claim), claim
        assert route_qav(claim, with_quote=True) == eval_claims.route_qav(claim, with_quote=True)
