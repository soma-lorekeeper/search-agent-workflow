"""설정 오류 탐지 파이프라인의 단위 계약 테스트.

**LLM·Neo4j·PostgreSQL을 부르지 않는다.** LLM이 필요한 자리는 유일한 관문인
`create_completion`을 가짜로 바꿔 고정 JSON을 돌려주고, 그래프가 필요한 자리는 검색
도구·검색 결과를 가짜 객체로 넣는다.

여기 모인 것은 전부 **"틀려도 예외가 안 나는" 계약**이다. 줄 번호가 조각마다 1로 돌아가도,
claim 번호가 뒤섞여도, 회차 상한이 반대로 걸려도, 폴백이 조용히 0점을 채워도 응답은
멀쩡한 모양으로 나온다 — 그리고 작가는 그 그럴듯한 오답을 믿고 원고를 고친다. 그래서
값 자체를 못박는다.
"""

from __future__ import annotations

import asyncio
import json
import re

from src.common.tenant import Tenant
from src.service.detect import (
    entity_nodes,
    extract_service,
    judge_service,
    retrieve_service,
    routing,
)
from src.service.detect.extract_service import CHUNK_SIZE, _cap_for, _chunk, assign_claim_ids
from src.service.detect.judge_service import ERROR_THRESHOLD, _resolve_cited, parse_verdicts
from src.service.detect.lines import number_lines, split_lines
from src.service.detect.retrieve_service import TOP_K, _is_future, _norm, _retrieve_one
from src.service.detect.routing import route_qav

TENANT = Tenant.of(42, 1)

# 조각이 두 개 이상 나오도록 CHUNK_SIZE(3000자)를 넘기는 원고. 문장이 마침표로 끝나서
# kss가 문장 단위로 자르고, 문장 안에 순번이 박혀 있어 줄 번호와 내용을 대조할 수 있다.
LONG_TEXT = "".join(
    f"{i}번 장면에서 카엘은 낡은 검을 고쳐 쥐고 북쪽 성문을 향해 천천히 걸어갔다. "
    for i in range(1, 101)
)

_L_PREFIX = re.compile(r"^L(\d+): ")


# ---------------------------------------------------------------------------
# LLM 관문을 대신할 가짜들
# ---------------------------------------------------------------------------


class _FakeResponse:
    """chat.completions 응답 흉내. `.choices[0].message.content`만 있으면 충분하다.

    usage는 두지 않는다 — `usage.from_response`가 usage 없는 응답을 0으로 처리하므로
    토큰 집계는 이 파일의 관심사에서 빠진다.
    """

    def __init__(self, content: str):
        message = type("_M", (), {"content": content})()
        self.choices = [type("_C", (), {"message": message})()]


class _FakeItem:
    """retriever가 돌려주는 결과 항목 하나(neo4j_graphrag RetrieverResultItem 흉내)."""

    def __init__(self, content: str, metadata: dict):
        self.content = content
        self.metadata = metadata


class _FakeTool:
    """검색 도구 하나. 호출 인자를 기록하고 정해진 항목을 그대로 돌려준다."""

    def __init__(self, items: list[_FakeItem]):
        self._items = items
        self.calls: list[dict] = []

    def execute(self, **kwargs):
        self.calls.append(kwargs)
        return type("_R", (), {"items": list(self._items)})()


def _chunk_meta(eid: str, chapter: int, index: int, text: str) -> dict:
    """hybrid_search가 청크를 물어왔을 때의 metadata 모양."""
    return {"eid": eid, "kind": "chunk", "chapter": chapter, "chunk_index": index, "text": text}


# ---------------------------------------------------------------------------
# 1. 줄 번호는 원고 전역에 한 번만 부여된다
# ---------------------------------------------------------------------------


def test_line_numbers_continue_across_chunks():
    """조각을 나눠도 L번호는 원고 전역으로 이어져야 한다.

    조각마다 1부터 다시 매기면 조각 2의 L5와 조각 1의 L5가 같은 이름이 된다. 그러면 합친
    뒤에는 claim이 가리키는 줄을 특정할 수 없고, 화면은 엉뚱한 문장에 하이라이트를 건다.
    예외는 나지 않는다 — 그냥 다른 문장이 빨갛게 칠해질 뿐이다.
    """
    units = _chunk(LONG_TEXT)
    assert len(units) >= 2, "이 테스트는 조각이 둘 이상이어야 의미가 있다"

    # 조각을 순서대로 이으면 L1부터 빠짐도 겹침도 없이 이어져야 한다.
    numbers = [int(_L_PREFIX.match(line).group(1)) for u in units for line in u.split("\n")]
    assert numbers == list(range(1, len(numbers) + 1))

    # 둘째 조각의 첫 줄이 L1이면 조각마다 번호를 다시 매긴 것이다.
    assert int(_L_PREFIX.match(units[1].split("\n")[0]).group(1)) > 1


def test_chunk_boundaries_never_cut_a_line_in_half():
    """경계는 줄에서만 끊는다 — 문장 중간에서 자르면 추출기가 반쪽 문장을 claim으로 뽑는다."""
    lines = split_lines(LONG_TEXT)
    numbered = number_lines(lines).split("\n")

    produced = [line for u in _chunk(LONG_TEXT) for line in u.split("\n")]
    # 조각들이 내놓은 줄이 전역 번호 매김의 줄과 **문자열 단위로 같아야** 한다.
    assert produced == numbered


def test_a_single_line_longer_than_the_chunk_still_produces_one_chunk():
    """한 줄이 조각 크기보다 길어도 빈 조각을 만들지 않는다(첫 줄은 무조건 담는다)."""
    units = _chunk("가" * (CHUNK_SIZE + 500))
    assert units and all(u.strip() for u in units)


# ---------------------------------------------------------------------------
# 2. 추출 상한은 설정값으로 계산한다(프롬프트 캐시가 깨지지 않는 근거)
# ---------------------------------------------------------------------------


def test_cap_is_computed_from_the_configured_chunk_size_not_the_actual_length():
    """상한이 조각의 **실제 길이**를 따라가면 안 된다.

    실제 길이로 계산하면 조각마다 system 프롬프트의 숫자가 달라지고, 그러면 매 호출이
    새 프리픽스가 되어 프롬프트 캐시가 통째로 무효가 된다(비용은 조용히 몇 배가 된다).
    """
    assert _cap_for(CHUNK_SIZE) == max(20, CHUNK_SIZE // 30)

    units = _chunk(LONG_TEXT)
    caps_by_actual_length = {_cap_for(len(u)) for u in units}
    # 실제 길이로 계산했다면 조각마다 값이 갈렸을 것이라는 사실 자체를 못박는다 —
    # 이게 성립해야 아래 "system이 하나뿐"이라는 테스트가 의미를 가진다.
    assert len(caps_by_actual_length) > 1


def test_extract_sends_the_same_system_prompt_to_every_chunk(monkeypatch):
    """조각이 몇 개든 system 프롬프트는 한 벌이어야 한다(캐시 프리픽스 안정성)."""
    entity_calls: list[tuple] = []

    # 등장인물 노드 조회는 Neo4j를 친다 — 여기서는 그래프 없이 빈 목록으로 대체한다.
    def _render(tenant, up_to_chapter=None):
        entity_calls.append((tenant, up_to_chapter))
        return ""

    monkeypatch.setattr(entity_nodes, "render", _render)

    seen: list[dict] = []

    async def _fake_completion(**kwargs):
        seen.append(kwargs)
        return _FakeResponse(json.dumps({"claims": []}))

    monkeypatch.setattr(extract_service, "create_completion", _fake_completion)

    asyncio.run(extract_service.extract(LONG_TEXT, TENANT, up_to_chapter=5))

    assert len(seen) >= 2  # 조각마다 한 번씩 불렀다
    systems = {call["messages"][0]["content"] for call in seen}
    assert len(systems) == 1
    # 캐시 라우팅 키도 호출마다 같아야 같은 캐시에 붙는다.
    assert {call["prompt_cache_key"] for call in seen} == {"detect-extract"}
    # 등장인물 노드는 회차 상한을 그대로 받아야 한다 — 상한이 없으면 6화 이후에 확립된
    # 상태를 "이미 알려진 설정"으로 읽고, 뒤에 나올 반전을 5화의 오류로 판정한다.
    assert entity_calls == [(TENANT, 5)]


# ---------------------------------------------------------------------------
# 3. claim 번호는 원고 등장 순서다
# ---------------------------------------------------------------------------


def test_claim_ids_follow_manuscript_order():
    """P번호는 화면 정렬 키다 — 순서가 흐트러지면 오류 목록이 원고와 다른 순서로 뜬다."""
    claims = [{"quote": f"q{i}"} for i in range(1, 6)]
    assign_claim_ids(claims)

    assert [c["id"] for c in claims] == ["P1", "P2", "P3", "P4", "P5"]
    # 번호가 붙어도 원소 순서 자체는 그대로여야 한다(제자리 수정이지 재정렬이 아니다).
    assert [c["quote"] for c in claims] == ["q1", "q2", "q3", "q4", "q5"]


def test_claim_ids_are_assigned_after_chunks_are_merged(monkeypatch):
    """조각별로 번호를 매기면 경계에서 P1이 두 번 나온다 — 합친 뒤에 한 번만 매겨야 한다."""
    monkeypatch.setattr(entity_nodes, "render", lambda tenant, up_to_chapter=None: "")

    # 조각 순서대로 서로 다른 claim을 돌려준다.
    payloads = iter(
        [
            json.dumps({"claims": [{"quote": "앞1"}, {"quote": "앞2"}]}),
            json.dumps({"claims": [{"quote": "뒤1"}]}),
        ]
    )

    async def _fake_completion(**kwargs):
        return _FakeResponse(next(payloads, json.dumps({"claims": []})))

    monkeypatch.setattr(extract_service, "create_completion", _fake_completion)

    claims, lines, _usage = asyncio.run(
        extract_service.extract(LONG_TEXT, TENANT, up_to_chapter=5)
    )

    assert [(c["id"], c["quote"]) for c in claims] == [
        ("P1", "앞1"),
        ("P2", "앞2"),
        ("P3", "뒤1"),  # 둘째 조각의 첫 claim이 다시 P1이 되면 안 된다
    ]
    # lines는 번호를 매긴 원본 줄 목록 — 호출자가 lineIds로 원문을 되짚는 통로다.
    assert lines == split_lines(LONG_TEXT)


# ---------------------------------------------------------------------------
# 4. 라우팅표 — 모델이 아니라 코드가 도구를 고른다
# ---------------------------------------------------------------------------


def test_route_qav_sends_axis_and_axis_value_to_both_channels():
    """axis와 `axis: value` 두 계열 × hybrid·fact 두 채널 = 네 번."""
    calls = route_qav({"quote": "카엘은 검을 뽑았다", "axis": "카엘의 검 상태", "value": "온전함"})

    assert calls == [
        ("hybrid_search", {"query_text": "카엘의 검 상태"}),
        ("fact_search", {"query_text": "카엘의 검 상태"}),
        ("hybrid_search", {"query_text": "카엘의 검 상태: 온전함"}),
        ("fact_search", {"query_text": "카엘의 검 상태: 온전함"}),
    ]


def test_route_qav_without_value_sends_axis_only():
    """값이 비면 `axis: value` 계열이 성립하지 않는다 — 두 번만 나간다."""
    calls = route_qav({"quote": "카엘은 검을 뽑았다", "axis": "카엘의 검 상태", "value": ""})

    assert calls == [
        ("hybrid_search", {"query_text": "카엘의 검 상태"}),
        ("fact_search", {"query_text": "카엘의 검 상태"}),
    ]


def test_route_qav_falls_back_to_quote_when_axis_is_empty():
    """axis를 못 세운 claim도 검색은 나가야 한다 — 안 그러면 근거가 0이라 무조건 무판정이다."""
    calls = route_qav({"quote": "카엘은 검을 뽑았다", "axis": "", "value": "온전함"})

    assert calls == [
        ("hybrid_search", {"query_text": "카엘은 검을 뽑았다"}),
        ("fact_search", {"query_text": "카엘은 검을 뽑았다"}),
    ]


def test_route_qav_with_quote_adds_a_third_series():
    """with_quote는 "추출기가 axis를 빗맞혔을 때"의 안전망이다 — quote 계열이 앞에 더 붙는다."""
    calls = route_qav(
        {"quote": "카엘은 검을 뽑았다", "axis": "카엘의 검 상태", "value": "온전함"},
        with_quote=True,
    )

    assert calls == [
        ("hybrid_search", {"query_text": "카엘은 검을 뽑았다"}),
        ("fact_search", {"query_text": "카엘은 검을 뽑았다"}),
        ("hybrid_search", {"query_text": "카엘의 검 상태"}),
        ("fact_search", {"query_text": "카엘의 검 상태"}),
        ("hybrid_search", {"query_text": "카엘의 검 상태: 온전함"}),
        ("fact_search", {"query_text": "카엘의 검 상태: 온전함"}),
    ]


def test_route_qav_returns_nothing_when_the_claim_is_empty():
    """axis도 quote도 없으면 던질 질의가 없다 — 빈 질의로 검색을 때리지 않는다."""
    assert routing.route_qav({}) == []


# ---------------------------------------------------------------------------
# 5. 회차 상한 — 미래를 근거로 쓰면 검사가 무의미해진다
# ---------------------------------------------------------------------------


def test_future_evidence_is_excluded_but_chapterless_nodes_survive():
    """5화를 검사하면 5화 이상은 근거에서 빠지고, 회차를 모르는 노드는 남는다.

    같은 회차(5화)까지 빼는 이유: 검사 대상 회차 자신이 만든 사실을 근거로 쓰면 원고가
    자기 자신과 일치한다고 자평한다.

    회차 없는 노드를 **남기는** 이유: 인물처럼 회차마다 같은 노드로 MERGE되는 정준
    엔티티에는 회차 표시가 없다. 모른다고 버리면 배경 설정이 통째로 사라지고, 그러면
    대조할 것이 없어 모든 claim이 조용히 "근거 없음"이 된다.
    """
    assert _is_future({"chapter": 6}, 5) is True  # 뒤에 나올 반전
    assert _is_future({"chapter": 5}, 5) is True  # 검사 대상 회차 자신
    assert _is_future({"chapter": 4}, 5) is False  # 과거 = 유효한 근거
    assert _is_future({}, 5) is False  # 회차를 모르는 노드 — 버리지 않는다
    assert _is_future({"chapter": None}, 5) is False
    assert _is_future({"chapter": "5화"}, 5) is False  # 정수가 아니면 "모른다"로 다룬다
    assert _is_future({"chapter": 6}, None) is False  # 상한이 없으면 아무것도 안 뺀다


def test_norm_ignores_whitespace_differences():
    """dedupe 키. 렌더링 차이(줄바꿈·들여쓰기)만으로 같은 원문을 두 번 싣지 않게 한다."""
    assert _norm("카엘의 검은  부러졌다.\n") == _norm("카엘의\n검은 부러졌다.")
    assert _norm(None) == ""


# ---------------------------------------------------------------------------
# 6. dedupe는 content에만 걸린다
# ---------------------------------------------------------------------------


def test_dedupe_drops_repeated_text_but_keeps_the_reference():
    """중복 제거는 **텍스트**에만 건다 — 참조(items)는 채널마다 남아야 한다.

    문서고는 items로 "이 claim이 어느 노드를 봤는가"를 복원한다. dedupe가 items까지
    지우면 두 번째 채널이 물어온 노드는 claim과의 연결이 끊기고, 판정기는 그 근거를
    가리키는 별칭을 받지 못한다 — 근거는 문서고에 실려 있는데 아무도 못 보는 상태가 된다.
    """
    same = _chunk_meta("e-chunk-1", 3, 1, "카엘의 검은 3화에서 부러졌다.")
    future = _chunk_meta("e-chunk-9", 9, 0, "카엘은 새 검을 얻는다.")

    # 두 도구가 같은 원문을 (공백만 다르게) 물어온다. 미래 회차 항목도 한 건씩 섞는다.
    hybrid = _FakeTool([_FakeItem("카엘의 검은 3화에서 부러졌다.", same), _FakeItem("미래", future)])
    fact = _FakeTool([_FakeItem("카엘의 검은\n3화에서 부러졌다.\n", same)])
    tools = {"hybrid_search": hybrid, "fact_search": fact}

    claim = {"id": "P1", "quote": "카엘은 검을 뽑았다", "axis": "카엘의 검 상태", "value": "온전함"}
    record = _retrieve_one(claim, tools, up_to_chapter=5)

    channels = record["channels"]
    assert len(channels) == 4  # route_qav의 네 채널
    assert record["claim"] is claim

    # 첫 채널만 원문을 싣는다.
    assert "[결과 1]\n카엘의 검은 3화에서 부러졌다." in channels[0]["content"]
    # 나머지 세 채널은 텍스트가 통째로 빠진다(공백만 다른 두 번째도 같은 것으로 본다).
    for ch in channels[1:]:
        assert ch["content"] == "(모든 결과가 이 claim의 앞 채널과 중복이라 생략)"
    # 그러나 참조는 모든 채널에 남는다 — 여기가 claim↔노드 연결이 복원되는 통로다.
    for ch in channels:
        assert [item["metadata"]["eid"] for item in ch["items"]] == ["e-chunk-1"]

    # 미래 회차(9화)는 content에도 items에도 없다. 검사 회차 상한이 items까지 걸린다.
    assert all("e-chunk-9" not in json.dumps(ch["items"]) for ch in channels)
    assert all("미래" not in ch["content"] for ch in channels)

    # 채널마다 top_k가 붙어 나간다.
    assert all(call["top_k"] == TOP_K for call in hybrid.calls + fact.calls)


def test_a_failing_channel_is_recorded_as_no_evidence():
    """검색 하나가 터져도 검사는 계속된다 — 한 채널 실패로 회차 전체가 ERROR가 되면 안 된다."""

    class _Broken:
        def execute(self, **kwargs):
            raise RuntimeError("Neo4j 연결 끊김")

    tools = {"hybrid_search": _Broken(), "fact_search": _Broken()}
    record = _retrieve_one({"axis": "카엘의 검 상태"}, tools, up_to_chapter=5)

    for ch in record["channels"]:
        assert "도구 실행 오류" in ch["content"]
        assert ch["items"] == []


# ---------------------------------------------------------------------------
# 7. parse_verdicts 폴백 — 위반은 버리지 않고 flag로 남긴다
# ---------------------------------------------------------------------------


def _verdict_json(*verdicts: dict) -> str:
    return json.dumps({"verdicts": list(verdicts)})


def test_unknown_claim_id_falls_back_to_the_numeric_index():
    """모델이 P번호를 엉뚱하게 쓰면 옛 형식(`i`)으로 떨어지되 그 사실을 flag로 남긴다."""
    claims = [{"id": "P1", "quote": "q1", "lines": [3, 4]}]
    raw = _verdict_json(
        {"claimId": "Q7", "i": 1, "score": 8, "lineIds": [3], "cited": ["C001"], "reason": "r"}
    )

    out, problems = parse_verdicts(raw, [0], claims)

    assert list(out) == [0]
    assert "claim_id_unknown" in out[0]["flags"]
    assert out[0]["score"] == 8  # 판정 자체는 버리지 않는다
    assert problems == []


def test_line_ids_outside_the_claim_candidates_fall_back_to_all_candidates():
    """판정기는 원고를 못 본다 — 없는 줄 번호를 지어내면 claim의 후보 줄 전체로 되돌린다.

    되돌리지 않으면 화면이 원고에 없는 줄을 하이라이트하거나 아무 데도 못 칠한다.
    """
    claims = [
        {"id": "P1", "quote": "q1", "lines": [3, 4]},
        {"id": "P2", "quote": "q2", "lines": [10]},
        {"id": "P3", "quote": "q3", "lines": [20, 21]},
    ]
    raw = _verdict_json(
        {"claimId": "P1", "score": 8, "lineIds": [99], "reason": "r"},        # 후보 밖
        {"claimId": "P2", "score": 8, "lineIds": [], "reason": "r"},          # 아예 안 골랐다
        {"claimId": "P3", "score": 8, "lineIds": [21], "reason": "r"},        # 후보의 부분집합
    )

    out, _problems = parse_verdicts(raw, [0, 1, 2], claims)

    assert out[0]["line_ids"] == [3, 4] and "line_fallback" in out[0]["flags"]
    assert out[1]["line_ids"] == [10] and "line_fallback" in out[1]["flags"]
    assert out[2]["line_ids"] == [21] and "line_fallback" not in out[2]["flags"]


def test_scores_outside_the_range_are_clamped():
    """0~10 밖의 점수는 잘라 담는다 — 12점짜리가 그대로 흐르면 임계값 비교가 무의미해진다."""
    claims = [{"id": "P1", "quote": "q1", "lines": [1]}, {"id": "P2", "quote": "q2", "lines": [2]}]
    raw = _verdict_json(
        {"claimId": "P1", "score": 12, "lineIds": [1], "reason": "r"},
        {"claimId": "P2", "score": -3, "lineIds": [2], "reason": "r"},
    )

    out, _problems = parse_verdicts(raw, [0, 1], claims)

    assert out[0]["score"] == 10 and "score_out_of_range" in out[0]["flags"]
    assert out[1]["score"] == 0 and "score_out_of_range" in out[1]["flags"]
    # 원본은 그대로 남긴다 — 모델이 얼마나 벗어났는지가 다음 개선의 신호다.
    assert out[0]["raw_score"] == 12


def test_missing_claims_are_not_filled_with_zero():
    """누락은 0점이 아니다.

    0은 "근거를 봤지만 모순이 아니다"라는 **유효한 판정**이다. 누락을 0으로 메우면 둘이
    영영 구분되지 않아, 판정기가 claim 절반을 통째로 빠뜨려도 "전부 정상"으로 보인다.
    """
    claims = [
        {"id": "P1", "quote": "q1", "lines": [1]},
        {"id": "P2", "quote": "q2", "lines": [2]},
        {"id": "P3", "quote": "q3", "lines": [3]},
    ]
    raw = _verdict_json(
        {"claimId": "P1", "score": 0, "lineIds": [1], "reason": "모순 아님"},  # 진짜 0점
        {"claimId": "P3", "score": 9, "lineIds": [3], "reason": "모순"},
    )

    out, problems = parse_verdicts(raw, [0, 1, 2], claims)

    assert 1 not in out  # P2는 자리 자체가 없다 — 0으로 채우지 않는다
    assert out[0]["score"] == 0 and out[0]["flags"] == []  # 0점은 정상 판정이다
    assert "missing:1" in problems


def test_broken_json_is_reported_instead_of_raising():
    """판정 응답이 JSON이 아니면 회차 전체가 터지는 대신 문제로 기록된다."""
    out, problems = parse_verdicts("이건 JSON이 아니다", [0, 1], [])

    assert out == {}
    assert problems == ["json_parse_failed"]


# ---------------------------------------------------------------------------
# 8. cited 해소 — 별칭을 화면이 열 수 있는 좌표로 바꾼다
# ---------------------------------------------------------------------------


def _store(chunks: dict, facts: dict) -> dict:
    """`build_docstore`가 만드는 문서고의 모양(필요한 부분만)."""
    return {"chunks": chunks, "facts": facts, "entities": {}, "refs": []}


def test_cited_chunk_alias_becomes_a_natural_key():
    """C### 별칭은 (회차, 조각 번호)로 펼쳐진다 — 화면이 그 조각을 바로 연다."""
    store = _store(
        {
            "e-c1": {"eid": "e-c1", "alias": "C001", "chapter": 3, "index": 1, "text": "..."},
            "e-c2": {"eid": "e-c2", "alias": "C002", "chapter": 4, "index": 0, "text": "..."},
        },
        {},
    )

    assert _resolve_cited(["C002", "C001"], store) == [
        {"episodeNo": 4, "chunkIndex": 0},
        {"episodeNo": 3, "chunkIndex": 1},  # 인용 순서를 그대로 지킨다
    ]


def test_cited_fact_alias_expands_to_its_evidence_chunks():
    """F### 별칭은 그 사실의 **근거 청크들**로 펼쳐진다.

    화면이 하이라이트할 수 있는 것은 원문 조각이지 사실 노드가 아니다. 사실을 그대로
    돌려주면 작가는 "근거가 있다"는 말만 보고 그 근거를 볼 수 없다.

    문서고에서 사실의 evidence는 **청크 eid 목록**이다(build_docstore가 근거 원문을 청크
    사전으로 보내고 키만 남긴다). 여기서 청크를 되짚어야 좌표가 나온다.
    """
    store = _store(
        {
            "e-c1": {"eid": "e-c1", "alias": "C001", "chapter": 2, "index": 0, "text": "..."},
            "e-c2": {"eid": "e-c2", "alias": "C002", "chapter": 3, "index": 1, "text": "..."},
        },
        {
            "e-f1": {
                "eid": "e-f1",
                "alias": "F001",
                "name": "카엘의 검이 부러짐",
                "chapter": 3,
                "story_order": 1,
                "evidence": ["e-c1", "e-c2"],
            }
        },
    )

    assert _resolve_cited(["F001"], store) == [
        {"episodeNo": 2, "chunkIndex": 0},
        {"episodeNo": 3, "chunkIndex": 1},
    ]


def test_hallucinated_aliases_are_dropped_and_duplicates_collapse():
    """문서고에 없는 별칭은 버린다 — 지어낸 인용을 내보내면 화면이 없는 자리를 가리킨다."""
    store = _store(
        {"e-c1": {"eid": "e-c1", "alias": "C001", "chapter": 3, "index": 1, "text": "..."}},
        {
            "e-f1": {
                "eid": "e-f1",
                "alias": "F001",
                "name": "f",
                "chapter": 3,
                "story_order": 1,
                "evidence": ["e-c1"],  # 같은 청크를 가리킨다
            }
        },
    )

    # C999는 문서고에 없고, F001은 C001과 같은 좌표로 풀린다(중복은 한 번만 남는다).
    assert _resolve_cited(["C001", "F001", "C999"], store) == [{"episodeNo": 3, "chunkIndex": 1}]
    assert _resolve_cited(["C999", "F999"], store) == []


# ---------------------------------------------------------------------------
# 9. judge — τ 컷과 findings 조립
# ---------------------------------------------------------------------------


def _evidence_for(claims: list[dict], items_per_claim: list[list[dict]]) -> dict:
    """`retrieve()`가 돌려주는 것과 **같은 모양**의 evidence를 만든다.

    이 모양이 곧 retrieve→judge 사이의 계약이다. 여기가 어긋나면 파이프라인은 마지막
    단계에서 통째로 터진다.
    """
    return {
        "records": [
            {
                "claim": claim,
                "channels": [
                    {"tool": "hybrid_search", "args": {}, "content": "(생략)", "items": items}
                ],
            }
            for claim, items in zip(claims, items_per_claim)
        ]
    }


def test_judge_returns_only_findings_at_or_above_the_threshold(monkeypatch):
    """τ 컷. 문턱 미만은 findings에 오지 않는다 — 응답은 오류 목록이지 판정 전량이 아니다."""
    claims = [
        {"quote": "카엘은 검을 뽑았다.", "axis": "카엘의 검 상태", "value": "온전함", "lines": [12]},
        {"quote": "레아는 북부 출신이다.", "axis": "레아의 출신", "value": "북부", "lines": [40]},
        {"quote": "성문은 남쪽이다.", "axis": "성문의 방향", "value": "남쪽", "lines": [55]},
    ]
    assign_claim_ids(claims)
    evidence = _evidence_for(
        claims,
        [
            [{"metadata": _chunk_meta("e-c1", 3, 1, "카엘의 검은 부러졌다.")}],
            [],
            [],
        ],
    )

    raw = _verdict_json(
        {"claimId": "P1", "score": ERROR_THRESHOLD, "lineIds": [12], "cited": ["C001"], "reason": "부러진 검"},
        {"claimId": "P2", "score": ERROR_THRESHOLD - 1, "lineIds": [40], "cited": [], "reason": "애매"},
        {"claimId": "P3", "score": 0, "lineIds": [55], "cited": [], "reason": "일치"},
    )

    seen: list[dict] = []

    async def _fake_completion(**kwargs):
        seen.append(kwargs)
        return _FakeResponse(raw)

    monkeypatch.setattr(judge_service, "create_completion", _fake_completion)

    findings = asyncio.run(judge_service.judge(claims, evidence))

    # 문턱 이상만 남는다. 문턱 **이상**(>=)이라 딱 7점인 P1은 포함된다.
    assert [f["claimId"] for f in findings] == ["P1"]
    assert findings[0]["quote"] == claims[0]["quote"]
    assert findings[0]["axis"] == claims[0]["axis"]
    assert findings[0]["value"] == claims[0]["value"]
    assert findings[0]["lineIds"] == [12]
    assert findings[0]["isError"] is True
    assert findings[0]["cited"] == [{"episodeNo": 3, "chunkIndex": 1}]

    # 판정은 claim마다 부르지 않고 **한 번의 배치**로 끝난다(문서고를 한 번만 싣는 근거).
    assert len(seen) == 1
    assert seen[0]["prompt_cache_key"] == "detect-judge"
    assert seen[0]["response_format"] == {"type": "json_object"}


def test_judge_without_claims_calls_no_llm(monkeypatch):
    """추출이 0건이면 판정할 것이 없다 — 빈 문서고로 LLM을 부르는 낭비를 하지 않는다."""

    async def _boom(**kwargs):
        raise AssertionError("claim이 없는데 LLM을 불렀다")

    monkeypatch.setattr(judge_service, "create_completion", _boom)

    assert asyncio.run(judge_service.judge([], {"records": []})) == []


def test_retrieve_without_claims_touches_no_graph():
    """같은 이유로 검색도 건너뛴다 — 도구를 조립하는 것만으로 Neo4j 드라이버가 열린다."""
    assert asyncio.run(retrieve_service.retrieve([], TENANT, up_to_chapter=5)) == {"records": []}
