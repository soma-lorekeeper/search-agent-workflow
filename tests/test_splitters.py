"""문장 청킹의 계약을 고정한다.

여기서 만든 Chunk가 검색의 앵커이자 판정기에게 보여줄 근거 원문이고, 화면이 본문 위에
하이라이트를 거는 좌표이기도 하다. 그래서 **청크는 원문을 그대로 잘라낸 부분문자열**이어야
한다 — 문장을 재조립하면 그 사이의 줄바꿈이 무엇이었는지 알 수 없어 임의의 구분자로 메우게
되고, 그 순간 청크는 원문 어디에도 없는 문자열이 된다.

깨져도 예외가 아니라 "원문에서 그 자리를 못 찾음"으로 나타나는 종류라 테스트로 붙들어 둔다.
"""

from __future__ import annotations

from src.service.index.splitters import (
    CHUNK_OVERLAP,
    CHUNK_SIZE,
    _group_sentences,
    _sentence_spans,
)

# 줄바꿈·빈 줄·마침표 없는 블록이 섞인 원고. 실제 웹소설의 시스템 메시지 구간을 본떴다.
_TEXT = """김독자는 지하철에 있었다. 도깨비가 나타났다.

[체력 Lv.1 -> 체력 Lv.10]
[추가 보상 100코인을 획득합니다.]
유중혁이 회귀를 반복했다. 그는 3회차였다."""

_SENTENCES = [
    "김독자는 지하철에 있었다.",
    "도깨비가 나타났다.",
    "[체력 Lv.1 -> 체력 Lv.10]",
    "[추가 보상 100코인을 획득합니다.]",
    "유중혁이 회귀를 반복했다.",
    "그는 3회차였다.",
]


def test_모든_청크가_원문의_부분문자열이다():
    chunks = _group_sentences(_TEXT, _SENTENCES, chunk_size=40, overlap=0)
    assert chunks, "청크가 하나도 안 나왔다"
    for c in chunks:
        assert c in _TEXT, f"원문에 없는 청크: {c!r}"


def test_문장_사이의_줄바꿈이_보존된다():
    """공백으로 이어 붙이면 원문의 개행 구조가 사라진다 — 시스템 메시지 블록이 한 줄로 뭉개진다."""
    chunks = _group_sentences(_TEXT, _SENTENCES, chunk_size=40, overlap=0)
    assert any("\n" in c for c in chunks), "줄바꿈이 하나도 안 남았다"


def test_긴_문장은_잘라도_부분문자열이다():
    """마침표 없이 이어지는 스탯창 블록은 문장 하나가 목표 크기를 훌쩍 넘는다.

    쪼개지 않으면 임베딩 최대 토큰(8192)을 넘겨 인덱싱이 실패한다. 문장 경계 정보가
    없으므로 글자 수로 자르되, 구간을 자르는 것이라 조각들도 여전히 부분문자열이다.
    """
    long_sent = "스탯창 " + "가나다라마바사아자차 " * 12
    text = f"앞 문장이다.\n{long_sent}\n뒤 문장이다."
    sentences = ["앞 문장이다.", long_sent.strip(), "뒤 문장이다."]

    chunks = _group_sentences(text, sentences, chunk_size=50, overlap=0)
    assert all(c in text for c in chunks)
    assert max(len(c) for c in chunks) <= 50


def test_겹침이_커도_청크가_목표를_넘지_않는다():
    """overlap이 chunk_size에 근접하면 넘길 것만으로 예산이 차, 순진하게 구현하면
    청크가 줄기는커녕 커진다. 그러면 임베딩 한도를 지키려던 목적이 무너진다."""
    text = "첫째 문장.\n둘째 문장.\n셋째 문장.\n넷째 문장.\n다섯째 문장."
    sentences = ["첫째 문장.", "둘째 문장.", "셋째 문장.", "넷째 문장.", "다섯째 문장."]

    for overlap in (0, 5, 10, 22, 100):
        chunks = _group_sentences(text, sentences, chunk_size=22, overlap=overlap)
        assert all(c in text for c in chunks), overlap
        assert max(len(c) for c in chunks) <= 22, f"overlap={overlap}에서 목표 초과"


def test_분리기가_원문에_없는_문장을_주면_버린다():
    """공백 정규화 같은 이유로 원문과 안 맞는 문장은 건너뛴다 —
    원문에 없는 문자열을 청크로 만드느니 빠지는 편이 낫다."""
    spans = _sentence_spans("가나다. 라마바.", ["가나다.", "원문에 없는 문장.", "라마바."])
    assert len(spans) == 2


def test_실제_설정값():
    """호출부가 인자를 넘기지 않고 이 기본값을 쓴다(indexing_service)."""
    assert CHUNK_SIZE == 100
    assert CHUNK_OVERLAP == 0


def test_원문이_통째로_담긴다():
    """겹침이 없을 때 청크들은 원문을 빠짐없이 나눠 가진다.

    청크 경계에서 사라지는 것은 문장 사이의 공백뿐이어야 한다 — 문장이 통째로 빠지면
    그 부분은 검색에도 근거에도 영영 등장하지 않는데, 예외 없이 조용히 없어진다.
    """
    import re

    chunks = _group_sentences(_TEXT, _SENTENCES, chunk_size=40, overlap=0)
    squash = lambda s: re.sub(r"\s+", "", s)  # noqa: E731
    assert squash("".join(chunks)) == squash(_TEXT)
