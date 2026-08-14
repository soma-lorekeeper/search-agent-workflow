r"""
TextSplitter 후보 모음.

neo4j-graphrag의 `TextSplitter` 인터페이스(`async def run(self, text) -> TextChunks`)를
구현하는 한국어용 커스텀 스플리터를 정의한다.

- `KiwiSentenceSplitter` : kiwipiepy 형태소 기반 문장 분리
- `KSSSentenceSplitter`   : KSS(Korean Sentence Splitter) 문장 분리
- `WholeTextSplitter`     : 원고 전체를 자르지 않고 1개 청크로 내보냄(회차=단일 추출 청크)
"""

from __future__ import annotations

from neo4j_graphrag.experimental.components.text_splitters.base import TextSplitter
from neo4j_graphrag.experimental.components.types import TextChunk, TextChunks

# 문장 분리 splitter가 공유하는 목표 청크 크기(글자 수)와 겹침.
#
# 100자면 청크당 평균 약 3문장이라 근거를 문장 단위로 정밀하게 짚을 수 있다. 이 청크가
# 검색의 앵커이자 판정기에게 보여줄 근거 원문이라, 크면 "어느 문장이 근거인지"가 흐려진다.
#
# 겹침은 0이다. 겹치면 경계 문장이 인접한 두 [C{i}] 마커에 중복 노출돼, 추출기가
# evidence_chunk 번호를 고를 때 어느 쪽을 써야 할지 모호해진다.
CHUNK_SIZE = 100
CHUNK_OVERLAP = 0


def _sentence_spans(text: str, sentences: list[str]) -> list[tuple[int, int]]:
    """문장 분리기가 돌려준 문장들이 원문에서 차지하는 구간 [start, end)를 찾는다.

    문자열이 아니라 구간으로 다루는 이유는 **청크가 원문의 부분문자열이어야** 하기
    때문이다. 문장을 다시 이어 붙이면 그 사이에 있던 줄바꿈·들여쓰기가 무엇이었는지
    알 수 없어 임의의 구분자로 메우게 되고, 그 순간 청크는 원문 어디에도 없는 문자열이
    된다 — 근거를 원문과 대조하거나 화면에서 그 자리를 하이라이트할 수 없어진다.

    앞에서부터 순서대로 찾는다(cursor). 같은 문장이 원문에 여러 번 나와도 등장 순서대로
    짝지어진다.
    """
    spans: list[tuple[int, int]] = []
    cursor = 0
    for sentence in sentences:
        stripped = sentence.strip()
        if not stripped:
            continue
        start = text.find(stripped, cursor)
        if start < 0:
            # 분리기가 문장을 원문 그대로 돌려주지 않은 경우(공백 정규화 등).
            # 그 문장은 건너뛴다 — 원문에 없는 문자열을 청크로 만드느니 빠지는 편이 낫다.
            continue
        end = start + len(stripped)
        spans.append((start, end))
        cursor = end
    return spans


def _split_oversized_span(span: tuple[int, int], chunk_size: int) -> list[tuple[int, int]]:
    """chunk_size보다 긴 문장 구간을 글자 수 기준으로 쪼갠다.

    KSS가 스탯창/시스템 메시지 블록(마침표 없이 이어지는 LitRPG 서술)을 문장 하나로
    인식하면 그 문장 하나가 chunk_size를 훨씬 넘을 수 있다. 문장 경계 정보가 없으므로
    단순히 글자 수로 자른다 — 임베딩 최대 토큰 초과를 막기 위한 최후 수단이다.

    구간을 자르므로 조각들은 여전히 원문의 부분문자열이다.
    """
    start, end = span
    return [(i, min(i + chunk_size, end)) for i in range(start, end, chunk_size)]


def _group_sentences(
    text: str, sentences: list[str], chunk_size: int, overlap: int
) -> list[str]:
    """
    문장들을 목표 글자 수(chunk_size)까지 묶어 청크 문자열 목록으로 만든다.

    각 청크는 **원문을 그대로 잘라낸 부분문자열**이다(`text[시작:끝]`). 문장 사이의
    줄바꿈·공백도 원문 그대로 남는다 — 문장을 재조립하지 않고 구간의 양끝만 쓰기 때문이다.

    다음 청크는 직전 청크의 끝 문장들(합계 약 overlap 글자)을 다시 포함해 문맥을 잇는다.
    문장 경계로만 자르되, 문장 하나가 chunk_size를 넘으면 글자 수 기준으로 강제 분할한다.
    """
    parts: list[tuple[int, int]] = []
    for span in _sentence_spans(text, sentences):
        if span[1] - span[0] > chunk_size:
            parts.extend(_split_oversized_span(span, chunk_size))
        else:
            parts.append(span)

    chunks: list[str] = []
    current: list[tuple[int, int]] = []  # 현재 청크에 담긴 구간들

    def _emit() -> None:
        """현재 구간들을 원문에서 통째로 잘라 청크로 확정한다."""
        if current:
            chunks.append(text[current[0][0] : current[-1][1]])

    for part in parts:
        # 이 조각을 더하면 목표를 넘고 이미 담긴 게 있으면 청크를 확정한다.
        # 길이는 구간의 실제 폭으로 잰다 — 사이에 낀 줄바꿈까지 청크에 들어가므로,
        # 문장 길이만 더하면 실제 청크가 목표보다 커진다.
        if current and part[1] - current[0][0] > chunk_size:
            _emit()
            # 겹침: 끝에서부터 되짚어 약 overlap 글자만큼 다음 청크의 시작으로 넘긴다.
            carried: list[tuple[int, int]] = []
            carried_len = 0
            for prev in reversed(current):
                if carried_len + (prev[1] - prev[0]) > overlap:
                    break
                carried.insert(0, prev)
                carried_len += prev[1] - prev[0]
            # 넘길 것과 이번 조각을 합쳐도 목표를 넘으면 겹침을 포기한다. 안 그러면 청크가
            # 줄지 않고 오히려 커져(overlap이 chunk_size에 가까울 때) 목표 크기가 무의미해진다
            # — 이 크기는 임베딩 최대 토큰을 넘지 않으려고 두는 값이라 조용히 넘기면 안 된다.
            if carried and part[1] - carried[0][0] > chunk_size:
                carried = []
            current = carried
        current.append(part)

    _emit()
    return chunks


class KiwiSentenceSplitter(TextSplitter):
    """kiwipiepy 형태소 분석기의 문장 분리로 청크를 만드는 스플리터."""

    def __init__(self, chunk_size: int = CHUNK_SIZE, chunk_overlap: int = CHUNK_OVERLAP):
        # 지연 import: kiwipiepy 미설치 환경에서도 이 모듈 자체는 로드되게 한다.
        from kiwipiepy import Kiwi

        self.kiwi = Kiwi()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    async def run(self, text: str) -> TextChunks:
        # split_into_sents는 Sentence 객체 리스트를 반환하므로 .text로 문자열만 추출한다.
        sentences = [s.text for s in self.kiwi.split_into_sents(text)]
        pieces = _group_sentences(text, sentences, self.chunk_size, self.chunk_overlap)
        return TextChunks(
            chunks=[TextChunk(text=p, index=i) for i, p in enumerate(pieces)]
        )


class KSSSentenceSplitter(TextSplitter):
    """KSS(Korean Sentence Splitter)의 문장 분리로 청크를 만드는 스플리터."""

    def __init__(self, chunk_size: int = CHUNK_SIZE, chunk_overlap: int = CHUNK_OVERLAP):
        # kss는 import 비용이 크므로 인스턴스 생성 시점에 지연 import한다.
        import kss

        self._split = kss.split_sentences
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    async def run(self, text: str) -> TextChunks:
        # backend='mecab' 명시: 기본값 'auto'는 mecab 미설치 시 순수 파이썬 pecab로
        # 조용히 폴백해 ~수백배 느려진다. python-mecab-ko를 의존성에 고정해 두었으므로
        # mecab을 강제한다(설치 안 됐으면 폴백 대신 에러로 드러나게).
        sentences = list(self._split(text, backend="mecab"))
        pieces = _group_sentences(text, sentences, self.chunk_size, self.chunk_overlap)
        return TextChunks(
            chunks=[TextChunk(text=p, index=i) for i, p in enumerate(pieces)]
        )


class WholeTextSplitter(TextSplitter):
    """원고 전체를 자르지 않고 1개 청크로 내보내는 splitter.

    회차 1개 = 추출 청크 1개 전제(회차 내 coreference를 한 컨텍스트에서 해소)에서 쓴다.
    [chapter:N]·[C{i}] 마커가 인라인으로 박힌 회차 원고를 그대로 한 청크로 넘겨,
    회차 크기와 무관하게 하위 분할 없이 통째로 추출되게 한다.
    """

    async def run(self, text: str) -> TextChunks:
        return TextChunks(chunks=[TextChunk(text=text, index=0)])
