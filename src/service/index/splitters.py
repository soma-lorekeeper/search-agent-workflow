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
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 100


def _split_oversized_sentence(sent: str, chunk_size: int) -> list[str]:
    """chunk_size보다 긴 단일 문장을 글자 수 기준으로 강제 분할한다.

    KSS가 스탯창/시스템 메시지 블록(마침표 없이 이어지는 LitRPG 서술)을 문장 하나로
    인식하면 그 문장 하나가 chunk_size를 훨씬 넘을 수 있다. 이 경우 문장 경계 정보가
    없으므로 overlap 없이 단순 슬라이싱해 임베딩 최대 토큰 초과를 막는다.
    """
    return [sent[i : i + chunk_size] for i in range(0, len(sent), chunk_size)]


def _group_sentences(
    sentences: list[str], chunk_size: int, overlap: int
) -> list[str]:
    """
    문장 리스트를 목표 글자 수(chunk_size)까지 이어 붙여 청크 문자열 목록으로 만든다.
    다음 청크는 직전 청크의 끝 문장들(합계 약 overlap 글자)을 다시 포함해 문맥을 잇는다.
    문장 경계로만 자르되, 문장 하나가 chunk_size를 넘으면 글자 수 기준으로 강제 분할한다.
    """
    chunks: list[str] = []
    current: list[str] = []          # 현재 청크에 담긴 문장들
    current_len = 0                  # 현재 청크의 누적 글자 수

    for sent in sentences:
        sent = sent.strip()
        if not sent:
            continue
        # 문장 하나가 이미 목표 크기를 넘으면 글자 수 기준으로 강제 분할한다.
        # (분할 안 하면 임베딩 최대 토큰(8192)을 초과해 인덱싱이 실패한다.)
        parts = (
            _split_oversized_sentence(sent, chunk_size)
            if len(sent) > chunk_size
            else [sent]
        )
        for part in parts:
            # 현재 청크에 이 조각을 더하면 목표를 넘고, 이미 담긴 게 있으면 청크를 확정한다.
            if current and current_len + len(part) > chunk_size:
                chunks.append(" ".join(current))
                # 겹침: 끝에서부터 되짚어 약 overlap 글자만큼 다음 청크의 시작으로 넘긴다.
                carried: list[str] = []
                carried_len = 0
                for prev in reversed(current):
                    if carried_len + len(prev) > overlap:
                        break
                    carried.insert(0, prev)
                    carried_len += len(prev)
                current = carried
                current_len = carried_len
            current.append(part)
            current_len += len(part)

    if current:
        chunks.append(" ".join(current))
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
        pieces = _group_sentences(sentences, self.chunk_size, self.chunk_overlap)
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
        pieces = _group_sentences(sentences, self.chunk_size, self.chunk_overlap)
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
