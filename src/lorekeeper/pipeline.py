"""
커스텀 인덱싱 파이프라인 조립 (방법 A — SchemaBuilder를 DAG 컴포넌트로 포함).

SimpleKGPipeline 대신 Pipeline을 직접 조립하는 이유:
- SimpleKGPipeline은 extractor에 few-shot `examples`를 넘길 경로가 없다.
- resolver가 SinglePropertyExactMatchResolver로 하드코딩돼 있어 커스텀 resolver로 스왑 불가.

DAG:
    splitter → embedder → extractor → pruner → writer → (순서만) → resolver
    schema(SchemaBuilder) → extractor, schema → pruner
resolver는 데이터 입력이 없으므로 writer→resolver를 빈 input_config로 연결해 실행 순서만 강제한다.
"""

from __future__ import annotations

import os

import neo4j

from neo4j_graphrag.embeddings import OpenAIEmbeddings
from neo4j_graphrag.experimental.components.embedder import TextChunkEmbedder
from neo4j_graphrag.experimental.components.entity_relation_extractor import (
    OnError,
)
from neo4j_graphrag.experimental.components.graph_pruning import GraphPruning
from neo4j_graphrag.experimental.components.kg_writer import Neo4jWriter
from neo4j_graphrag.experimental.components.resolver import EntityResolver
from neo4j_graphrag.experimental.components.schema import SchemaBuilder
from neo4j_graphrag.experimental.components.text_splitters.base import TextSplitter
from neo4j_graphrag.experimental.pipeline import Pipeline
from neo4j_graphrag.llm import OpenAILLM

from .extractor import KoreanWebNovelERTemplate, NovelContextExtractor

# 추출(스키마·그래프)에 쓸 OpenAI 모델. 기본값 gpt-5.6-luna. GPT-5 계열이므로 temperature/top_p 등
# 샘플링 파라미터는 전달하지 않고 모델 기본값을 쓴다(비기본 temperature는 400으로 거부됨).
# 필요 시 LOREKEEPER_MODEL 환경변수로 덮어쓸 수 있다.
EXTRACTION_MODEL = os.environ.get("LOREKEEPER_MODEL") or "gpt-5.6-luna"
# 청크 임베딩 모델. retrieval을 아직 안 쓰므로 저비용 모델로 통일.
EMBEDDING_MODEL = "text-embedding-3-small"
# 반복 전달되는 프리픽스(스키마+few-shot)의 프롬프트 캐시 라우팅 안정화용 키.
PROMPT_CACHE_KEY = "lorekeeper-extract"


class TokenCountingLLM(OpenAILLM):
    """
    ainvoke 호출마다 응답 usage를 누적해 변형별 총 토큰 사용량을 집계하는 OpenAILLM.

    extractor는 청크마다 llm.ainvoke를 호출하지만 응답의 usage를 버린다. 여기서 usage를
    가로채 누적한다. build_llm이 변형마다 새 인스턴스를 만들므로 카운터는 0에서 시작한다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.total_request_tokens = 0   # 입력(프롬프트) 토큰 누적
        self.total_response_tokens = 0  # 출력(완성) 토큰 누적
        self.total_tokens = 0           # 합계 누적
        self.call_count = 0             # ainvoke 호출 횟수(= 추출 LLM 호출 수)

    async def ainvoke(self, *args, **kwargs):
        resp = await super().ainvoke(*args, **kwargs)
        self.call_count += 1
        # usage는 Optional. 모델이 usage를 안 주면 건너뛴다.
        if resp.usage:
            self.total_request_tokens += resp.usage.request_tokens or 0
            self.total_response_tokens += resp.usage.response_tokens or 0
            self.total_tokens += resp.usage.total_tokens or 0
        return resp


def build_llm(reasoning_effort: str | None = None) -> TokenCountingLLM:
    """추출용 OpenAI LLM 인스턴스. prompt_cache_key를 model_params로 넣어 모든 호출 경로에 전달한다.

    reasoning_effort를 주면 model_params에 함께 실어 전달한다(gpt-5 계열 추론 강도 조절).
    """
    params = {"prompt_cache_key": PROMPT_CACHE_KEY}
    if reasoning_effort:
        params["reasoning_effort"] = reasoning_effort
    return TokenCountingLLM(
        model_name=EXTRACTION_MODEL,
        model_params=params,
    )


def build_embedder() -> TextChunkEmbedder:
    """청크 임베딩 컴포넌트."""
    return TextChunkEmbedder(embedder=OpenAIEmbeddings(model=EMBEDDING_MODEL))


def build_pipeline(
    splitter: TextSplitter,
    resolver: EntityResolver,
    driver: neo4j.Driver,
    database: str,
    reasoning_effort: str | None = None,
    novel_context: str = "",
    clean_db: bool = True,
) -> tuple[Pipeline, TokenCountingLLM]:
    """
    변형별 splitter/resolver를 받아 인덱싱 DAG를 조립해 (pipeline, llm)을 반환한다.

    llm을 함께 반환하는 이유: 호출측(harness)이 실행 후 llm의 누적 토큰 카운터를 읽어야 한다.

    novel_context: 회차 누적 인덱싱에서 각 청크 프롬프트에 주입할 배경 컨텍스트
    (그래프 덤프 + rolling summary). 빈 문자열이면 배경 없이 추출한다.
    clean_db: Neo4jWriter가 쓰기를 마친 뒤 임시 속성 __tmp_internal_id를 지울지 여부.
    이름과 달리 DB를 비우는 옵션이 아니다 — Neo4jWriter에는 삭제 기능 자체가 없고,
    이 값은 쓰기 후 _db_cleaning() 호출 여부만 결정한다. 회차 누적 모드에서도 True가 맞다.

    schema/extractor는 매 파이프라인마다 새로 만든다(상태 오염 방지).
    스키마 자체(node_types/relationship_types/patterns)는 run 데이터로 주입하므로
    여기서는 SchemaBuilder 컴포넌트만 등록한다.
    """
    llm = build_llm(reasoning_effort)

    pipe = Pipeline()
    pipe.add_component(splitter, "splitter")
    # embedder는 DAG에서 뺐다: 회차 통째를 임베딩할 필요가 없고(coarse Chunk 제거),
    # 근거·벡터RAG용 임베딩은 indexing의 별도 Chunk 레이어가 KSS 조각 단위로 만든다.
    pipe.add_component(SchemaBuilder(), "schema")
    pipe.add_component(
        # 한국어 웹소설용 커스텀 프롬프트 + novel_context 주입 extractor.
        # V2 structured output 사용(OpenAILLM은 supports_structured_output=True).
        # on_error=RAISE: 한 청크의 추출 실패를 조용히 빈 그래프로 삼키지 않고 드러낸다.
        # create_lexical_graph=False: 라이브러리 자동 lexical graph(회차 통째 Chunk +
        # FROM_CHUNK + 임베딩)를 끈다 — Chunk 노드는 indexing의 KSS 근거 레이어가 전담한다.
        NovelContextExtractor(
            llm=llm,
            prompt_template=KoreanWebNovelERTemplate(),
            novel_context=novel_context,
            use_structured_output=True,
            create_lexical_graph=False,
            on_error=OnError.RAISE,
        ),
        "extractor",
    )
    pipe.add_component(GraphPruning(), "pruner")
    # clean_db=True면 쓰기 직후 __tmp_internal_id(관계 배선용 임시 id)를 노드에서 지운다.
    # 이 속성을 남기면 그래프 덤프(context.dump_graph_text)로 새어 나가 배경 컨텍스트가
    # UUID로 오염되므로 항상 켜 둔다. resolver는 name/elementId로만 매칭하고(이번 개편으로
    # Event/CharacterState도 name을 가져 resolver 매칭 대상에 들어온다) evidence.py는
    # evidence_chunk만 쓰므로, 이 시점에 지워도 이후 단계에 영향이 없다.
    pipe.add_component(
        Neo4jWriter(driver=driver, neo4j_database=database, clean_db=clean_db), "writer"
    )
    pipe.add_component(resolver, "resolver")

    # 데이터 흐름 배선 (하위 입력 파라미터 ← 상위 출력 필드)
    # embedder 제거로 splitter가 extractor의 chunks에 직접 연결된다.
    pipe.connect("splitter", "extractor", {"chunks": "splitter"})
    pipe.connect("schema", "extractor", {"schema": "schema"})
    pipe.connect("schema", "pruner", {"schema": "schema"})
    pipe.connect("extractor", "pruner", {"graph": "extractor"})
    pipe.connect("pruner", "writer", {"graph": "pruner.graph"})
    # resolver는 입력이 없어 데이터 매핑 없이 순서만 강제(writer 완료 후 실행).
    pipe.connect("writer", "resolver", {})

    return pipe, llm
