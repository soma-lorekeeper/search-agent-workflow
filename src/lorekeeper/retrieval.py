"""
Querying(조회) 단계 구현.

인덱싱으로 쌓인 KG를 여러 방식으로 조회하는 retriever들을 조립한다. 모든 retriever는
neo4j-graphrag의 base `Retriever`를 상속하거나 그 구현체이며, LLM 에이전트가 도구로 쓸 수
있도록 `convert_to_tool`로 감쌀 수 있다.

전략은 세 가지이고, **서로 다른 좌표계에 앵커한다**는 점이 설계의 핵심이다.
  1. hybrid_cypher : 원문 표현에 앵커. Chunk 임베딩+풀텍스트(cjk)로 원문 조각을 찾아 반환.
                     표기·숫자·고유명처럼 '그 표현이 원문에 있나'를 묻는 질의에 강하다.
  2. fact_search   : 사실·행위에 앵커. Event/CharacterState(`:Fact`)를 임베딩+풀텍스트로 찾고
                     참가자·근거 원문을 반환. '누가 X를 했나'처럼 주어가 의심
                     대상인 귀속형 질의, 규칙·제약 질의에 강하다.
  3. entity_search : 엔티티에 앵커. 인물·아이템·조직·장소를 이름/별칭으로 정확 조회해 속성과
                     상태 이력(회차순)·참여 사건을 반환. 임베딩을 쓰지 않는 정형 조회다.

유사도 검색(1)은 '질의에 있는 것과 비슷한 것'만 찾을 수 있어서, 질의에 없는 이름이 정답인
귀속·부재 증명형(예: "이지혜가 수표를 냈다" → 실제로는 한명오)을 원리상 풀지 못한다. 2·3은
닫힌 집합(사건의 참가자 목록, 인물의 상태 이력)을 돌려주므로 그 빈칸을 메운다.

핵심 설계:
  - 벡터 인덱스는 Chunk(chunk_emb)와 Fact(fact_emb) 두 계층에 있다. 앵커가 Chunk면 EVIDENCED_BY
    역방향으로 사실을 찾고, 앵커가 Fact면 EVIDENCED_BY 정방향으로 근거 원문을 딸려온다.
  - 결과는 LLM이 바로 읽을 수 있는 텍스트(content)로 직렬화하고, 소비 쪽이 노드 단위로
    병합·참조할 수 있도록 같은 값을 metadata에도 구조화해 담는다(kind/eid + 계층별 필드).
  - 이웃 서브그래프(`[관련 그래프]` 부록)는 **기본으로 수집하지 않는다**(include_graph=False).
    판정에 필요한 관계는 이미 본문에 흡수돼 있고(참가자 목록·근거 원문·[관련인물]/[상위]),
    부록은 같은 인물 설명이 결과마다 반복돼 근거 토큰의 2/3를 차지했다. 켜면 1-hop 도메인
    이웃과 '사실을 완성하는' 2-hop 관계(ESTABLISHED_IN/ABOUT/HOSTS/RELATED_TO/LOCATED_IN/
    PART_OF)까지 화이트리스트로 확장한다 — 참가자→그들의 상태로 재확장하지는 않는다.
    **사실 계층(fact_search)에는 이 부록이 아예 없다.** 설명문을 통째로 나르는 대신
    이름 수준의 `[연관]` 줄(_FACT_RELATED)로 대체했다 — 사실 1개당 0.42개만 늘어난다.
"""

from __future__ import annotations

import os
import re

import neo4j

# 외부 라이브러리 — retriever/임베더/도구 타입.
from neo4j_graphrag.embeddings import OpenAIEmbeddings
from neo4j_graphrag.retrievers import HybridCypherRetriever
from neo4j_graphrag.retrievers.base import Retriever
from neo4j_graphrag.types import RawSearchResult, RetrieverResultItem

# 패키지 내부 모듈 — 곧 lorekeeper 패키지로 묶이므로 상대 import로 작성한다.
from .chunks import CHUNK_FULLTEXT_INDEX, CHUNK_VECTOR_INDEX
from .client import get_driver
from .facts import FACT_FULLTEXT_INDEX, FACT_VECTOR_INDEX
from .pipeline import EMBEDDING_MODEL

# ---------------------------------------------------------------------------
# 0) 내부 헬퍼 (드라이버·DB 이름·임베더)
# ---------------------------------------------------------------------------

# neo4j database 이름. indexing.py와 동일하게 NEO4J_DATABASE(기본 'neo4j')를 쓴다.
DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

# 모듈 레벨 lazy singleton 캐시. retriever/인덱스 헬퍼가 매번 새 드라이버를 만들지 않도록
# 최초 호출 시 한 번만 만들어 재사용한다.
_DRIVER: neo4j.Driver | None = None
_EMBEDDER: OpenAIEmbeddings | None = None


def _driver() -> neo4j.Driver:
    """공유 Neo4j 드라이버(lazy singleton). 최초 호출 시 get_driver()로 생성 후 캐시."""
    global _DRIVER
    if _DRIVER is None:
        _DRIVER = get_driver()
    return _DRIVER


def _embedder() -> OpenAIEmbeddings:
    """
    질의 텍스트를 임베딩할 embedder(lazy singleton).

    ⚠️ pipeline.build_embedder()는 청킹용 TextChunkEmbedder를 반환하므로 retriever에는
    쓸 수 없다(embed_query 인터페이스가 아님). retriever가 요구하는 embed_query를 가진
    OpenAIEmbeddings를 직접 만들어야 한다. 임베딩 모델은 인덱싱 때 Chunk를 임베딩한 모델과
    같아야(EMBEDDING_MODEL) 벡터 공간이 일치한다.
    """
    global _EMBEDDER
    if _EMBEDDER is None:
        _EMBEDDER = OpenAIEmbeddings(model=EMBEDDING_MODEL)
    return _EMBEDDER


# Lucene 질의 문법에서 특수 의미를 갖는 문자들. 한국어 본문·시스템 메시지에는 그냥 문장부호로
# 섞여 있어서(예: `[체력 Lv.1 -> 체력 Lv.10]`), 이스케이프하지 않으면 풀텍스트 질의가 문법
# 오류로 깨진다. 대괄호가 든 질의에서 실제로 확인된 버그다.
_LUCENE_SPECIAL = re.compile(r'([+\-&|!(){}\[\]^"~*?:\\/])')


def _escape_lucene(query: str) -> str:
    """풀텍스트 인덱스에 넘길 질의의 Lucene 특수문자를 이스케이프한다."""
    return _LUCENE_SPECIAL.sub(r"\\\1", query)


class _EscapedHybridCypherRetriever(HybridCypherRetriever):
    """
    풀텍스트 채널에만 Lucene 이스케이프를 적용하는 HybridCypherRetriever.

    이스케이프를 질의 문자열 전체에 미리 걸면 **임베딩까지 오염된다** — `\\[체력\\]` 같은
    백슬래시투성이 문자열이 벡터 검색의 입력이 되어 의미가 흐려진다. 라이브러리는
    query_vector가 주어지면 그것을 벡터 검색에 쓰고 query_text는 풀텍스트에만 쓰므로
    (hybrid.py의 `if query_text and not query_vector` 분기), 임베딩을 **원문으로 먼저
    계산해 넘기고** query_text만 이스케이프하면 두 채널을 깔끔히 분리할 수 있다.
    """

    def get_search_results(
        self, query_text: str, query_vector: list[float] | None = None, **kwargs
    ) -> RawSearchResult:
        if query_vector is None and query_text:
            # 임베딩은 이스케이프하지 않은 원문으로 계산한다.
            query_vector = self.embedder.embed_query(query_text)
        return super().get_search_results(
            query_text=_escape_lucene(query_text),
            query_vector=query_vector,
            **kwargs,
        )


# ---------------------------------------------------------------------------
# 1) 청크 계층 retrieval_query (VectorCypher·HybridCypher 공용)
# ---------------------------------------------------------------------------
# 앵커 Chunk(`node`)에서 EVIDENCED_BY 역방향으로 사실을 모으고, 각 사실에서 1-hop 도메인
# 이웃(+RELATED_TO 인물, LOCATED_IN 상위 장소, PART_OF 상위 조직)으로 확장한 뒤 그 노드 집합
# 내부의 관계만 추리는 무거운 절. include_graph=False면 통째로 빠진다 — 남는 건 IN_CHAPTER
# 한 줄이라 쿼리 자체가 크게 가벼워진다.
_CHUNK_GRAPH = """
OPTIONAL MATCH (node)<-[:EVIDENCED_BY]-(fact)
WHERE fact:Event OR fact:CharacterState
WITH node, score, ch, collect(DISTINCT fact) AS facts
CALL (facts) {
  UNWIND facts AS f
  MATCH (f)--(nbr)
  WHERE nbr:Character OR nbr:Location OR nbr:Organization
        OR nbr:Item OR nbr:Event OR nbr:CharacterState
  OPTIONAL MATCH (nbr)-[:RELATED_TO]-(rc:Character)
  OPTIONAL MATCH (nbr)-[:LOCATED_IN*1..]->(pl:Location)
  OPTIONAL MATCH (nbr)-[:PART_OF*1..]->(po:Organization)
  UNWIND [nbr, rc, pl, po] AS x
  WITH x WHERE x IS NOT NULL
  RETURN collect(DISTINCT x) AS neighbors
}
WITH node, score, ch, facts + neighbors AS raw
CALL (raw) {
  UNWIND raw AS n
  RETURN collect(DISTINCT n) AS subgraph
}
CALL (subgraph) {
  UNWIND subgraph AS a
  MATCH (a)-[r]->(b)
  WHERE b IN subgraph
  RETURN collect(DISTINCT {
    source: a.name, source_labels: labels(a),
    type: type(r), props: properties(r),
    target: b.name, target_labels: labels(b)
  }) AS relationships
}"""

# 그래프를 끄면 subgraph·relationships 바인딩 자체가 없으므로 RETURN에서도 빠져야 한다.
_CHUNK_GRAPH_RETURN = """,
  [n IN subgraph | {labels: labels(n), name: n.name, description: n.description}] AS nodes,
  relationships"""


def _build_chunk_query(include_graph: bool) -> str:
    """청크 계층 retrieval_query. `node`/`score`는 base 벡터 쿼리가 바인딩해 준다.

    eid(elementId)는 소비 쪽에서 노드 단위로 결과를 병합할 때 쓰는 키다 — 같은 청크가
    hybrid의 앵커로도, fact의 근거 원문으로도 실려 오기 때문에 자연키만으로는 두 경로가
    같은 노드인지 확신할 수 없다(동명·동좌표 충돌 여지).
    """
    return f"""
WITH node, score
OPTIONAL MATCH (node)-[:IN_CHAPTER]->(ch:Chapter){_CHUNK_GRAPH if include_graph else ""}
RETURN
  elementId(node) AS eid,
  node.text AS content,
  coalesce(node.chapter, ch.number) AS chapter,
  node.index AS chunk_index,
  score{_CHUNK_GRAPH_RETURN if include_graph else ""}
"""


# ---------------------------------------------------------------------------
# 2) 정규화 result_formatter (VectorCypher·HybridCypher 공용)
# ---------------------------------------------------------------------------
# 그래프 덤프에서 뺄 메타/lexical 라벨. content에는 도메인 라벨만 노출한다.
# 'Fact'는 facts.ensure_fact_layer가 Event/CharacterState에 붙이는 검색용 보조 라벨이라
# 도메인 라벨이 아니다(빼지 않으면 '(Fact) 한명오가...'로 렌더될 수 있다 — labels() 순서는
# 비결정적이라 Event가 먼저 온다는 보장이 없다).
_META_LABELS = {"__Entity__", "__KGBuilder__", "Chunk", "Chapter", "Story", "Fact"}


def _domain_label(labels: list[str] | None) -> str:
    """라벨 목록에서 메타 라벨을 뺀 도메인 라벨 하나를 고른다(없으면 'Node')."""
    if not labels:
        return "Node"
    domain = [lab for lab in labels if lab not in _META_LABELS]
    return domain[0] if domain else "Node"


def _props_summary(props: dict | None) -> str:
    """관계 속성(dict)을 'k=v, k=v'로 요약한다. 값이 비면 생략."""
    if not props:
        return ""
    return ", ".join(f"{k}={v}" for k, v in props.items() if v not in (None, ""))


def _render_subgraph(nodes: list, relationships: list) -> str:
    """
    record 하나의 서브그래프(nodes/relationships)를 사람이 읽기 좋은 여러 줄로 직렬화한다.

    context.dump_graph_text의 렌더 스타일을 참고하되, 여기서는 전체 DB가 아니라 record 하나의
    작은 서브그래프만 렌더한다. 노드는 '(라벨) 이름 — 설명', 관계는 'source —타입(속성)→ target'.
    """
    lines: list[str] = []

    # --- 노드 줄 ---
    for n in nodes or []:
        name = n.get("name") or "?"
        label = _domain_label(n.get("labels"))
        desc = n.get("description")
        line = f"- ({label}) {name}"
        if desc:
            line += f" — {desc}"
        lines.append(line)

    # --- 관계 줄 ---
    for r in relationships or []:
        src = r.get("source") or "?"
        tgt = r.get("target") or "?"
        rtype = r.get("type") or ""
        prop_str = _props_summary(r.get("props"))
        suffix = f"({prop_str})" if prop_str else ""
        lines.append(f"- {src} —{rtype}{suffix}→ {tgt}")

    return "\n".join(lines)


def _graph_result_formatter(record: neo4j.Record) -> RetrieverResultItem:
    """
    VectorCypher/HybridCypher record → RetrieverResultItem.

    _RETRIEVAL_QUERY가 RETURN하는 컬럼(content/chapter/chunk_index/score/nodes/relationships)을
    LLM이 바로 읽을 수 있는 텍스트로 렌더한다. content는 '원문 발췌 + 관련 그래프' 두 섹션으로,
    metadata에는 구조화 필드를 그대로 담아 후속 처리(인용·필터)에 쓸 수 있게 한다.
    """
    text = record.get("content")
    chapter = record.get("chapter")
    # include_graph=False면 쿼리가 이 컬럼들을 아예 RETURN하지 않는다. neo4j.Record.get은
    # 없는 키에 default(None)를 돌려주므로 여기서 빈 리스트가 되고, 아래 graph_text가 ""가
    # 되어 [관련 그래프] 섹션이 자연히 빠진다 — 포매터에 분기를 둘 필요가 없다.
    nodes = record.get("nodes") or []
    relationships = record.get("relationships") or []

    # 발췌 섹션 머리말(회차 표기는 있을 때만).
    chapter_label = f" · {chapter}화" if chapter is not None else ""
    parts = [f"[원문 발췌{chapter_label}]", text or ""]

    # 관련 그래프 섹션(노드/관계가 하나라도 있으면).
    graph_text = _render_subgraph(nodes, relationships)
    if graph_text:
        parts.append("\n[관련 그래프]\n" + graph_text)

    content = "\n".join(parts)

    return RetrieverResultItem(
        content=content,
        metadata={
            # kind/eid는 세 도구가 공유하는 유일한 공통 키다. 소비 쪽이 도구 이름이 아니라
            # kind로 분기하면 라우팅이 바뀌어도 조립 로직은 그대로다.
            "kind": "chunk",
            "eid": record.get("eid"),
            "text": text,  # content 문자열을 되파싱하지 않고 원문을 그대로 꺼내 쓰라고
            "chapter": chapter,
            "chunk_index": record.get("chunk_index"),
            "score": record.get("score"),
            "nodes": nodes,
            "relationships": relationships,
        },
    )


# ---------------------------------------------------------------------------
# 3) retriever 팩토리들 (전부 인자 없음)
# ---------------------------------------------------------------------------
# 풀텍스트 인덱스 이름은 인덱싱 생성처(chunks.py)와 단일 출처를 공유한다. 이 인덱스(cjk analyzer)는
# 인덱싱(chunks.write_chunk_layer)이 만들므로 여기서는 이름만 참조해 HybridCypher에 연결한다.
_FT_INDEX = CHUNK_FULLTEXT_INDEX


def build_hybrid_cypher_retriever(include_graph: bool = False) -> HybridCypherRetriever:
    """
    벡터+풀텍스트 하이브리드 + 그래프 확장 retriever(청크 계층).

    풀텍스트 인덱스(_FT_INDEX)는 cjk analyzer로 인덱싱 때 생성된다(chunks.write_chunk_layer).
    _EscapedHybridCypherRetriever를 쓰므로 대괄호 등 Lucene 특수문자가 든 질의도 깨지지 않는다.
    """
    return _EscapedHybridCypherRetriever(
        driver=_driver(),
        vector_index_name=CHUNK_VECTOR_INDEX,
        fulltext_index_name=_FT_INDEX,
        retrieval_query=_build_chunk_query(include_graph),
        embedder=_embedder(),
        result_formatter=_graph_result_formatter,
        neo4j_database=DATABASE,
    )


# ---------------------------------------------------------------------------
# 4) fact_search — 사실 계층 하이브리드 검색
# ---------------------------------------------------------------------------
# 사실(f)에서 뻗는 이웃 확장. fact_search와 entity_search가 공유한다.
#   1-hop: 사실에 직접 붙은 도메인 노드(참가자·무대·대상 등)만 화이트리스트로 받는다.
#   2-hop: 그 이웃에서 '사실을 완성하는' 관계만 한 번 더 탄다 — 상태가 성립한 사건
#          (ESTABLISHED_IN), 상태의 대상(ABOUT), 사건의 무대(HOSTS), 인물 관계(RELATED_TO),
#          상위 장소·조직(LOCATED_IN/PART_OF).
# APPEARS_IN/HAS_STATE로는 2-hop을 타지 않는다. 타면 '사건 → 다른 참가자 → 그들의 모든 상태'로
# 번져서, 김독자처럼 사건 수십 개에 참여하는 인물에서 그래프 절반이 딸려온다.
_FACT_NEIGHBORS = """
  MATCH (f)--(nbr)
  WHERE nbr:Character OR nbr:Location OR nbr:Organization
        OR nbr:Item OR nbr:Event OR nbr:CharacterState
  OPTIONAL MATCH (nbr)-[:ESTABLISHED_IN|ABOUT|HOSTS]->(w)
  OPTIONAL MATCH (nbr)-[:RELATED_TO]-(rc:Character)
  OPTIONAL MATCH (nbr)-[:LOCATED_IN*1..]->(pl:Location)
  OPTIONAL MATCH (nbr)-[:PART_OF*1..]->(po:Organization)
  UNWIND [nbr, w, rc, pl, po] AS x
  WITH x WHERE x IS NOT NULL
"""

# 사실의 성립 회차와 근거 원문을 뽑는 공통 절. fact_search와 entity_search가 공유한다.
#   회차는 3단 폴백이다 — Event는 자체 chapter를 갖지만 CharacterState는 회차 속성이 아예
#   없어서(schema.py) ESTABLISHED_IN이 가리키는 Event의 chapter를 쓰고, 그것도 없으면 근거
#   Chunk가 속한 회차로 대체한다. 여러 개면 가장 이른 회차를 성립 시점으로 본다.
_FACT_CHAPTER_EVIDENCE = """
OPTIONAL MATCH (f)-[:ESTABLISHED_IN]->(est:Event)
OPTIONAL MATCH (f)-[:EVIDENCED_BY]->(ck:Chunk)
"""

# 사실이 어떤 도메인 노드와 묶여 있는지를 **이름으로만** 딸고 오는 절. 사실 타입마다 붙는
# 관계가 다르므로 갈라서 받는다(schema.PATTERNS와 1:1이다).
#   Event         ← (Location)-[:HOSTS]->     무대가 된 장소
#   CharacterState → -[:ABOUT]-> Item|Organization  소유물·소속 조직처럼 상태가 가리키는 대상
# 인물은 위쪽 participants가 이미 담당하므로 여기서 다시 받지 않는다.
#
# 설명문(description)을 싣지 않는 것이 요점이다. 예전 `[관련 그래프]` 부록은 같은 인물 설명이
# 결과마다 반복돼 근거 토큰의 2/3를 차지했는데, 판정에 실제로 필요한 건 "이 사실이 어느 조직·
# 물건·장소에 묶여 있나"라는 이름 수준의 연결뿐이다. 실측으로 사실 1개당 0.42개만 늘어난다.
_FACT_RELATED = """
OPTIONAL MATCH (loc:Location)-[:HOSTS]->(f) WHERE f:Event
OPTIONAL MATCH (f)-[:ABOUT]->(tgt) WHERE f:CharacterState AND (tgt:Item OR tgt:Organization)
WITH f, score, chapter, evidence, participants,
     [x IN collect(DISTINCT {name: loc.name, label: 'Location', rel: 'HOSTS'})
      WHERE x.name IS NOT NULL]
     + [x IN collect(DISTINCT {name: tgt.name, rel: 'ABOUT',
                               label: head([l IN labels(tgt) WHERE l IN ['Item', 'Organization']])})
        WHERE x.name IS NOT NULL] AS related
"""


def _build_fact_query() -> str:
    """사실 계층 retrieval_query. 앵커 사실(`node`)에서 참가자·연관 노드·근거 원문을 모은다.

    청크 계층(_build_chunk_query)이 Chunk에서 사실로 거슬러 올라가는 것과 방향이 반대다.
    근거 원문에 eid를 함께 실어, 소비 쪽이 hybrid가 앵커로 물어온 청크와 같은 노드임을
    알아보고 한 번만 싣게 한다(같은 청크가 두 경로로 이중 수납되는 것을 막는다).

    include_graph 분기가 없다 — 예전의 `[관련 그래프]` 부록(1~2홉 서브그래프 덤프)은
    _FACT_RELATED가 이름 수준으로 대체했다. 청크 계층에는 부록이 그대로 남아 있다.
    """
    return f"""
WITH node AS f, score
{_FACT_CHAPTER_EVIDENCE}
WITH f, score,
     coalesce(f.chapter, min(est.chapter), min(ck.chapter)) AS chapter,
     [e IN collect(DISTINCT {{eid: elementId(ck), chapter: ck.chapter,
                              index: ck.index, text: ck.text}})
      WHERE e.text IS NOT NULL] AS evidence
// 이 사실에 참여한 인물. 귀속형 질의("누가 X를 했나")의 답이자, 목록에 없다는 사실 자체가
// 부재 증명의 근거가 된다.
OPTIONAL MATCH (p:Character)-[:APPEARS_IN|HAS_STATE]->(f)
WITH f, score, chapter, evidence,
     [n IN collect(DISTINCT p.name) WHERE n IS NOT NULL] AS participants
{_FACT_RELATED}
RETURN
  elementId(f) AS eid,
  f.name AS name, f.description AS description, labels(f) AS fact_labels,
  chapter, f.story_order AS story_order, score, evidence,
  participants, related
"""


def _render_evidence(evidence: list | None) -> list[str]:
    """근거 원문 줄들. 원문은 **가공하지 않고 그대로** 싣는다.

    판정 단계가 원문 표현을 그대로 대조하고, 평가 하네스도 마커 문자열을 부분 일치로 찾기
    때문에, 요약하거나 잘라내면 두 용도가 모두 깨진다.
    """
    lines = []
    for e in sorted(evidence or [], key=lambda x: (x.get("chapter") or 0, x.get("index") or 0)):
        head = f"{e.get('chapter')}화#{e.get('index')}"
        lines.append(f"[근거 원문 · {head}] {e.get('text')}")
    return lines


def _fact_result_formatter(record: neo4j.Record) -> RetrieverResultItem:
    """
    fact_search record → RetrieverResultItem.

    청크 계층의 `[원문 발췌 · N화]` 머리말은 쓰지 않는다 — 그 문자열은 소비 쪽이 회차를
    정규식으로 파싱하는 계약이라, 사실 계층이 같은 머리말을 쓰면 두 계층이 섞인다.
    """
    chapter = record.get("chapter")
    participants = record.get("participants") or []
    related = record.get("related") or []

    chapter_label = f" · {chapter}화" if chapter is not None else ""
    # 서사 순서(Event에만 있는 속성). 회차만으로는 같은 회차 안의 선후를 알 수 없어서,
    # "A가 B보다 먼저/나중"을 다투는 시점·순서형 판정이 회차 단위에서 막힌다.
    story_order = record.get("story_order")
    order_label = f" · 순서 {story_order}" if story_order is not None else ""
    label = _domain_label(record.get("fact_labels"))
    head = f"[사실{chapter_label}{order_label}] ({label}) {record.get('name')}"
    if record.get("description"):
        head += f" — {record.get('description')}"
    parts = [head]

    if participants:
        parts.append("[참가자] " + ", ".join(str(p) for p in participants))
    # 이 사실이 묶여 있는 조직·물건·장소. `이름(라벨, 관계)` 한 줄로 낸다 — 소속·소유·무대를
    # 대조하는 판정(예: "한명오는 인사팀 부장"이 재무팀 노드와 어긋나는가)이 이 줄을 쓴다.
    if related:
        parts.append("[연관] " + ", ".join(
            f"{r.get('name')}({r.get('label')}, {r.get('rel')})" for r in related if r.get("name")
        ))
    parts.extend(_render_evidence(record.get("evidence")))

    return RetrieverResultItem(
        content="\n".join(parts),
        metadata={
            "kind": "fact",
            "eid": record.get("eid"),
            "name": record.get("name"),
            "description": record.get("description"),
            "fact_labels": record.get("fact_labels"),
            "chapter": chapter,
            "story_order": story_order,
            "score": record.get("score"),
            "participants": participants,
            "related": related,
            "evidence": record.get("evidence") or [],
        },
    )


def build_fact_search_retriever(include_graph: bool = False) -> HybridCypherRetriever:
    """
    사실(Event/CharacterState) 계층 하이브리드 검색 retriever.

    인덱스(fact_emb / fact_text_ft)는 facts.ensure_fact_layer가 만든다. 검색 대상이 원문
    82만 자가 아니라 정제된 사실 노드라 신호 밀도가 청크 계층과 자릿수로 다르다.

    include_graph는 **사실 계층에서 더 이상 쓰이지 않는다**. 예전의 `[관련 그래프]` 부록은
    이름 수준의 `[연관]` 줄(_FACT_RELATED)로 대체됐다. 인자를 남겨 둔 것은 build_retrievers·
    build_retrieval_tools가 세 retriever에 같은 플래그를 넘기는 공개 API이기 때문이고,
    청크 계층(build_hybrid_cypher_retriever)에서는 그대로 동작한다.
    """
    return _EscapedHybridCypherRetriever(
        driver=_driver(),
        vector_index_name=FACT_VECTOR_INDEX,
        fulltext_index_name=FACT_FULLTEXT_INDEX,
        retrieval_query=_build_fact_query(),
        embedder=_embedder(),
        result_formatter=_fact_result_formatter,
        neo4j_database=DATABASE,
    )


# ---------------------------------------------------------------------------
# 5) entity_search — 엔티티 정형 조회 (임베딩 없음)
# ---------------------------------------------------------------------------
# 쿼리 1: 이름/별칭으로 엔티티를 특정하고 프로필(속성·인물관계·상위 계층)을 낸다.
#   aliases는 STRING(쉼표 나열)이라 `$name IN e.aliases`로는 못 찾는다 — 그건 리스트 속성일
#   때의 문법이고, 문자열에 쓰면 부분 문자열 검사가 되어 '강철검제' 같은 별칭이 통째로
#   누락된다. split+trim으로 원소를 만들어 정확히 비교한다.
_ENTITY_PROFILE_QUERY = """
MATCH (e)
WHERE (e:Character OR e:Item OR e:Organization OR e:Location)
  AND (e.name = $entity_name
       OR any(a IN split(coalesce(e.aliases, ''), ',') WHERE trim(a) = $entity_name))
OPTIONAL MATCH (e)-[rel:RELATED_TO]-(oc:Character)
WITH e, [r IN collect(DISTINCT {name: oc.name, type: rel.type, description: rel.description})
         WHERE r.name IS NOT NULL] AS related_characters
OPTIONAL MATCH (e)-[:LOCATED_IN*1..]->(pl:Location)
OPTIONAL MATCH (e)-[:PART_OF*1..]->(po:Organization)
RETURN 'profile' AS kind, elementId(e) AS eid, labels(e) AS entity_labels,
       e.name AS name, e.aliases AS aliases, e.description AS description,
       related_characters,
       [x IN collect(DISTINCT pl.name) WHERE x IS NOT NULL]
       + [x IN collect(DISTINCT po.name) WHERE x IS NOT NULL] AS parents
"""

# 쿼리 2: 그 엔티티에 걸린 사실을 성립 회차 순으로 낸다.
#   엔티티→사실 연결은 라벨마다 관계가 다르다(Character는 APPEARS_IN·HAS_STATE, Location은
#   HOSTS, Item/Organization은 CharacterState의 ABOUT 역방향). 관계 타입을 열거하는 대신
#   방향·타입 없는 `(e)--(f)`로 한 번에 잡고 f의 라벨로만 거른다.
_ENTITY_FACTS_GRAPH = f"""
CALL (f) {{
{_FACT_NEIGHBORS}
  RETURN collect(DISTINCT {{labels: labels(x), name: x.name, description: x.description}}) AS neighbors
}}"""


def _build_entity_facts_query(include_graph: bool) -> str:
    """엔티티에 걸린 사실을 성립 회차 순으로 내는 쿼리.

    이웃 확장(CALL)은 LIMIT 60 사실마다 도는 2-hop이라 세 도구 중 가장 비싸다 —
    include_graph=False의 속도 이득이 여기서 가장 크다.
    """
    return f"""
MATCH (e) WHERE elementId(e) IN $eids
MATCH (e)--(f) WHERE f:Event OR f:CharacterState
{_FACT_CHAPTER_EVIDENCE}
WITH e, f,
     coalesce(f.chapter, min(est.chapter), min(ck.chapter)) AS est_chapter,
     [x IN collect(DISTINCT {{eid: elementId(ck), chapter: ck.chapter,
                              index: ck.index, text: ck.text}})
      WHERE x.text IS NOT NULL] AS evidence
WHERE $up_to_chapter IS NULL OR est_chapter <= $up_to_chapter
OPTIONAL MATCH (p:Character)-[:APPEARS_IN|HAS_STATE]->(f)
WITH e, f, est_chapter, evidence,
     [n IN collect(DISTINCT p.name) WHERE n IS NOT NULL] AS participants{
    _ENTITY_FACTS_GRAPH if include_graph else ""}
RETURN 'fact' AS kind, elementId(f) AS eid, e.name AS entity, labels(f) AS fact_labels,
       f.name AS fact_name, f.description AS fact_description,
       est_chapter, f.story_order AS story_order, evidence, participants{
    ", neighbors" if include_graph else ""}
ORDER BY est_chapter, story_order, fact_name
LIMIT 60
"""


class EntitySearchRetriever(Retriever):
    """
    인물·아이템·조직·장소를 이름/별칭으로 정확 조회하는 retriever(임베딩 없음).

    동작 흐름:
      - 쿼리 1로 $entity_name(이름 또는 별칭)에 해당하는 엔티티를 특정하고 프로필을 낸다.
      - 쿼리 2로 그 엔티티에 걸린 사실(Event/CharacterState)을 성립 회차 순으로 낸다.
        각 사실에는 참가자·근거 원문·화이트리스트 이웃이 딸려온다.
      - up_to_chapter가 주어지면 그 회차까지 성립한 사실만 남긴다(특정 시점의 스냅샷).
        신규 회차를 검사할 때 '아직 일어나지 않은 일'이 근거로 새는 것을 막는 장치다.

    유사도 검색과 달리 닫힌 집합을 돌려주므로, "그 목록에 없다"는 부재 증명이 가능하다.
    """

    def __init__(self, include_graph: bool = False) -> None:
        # base Retriever가 Neo4j 버전을 검증하므로(VERIFY_NEO4J_VERSION=True) 유효한 드라이버가 필요하다.
        super().__init__(_driver(), neo4j_database=DATABASE)
        # 쿼리를 인스턴스에 굳혀 둔다 — get_search_results 시그니처에 넣으면 convert_to_tool이
        # 자동 추론해 LLM 파라미터로 노출해 버린다(base.py의 추론기엔 bool 분기가 없어
        # string으로 샌다). 켜고 끄는 건 호출자가 아니라 조립 시점의 결정이다.
        self._facts_query = _build_entity_facts_query(include_graph)

    def get_search_results(
        self, entity_name: str, up_to_chapter: int | None = None
    ) -> RawSearchResult:
        """
        엔티티 프로필 + 사실 이력을 조회한다.

        Args:
            entity_name: 조회할 엔티티의 이름 또는 별칭.
            up_to_chapter: 이 회차까지 성립한 사실만 조회(None이면 전체 이력).

        Returns:
            RawSearchResult: profile record 뒤에 fact record들이 이어진 리스트.
            포맷팅은 default_record_formatter가 kind로 분기해 담당한다.
        """
        profiles, _, _ = self.driver.execute_query(
            _ENTITY_PROFILE_QUERY,
            {"entity_name": entity_name},
            database_=self.neo4j_database,
            routing_=neo4j.RoutingControl.READ,
        )
        if not profiles:
            # 이름·별칭 어디에도 없는 엔티티. 빈 결과를 그대로 돌려준다(소비 쪽이 '결과 없음'으로 렌더).
            return RawSearchResult(records=[], metadata={})

        eids = [p["eid"] for p in profiles]
        facts, _, _ = self.driver.execute_query(
            self._facts_query,
            {"eids": eids, "up_to_chapter": up_to_chapter},
            database_=self.neo4j_database,
            routing_=neo4j.RoutingControl.READ,
        )
        return RawSearchResult(records=list(profiles) + list(facts), metadata={})

    def default_record_formatter(self, record: neo4j.Record) -> RetrieverResultItem:
        """record 하나를 LLM-ready 텍스트로 렌더한다. kind(profile/fact)로 분기한다."""
        if record.get("kind") == "profile":
            return self._format_profile(record)
        return self._format_fact(record)

    def _format_profile(self, record: neo4j.Record) -> RetrieverResultItem:
        """엔티티 프로필: '(라벨) 이름 [별칭: ...] — 설명 [관련인물: ...] [상위: ...]'."""
        label = _domain_label(record.get("entity_labels"))
        related = record.get("related_characters") or []
        parents = record.get("parents") or []

        line = f"[엔티티] ({label}) {record.get('name')}"
        if record.get("aliases"):
            line += f" [별칭: {record.get('aliases')}]"
        if record.get("description"):
            line += f" — {record.get('description')}"
        if related:
            rel_str = ", ".join(
                f"{r.get('name')}({r.get('type')})" if r.get("type") else str(r.get("name"))
                for r in related
                if r.get("name")
            )
            if rel_str:
                line += f" [관련인물: {rel_str}]"
        if parents:
            line += " [상위: " + ", ".join(str(p) for p in parents) + "]"

        return RetrieverResultItem(
            content=line,
            metadata={
                "kind": "profile",
                "eid": record.get("eid"),  # 쿼리는 원래 반환하고 있었고, 여기서 버려지던 값
                "name": record.get("name"),
                "entity_labels": record.get("entity_labels"),
                "aliases": record.get("aliases"),
                "description": record.get("description"),
                "related_characters": related,
                "parents": parents,
            },
        )

    def _format_fact(self, record: neo4j.Record) -> RetrieverResultItem:
        """사실 한 건: 회차 머리말 + 참가자 + 근거 원문 + 이웃 요약."""
        chapter = record.get("est_chapter")
        label = _domain_label(record.get("fact_labels"))
        participants = record.get("participants") or []
        neighbors = record.get("neighbors") or []

        head = f"{chapter}화" if chapter is not None else "?화"
        line = f"[사실 · {head}] ({label}) {record.get('fact_name')}"
        if record.get("fact_description"):
            line += f" — {record.get('fact_description')}"
        parts = [line]

        if participants:
            parts.append("[참가자] " + ", ".join(str(p) for p in participants))
        parts.extend(_render_evidence(record.get("evidence")))

        # 이웃은 관계 없이 노드만 짧게 나열한다(사실 하나당 줄 수를 억제).
        nbr_text = _render_subgraph(neighbors, [])
        if nbr_text:
            parts.append("[관련 그래프]\n" + nbr_text)

        return RetrieverResultItem(
            content="\n".join(parts),
            metadata={
                "kind": "fact",
                "eid": record.get("eid"),
                "entity": record.get("entity"),
                "fact_name": record.get("fact_name"),
                "fact_labels": record.get("fact_labels"),
                "description": record.get("fact_description"),
                "chapter": chapter,
                "story_order": record.get("story_order"),
                "participants": participants,
                "evidence": record.get("evidence") or [],
                "neighbors": neighbors,
            },
        )


def build_entity_search_retriever(include_graph: bool = False) -> "EntitySearchRetriever":
    """엔티티를 이름/별칭으로 정확 조회하는 커스텀 retriever."""
    return EntitySearchRetriever(include_graph=include_graph)


# ---------------------------------------------------------------------------
# 6) 노출 함수 (retriever 묶음 · 도구 묶음)
# ---------------------------------------------------------------------------


def build_retrievers(include_graph: bool = False) -> dict[str, Retriever]:
    """
    세 가지 retriever를 이름 → 인스턴스 dict로 조립해 반환한다.

    키: hybrid_cypher / fact_search / entity_search.

    include_graph=True면 각 결과 뒤에 `[관련 그래프]` 부록(앵커에서 1~2홉 떨어진 이웃 노드의
    설명 + 관계 트리플)이 붙는다. 기본은 끔 — 판정에 필요한 관계는 이미 본문에 구조화 텍스트로
    흡수돼 있고(참가자 목록·근거 원문·[관련인물]/[상위]), 부록은 같은 인물 설명이 결과마다
    반복돼 실측상 근거 토큰의 2/3를 차지했다. 도달률은 부록을 떼도 불변이었다.
    """
    return {
        "hybrid_cypher": build_hybrid_cypher_retriever(include_graph),
        "fact_search": build_fact_search_retriever(include_graph),
        "entity_search": build_entity_search_retriever(include_graph),
    }


def build_retrieval_tools(include_graph: bool = False) -> list:
    """
    각 retriever를 LLM 에이전트용 Tool(neo4j_graphrag.tool.Tool)로 감싼 리스트를 반환한다.

    convert_to_tool(name, description, parameter_descriptions)은 retriever의
    get_search_results 시그니처에서 파라미터를 자동 추론한다(base.py 참고). 여기서는 각 도구에
    한국어 name/description과 주요 파라미터 설명을 부여해 LLM이 도구 선택·인자 지정을 잘 하도록 한다.

    include_graph는 여기서 소비되고 끝난다 — 도구 파라미터가 아니라 조립 시점의 결정이라
    LLM에는 보이지 않는다(build_retrievers 참고).
    """
    retrievers = build_retrievers(include_graph)

    tools = []

    # 청크 계층 하이브리드 검색(벡터 + 풀텍스트). 풀텍스트 인덱스(cjk)는 인덱싱이 생성한다.
    tools.append(
        retrievers["hybrid_cypher"].convert_to_tool(
            name="hybrid_search",
            description=(
                "벡터 검색과 풀텍스트 검색을 결합해 **원문 조각**을 찾아 반환한다. "
                "표기·숫자·고유명처럼 '그 표현이 원문에 있는가'를 확인할 때 쓴다."
            ),
            parameter_descriptions={
                "query_text": "검색할 자연어 질의(키워드 포함).",
                "top_k": "반환할 상위 결과 개수(기본 5).",
            },
        )
    )

    # 사실 계층 하이브리드 검색.
    tools.append(
        retrievers["fact_search"].convert_to_tool(
            name="fact_search",
            description=(
                "사건·인물상태를 의미로 검색해 **참가자 목록·근거 원문**을 반환한다. "
                "'누가 그 일을 했는가'처럼 행위의 주체를 확인할 때, 규칙·제약을 확인할 때 쓴다. "
                "참가자 목록에 특정 인물이 없다는 것 자체가 근거가 된다."
            ),
            parameter_descriptions={
                "query_text": "찾을 사건·상태를 서술한 자연어 질의(행위·목적어 중심).",
                "top_k": "반환할 상위 결과 개수(기본 5).",
            },
        )
    )

    # 엔티티 정형 조회.
    tools.append(
        retrievers["entity_search"].convert_to_tool(
            name="entity_search",
            description=(
                "인물·아이템·조직·장소를 이름 또는 별칭으로 정확 조회한다. 별칭·설명 같은 속성과 "
                "상태(신분·소속·능력·부상·생사·소유·역할) 이력을 성립 회차 순으로, 참여 사건과 함께 "
                "반환한다. 특정 대상의 속성을 확인할 때 가장 먼저 쓴다."
            ),
            parameter_descriptions={
                "entity_name": "조회할 대상의 이름 또는 별칭.",
                "up_to_chapter": "이 회차까지 성립한 사실만 조회한다(생략 시 전체 이력).",
            },
        )
    )

    return tools
