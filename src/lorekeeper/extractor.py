"""
한국어 웹소설용 커스텀 추출 프롬프트 + 증분 컨텍스트 주입 extractor.

두 클래스로 구성된다.
- KoreanWebNovelERTemplate : neo4j-graphrag 기본 영어 프롬프트(ERExtractionTemplate)를
  한국어 웹소설 도메인용으로 재작성한 프롬프트 템플릿. 라이브러리 원본의 일반 추출
  지시(역할·작업정의·출력구조·스키마제약·ID규칙·관계방향·JSON유효성)를 빠짐없이 이식한 뒤
  회차 마커·CharacterState 시간축 등 도메인 규칙을 얹고, 전용 {novel_context} placeholder를
  하나 추가한다.
- NovelContextExtractor : 위 템플릿의 {novel_context} 빈칸을, 청크 추출 시점에 인스턴스가
  들고 있는 누적 컨텍스트(그래프 덤프 + rolling summary)로 채워 넣는 extractor.

주의: NovelContextExtractor.extract_for_chunk는 neo4j_graphrag 1.18.0의
LLMEntityRelationExtractor.extract_for_chunk 본문을 그대로 복제하되 self.prompt_template.format
호출에 novel_context 인자만 추가한 것이다. 라이브러리가 format 인자를 하드코딩하고 있어
오버라이드가 불가피하다. 라이브러리 업그레이드 시 원본 extract_for_chunk가 바뀌면 이 메서드도
동기화해야 한다.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from typing import Optional

from pydantic import ValidationError, validate_call

from neo4j_graphrag.exceptions import LLMGenerationError
from neo4j_graphrag.experimental.components.entity_relation_extractor import (
    LLMEntityRelationExtractor,
    OnError,
    fix_invalid_json,
)
from neo4j_graphrag.experimental.components.schema import GraphSchema
from neo4j_graphrag.experimental.components.types import (
    DocumentInfo,
    LexicalGraphConfig,
    Neo4jGraph,
    TextChunk,
    TextChunks,
)
from neo4j_graphrag.experimental.pipeline.exceptions import InvalidJSONError
from neo4j_graphrag.generation.prompts import ERExtractionTemplate, PromptTemplate
from neo4j_graphrag.types import LLMMessage

# 복제한 extract_for_chunk V1 분기가 쓰는 로거(원본과 동일 이름 규칙).
logger = logging.getLogger(__name__)


class KoreanWebNovelERTemplate(ERExtractionTemplate):
    """
    한국어 웹소설 KG 추출용 프롬프트 템플릿.

    ERExtractionTemplate을 상속하되 DEFAULT_TEMPLATE를 한국어로 전면 재작성한다.
    placeholder 4개: {novel_context}, {schema}, {examples}, {text}.
    JSON 리터럴의 중괄호는 .format() 충돌을 피하려 반드시 {{ }}로 이스케이프한다.
    """

    # 원본 ERExtractionTemplate의 7개 요소(역할·작업정의·출력구조·스키마제약·ID규칙·
    # 관계방향·JSON유효성)를 한국어로 이식하고, 웹소설 도메인 규칙과 증분 컨텍스트
    # 우선순위 지침을 추가한 프롬프트.
    DEFAULT_TEMPLATE = """\
당신은 지식 그래프(knowledge graph)를 구축하기 위해 텍스트에서 구조화된 정보를 추출하는 최상위 알고리즘이다.

주어진 텍스트에서 엔티티(노드)를 찾아 각각의 타입을 지정하고, 그 노드들 사이의 관계도 함께 추출하라.

결과는 반드시 아래 형식의 JSON으로 반환하라(nodes 배열과 relationships 배열):
{{"nodes": [ {{"id": "0", "label": "Character", "properties": {{"name": "홍길동"}} }}],
"relationships": [{{"type": "APPEARS_IN", "start_node_id": "0", "end_node_id": "1", "properties": {{}} }}] }}

아래에 주어진 노드/관계 타입만 사용하라(주어진 경우). 스키마에 없는 타입은 만들지 말라:
{schema}

각 노드에는 문자열로 된 고유 ID를 부여하고, 관계를 정의할 때 그 ID를 그대로 재사용하라.
관계는 스키마 패턴이 정한 source/target 노드 타입과 방향을 반드시 지켜라(예: Character 가 Event 를 향하는 APPEARS_IN).

--- 웹소설 도메인 추출 규칙 ---
- 회차/순서: [chapter:N] 마커의 N을 각 Event.chapter에 넣는다. story_order는 작중 시간순 값 —
  같은 회차 내 여러 사건은 원문에 등장하는 순서대로 N.0, N.1, N.2…로 0.1씩 증가(사건 하나면 N.0).
  회상·과거 사건만 예외로 더 작은 값을 주고, 1화보다 이전(프리퀄)이면 0이나 음수도 가능.
- 인물에 관한 사실은 변하든 변하지 않든 전부 CharacterState 노드로 만들고, HAS_STATE(인물)·
  ESTABLISHED_IN(성립 Event)으로 잇는다 — 나이·신분·직급·학년·소속·능력·무공·부상·생사·소지품
  획득/상실·작품에 대한 역할 등. 사실이 바뀌면 기존 노드를 고치지 말고 새 노드를 만든다.
  CharacterState 노드를 하나 낼 때마다 relationships에 HAS_STATE와 ESTABLISHED_IN 항목을 반드시
  함께 넣는다 — 노드만 있고 관계가 빠지면 그 상태는 인물에서 도달할 수 없는 고아가 된다. 상태가
  많아 출력이 길어져도 이 두 관계는 상태마다 하나씩, 생략 없이 낸다.
- Character.description은 그 인물이 어떤 인물인지 설명하는 서술이며 CharacterState와 내용이 겹쳐도
  된다. 다만 인물에 관한 사실을 description에만 남기지 말고 반드시 CharacterState로도 만든다.
- Event.name·CharacterState.name에는 사건/상태를 그 자체로 읽히게 압축해 쓴다(예: '어깨를 칼날에
  깊게 베임', '화산파에 정식 입문', '코인 6200 보유', '스물여덟 살'). 이 노드만 따로 읽혔을 때도
  뜻이 통해야 한다 — 정황·경위·정도는 description이 담당하므로, name은 원문 문장을 그대로 옮기지
  말고 식별용으로 압축한다.
- 인물의 상태가 원문에 명시적으로 제시되면(서술이든 목록·표·공지 형태든) CharacterState로 만든다.
  원문이 값을 직접 확정해 준 것은 해석 여지가 없으므로 우선 추출 대상이다. 능력·스킬·특성·칭호·업적의
  발동/획득/달성이 형식화된 표기(시스템 메시지·상태창·공지 등)로 명시되면 각각 그 인물의 상태로
  빠짐없이 만든다 — 발동·획득 표기는 보유의 명시다.
- 한 인물의 여러 상태가 한자리에 열거되면 일부만 고르지 말고 열거된 항목을 빠짐없이 만든다.
- 이름 있는 소지품·물건(선물·첨부물 등 고유명이 없으면 '작가의 선물'처럼 지시적 이름으로)은 Item
  노드로 만들고, 소유는 CharacterState + ABOUT→Item으로 표현한다(이동 시 넘긴 인물과 받은 인물의
  상태를 각각 만든다).
- 작품·사물을 저작/제작/열독한 인물은 그 역할을 CharacterState(예: '탑의 문의 저자') + ABOUT→Item으로
  표현한다. 이런 역할은 사람-사람 관계가 아니므로 RELATED_TO로 묶지 말 것. 단, 이 역할과 그 사물/작품
  Item도 아래 '비중 필터'를 통과할 때만 만든다.
- 사건의 구체적 물리 공간은 Location + HOSTS로, 상위 장소는 LOCATED_IN으로 한 단계씩 잇는다. 댓글창·
  게시판·앱 화면 같은 온라인·가상 공간은 Location으로 만들지 않는다(실제 물리 공간만).
- 조직·세력·회사·부서는 Organization으로, 인물 소속은 CharacterState + ABOUT→Organization으로
  표현한다. 인물이 실제로 소속된 조직은 아래 '비중 필터'의 예외로, 지나가듯 언급돼도 만든다
  (누가 어디 소속인지는 그 자체로 추적 대상이다). '회사원'·'계약직'·'정직원' 같은 신분·고용형태는
  소속과 별개의 상태로 분리한다. 부서·지부처럼 더 큰 조직의 일부인 조직은 PART_OF로 상위 조직에
  한 단계씩 잇는다(부서→회사, 건너뛰기 금지 — Location의 LOCATED_IN과 같은 방식). 소속 상태의
  ABOUT은 가장 구체적인 조직(예: 회사가 아니라 그 안의 부서)을 가리킨다.
- 개별 이름 없이 집합적으로만 언급되는 세력·집단(관중 세력·후원 세력·군중 등)도, 단순 배경이 아니라
  사건의 진행이나 인물의 행동·운명에 실제로 개입하면 Organization으로 만든다(구성원이 무명이어도
  세력 자체가 서사에 개입하면 배경이 아니다). 그 집단의 개별 구성원이 이름·호칭을 갖고 행위 주체로
  등장하면 별도 Character로 만든다(집단 Organization 노드로 뭉개지 않는다).
- 인물↔인물의 서사적 관계(사제·동맹·적대·혈연·연인·동료 등)는 RELATED_TO로 잇고 종류를 type에
  담는다(단순 동반 등장만으로는 만들지 않음).
- 비중 필터(스스로 판단): 각 대상이 이 이야기에서 서사적으로 의미가 있는지 직접 판단해 결정한다.
  지나가는 행인이나 순전히 배경·분위기·농담으로만 스치고 이후 아무 역할이 없는 사물·작품·조직은 빼되,
  인물의 행동·상태·관계·사건에 얽혀 서사적으로 중요해 보이는 대상은 포함한다(중요해 보이면 포함하는
  쪽으로 판단한다). 그렇게 제외되는 대상은 거기 딸린 관계·상대 노드도 함께 뺀다.
- 과추출 금지: 제외 기준은 '변하는가'가 아니라 '지속되는가·서사적으로 의미가 있는가'다. 일시적
  통증·피로·긴장처럼 그 회차에서 소모되는 상태나, 배경 묘사로만 스치고 이후 아무 역할이 없는
  사실은 CharacterState로 만들지 않는다.
- 구조 완전성: description은 노드·관계를 원문에 근거해 설명하는 자리다. 노드/관계와 내용이
  겹쳐도 되나, 구조로 표현 가능한 사실(소속·신분·소유·역할·관계·장소·상태)이 description에만 남고
  해당 노드/관계로 나타나지 않아서는 안 된다.
- 소유·소속·역할 CharacterState는 대상(Item/Organization)을 같은 출력에서 ABOUT으로 함께 낸다.
- description: 각 Event·CharacterState에, name이 압축하며 버린 정황을 복원한다 — 누가 관여했는지,
  어떤 계기로, 어느 정도로, 원문이 결과를 어디까지 서술했는지. evidence_chunk가 가리키는 청크의
  원문에 근거해 쓰고 그 청크에 없는 인과·동기·감정은 지어내지 않으며, 고유명·수치·호칭은 원문
  표기 그대로 쓴다. name을 어미만 바꿔 되풀이하면 쓰지 않은 것과 같다 — 덧붙일 정황이 원문에
  없으면 근거 문장을 풀어 쓰는 데서 그친다(짧은 서술이 지어낸 서술보다 낫다). 인물의 예상·판단·감정을
  요약할 때 그 방향(긍정/부정, 생존/죽음, 성공/실패)을 원문 그대로 유지한다 — 완곡하게 바꾸다
  방향이 뒤집히면 사실 왜곡이다.
- 서술 귀속: '인물이 직접 말하거나 드러낸 것'과 '다른 인물이 속으로 알아본 것'을 구분해 쓴다.
  원문에서 발화되지 않은 이름·정체·사실을 그 인물이 스스로 밝힌 것처럼 쓰지 않는다(예: 계급만
  밝힌 인물의 이름을 '~라고 밝혔다'로 쓰지 말 것 — 이름은 알아본 쪽의 인지로 서술한다).
- evidence_chunk: 각 Event·CharacterState에, 그 사실의 근거가 되는 원문이 있는 청크 번호를
  채운다(예: "C3", 여럿이면 "C3,C4"). 실제 그 원문이 있는 청크만. description은 여기 적은 청크의
  원문에만 근거해야 하므로 실제 근거 청크를 빠짐없이 적는다.

--- 유효한 JSON 생성 규칙 ---
- JSON 외의 부가 설명·문장을 함께 반환하지 말라(JSON만 출력).
- JSON을 backtick(```)으로 감싸지 말라.
- 전체를 list로 감싸지 말라 — 최상위는 nodes/relationships를 가진 하나의 JSON 객체다.
- property 이름은 반드시 큰따옴표로 감싼다.

예시:
{examples}

--- 지금까지의 배경 컨텍스트(참고용) ---
아래는 이전 회차까지의 그래프 덤프와 줄거리 요약이다(첫 회차면 비어 있으니 무시).
- 별칭 정합: 이번 회차의 대상이 다른 호칭으로 불려도, 배경 그래프에 같은 대상이 있으면 그 name을
  그대로 써서 같은 노드로 추출한다(새 이름으로 분리 금지).
- 상태 갱신: 배경의 CharacterState가 이번 회차에 바뀌면 기존 노드를 고치지 말고 새 노드를 만든다.
  단, 배경에 이미 있는 상태가 이번 회차에도 그대로 유지되면(값이 안 바뀌면) 다시 만들지 않는다 —
  같은 사실을 회차마다 중복 생성하지 말고, 값이 실제로 바뀔 때만 새 노드를 낸다.
- 충돌 시 새 회차 원문을 우선한다 — 배경에 맞추려 사실을 왜곡하지 말 것(모순은 그대로 둬야 나중에 탐지된다).
{novel_context}

입력 텍스트:

{text}
"""
    EXPECTED_INPUTS = ["text"]

    def format(
        self,
        schema: dict[str, Any],
        examples: str,
        text: str = "",
        novel_context: str = "",
    ) -> str:
        """
        네 값(schema/examples/text/novel_context)을 모두 템플릿에 채워 렌더한다.

        부모 ERExtractionTemplate.format은 novel_context 인자를 받지 않으므로(그대로 위임하면
        TypeError) 조부모 PromptTemplate.format을 직접 호출해 네 값을 모두 전달한다.
        """
        return PromptTemplate.format(
            self,
            text=text,
            schema=schema,
            examples=examples,
            novel_context=novel_context,
        )


class NovelContextExtractor(LLMEntityRelationExtractor):
    """
    청크 추출 프롬프트에 누적 컨텍스트(novel_context)를 주입하는 extractor.

    KoreanWebNovelERTemplate의 {novel_context} placeholder는 라이브러리 run 경로가 채워주지
    않는다(라이브러리는 text/schema/examples만 넘긴다). 이 클래스가 그 빈칸을 인스턴스에
    저장해 둔 self.novel_context로 배선한다.
    """

    def __init__(self, *args: Any, novel_context: str = "", **kwargs: Any) -> None:
        # 나머지 인자(llm/prompt_template/use_structured_output/on_error 등)는 부모에 위임한다.
        super().__init__(*args, **kwargs)
        self.novel_context = novel_context

    @validate_call
    async def run(
        self,
        chunks: TextChunks,
        document_info: Optional[DocumentInfo] = None,
        lexical_graph_config: Optional[LexicalGraphConfig] = None,
        schema: Optional[GraphSchema] = None,
        examples: str = "",
        **kwargs: Any,
    ) -> Neo4jGraph:
        """
        부모 LLMEntityRelationExtractor.run에 그대로 위임한다.

        Component 메타클래스가 서브클래스 본문에 run 정의를 요구하고(상속만으로는 인정 안 함),
        run 시그니처에서 파이프라인 입력 파라미터(chunks/schema/examples 등)를 읽어 배선한다.
        따라서 부모와 동일한 시그니처를 재선언해 위임만 한다.
        """
        return await super().run(
            chunks,
            document_info=document_info,
            lexical_graph_config=lexical_graph_config,
            schema=schema,
            examples=examples,
            **kwargs,
        )

    async def extract_for_chunk(
        self, schema: GraphSchema, examples: str, chunk: TextChunk
    ) -> Neo4jGraph:
        """Run entity extraction for a given text chunk.

        neo4j_graphrag 1.18.0의 원본 메서드를 그대로 복제하되, prompt_template.format 호출에
        novel_context=self.novel_context 인자만 추가했다(V2/V1 분기 로직은 원본 그대로 유지).
        """
        prompt = self.prompt_template.format(
            text=chunk.text,
            schema=schema.model_dump(exclude_none=True),
            examples=examples,
            novel_context=self.novel_context,
        )

        # Use structured output (V2) if enabled
        if self.use_structured_output:
            # Capability check
            # This should never happen due to __init__ validation
            if not self.llm.supports_structured_output:
                raise RuntimeError(
                    f"Structured output is not supported by {type(self.llm).__name__}"
                )

            messages = [LLMMessage(role="user", content=prompt)]
            llm_result = await self.llm.ainvoke(messages, response_format=Neo4jGraph)  # type: ignore[call-arg, arg-type]
            try:
                chunk_graph = Neo4jGraph.model_validate_json(llm_result.content)
            except ValidationError as e:
                if self.on_error == OnError.RAISE:
                    raise LLMGenerationError("LLM response has improper format") from e
                logger.error(
                    f"LLM response has improper format for chunk_index={chunk.index}"
                )
                logger.debug(f"Invalid response: {llm_result.content}")
                chunk_graph = Neo4jGraph()
            return chunk_graph

        # Use V1 prompt-based JSON extraction (default)
        llm_result = await self.llm.ainvoke(prompt)
        try:
            llm_generated_json = fix_invalid_json(llm_result.content)
            result = json.loads(llm_generated_json)
        except (json.JSONDecodeError, InvalidJSONError) as e:
            if self.on_error == OnError.RAISE:
                raise LLMGenerationError("LLM response is not valid JSON") from e
            logger.error(
                f"LLM response is not valid JSON for chunk_index={chunk.index}"
            )
            logger.debug(f"Invalid JSON: {llm_result.content}")
            result = {"nodes": [], "relationships": []}
        try:
            chunk_graph = Neo4jGraph.model_validate(result)
        except ValidationError as e:
            if self.on_error == OnError.RAISE:
                raise LLMGenerationError("LLM response has improper format") from e
            logger.error(
                f"LLM response has improper format for chunk_index={chunk.index}"
            )
            logger.debug(f"Invalid JSON format: {result}")
            chunk_graph = Neo4jGraph()
        return chunk_graph
