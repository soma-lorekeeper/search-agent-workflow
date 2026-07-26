"""lorekeeper의 retrieval 도구 4종을 OpenAI function-calling 스키마로 감싼다.

lorekeeper.build_retrieval_tools()가 반환하는 neo4j_graphrag.tool.Tool 객체를 실행기
(execute)로 그대로 쓰되, OpenAI Chat Completions의 `tools` 파라미터가 요구하는
{"type": "function", "function": {...}} 스키마로 변환해 claim 검증 에이전트(agent.py)에 넘긴다.
"""

from __future__ import annotations

from typing import Any

from lorekeeper import build_retrieval_tools
from neo4j_graphrag.tool import Tool
from neo4j_graphrag.types import RetrieverResult

# claim 검증 에이전트의 시스템 프롬프트에 그대로 삽입되는 도구 사용 가이드.
# 도구 자체의 name/description(lorekeeper 쪽 Tool.description)과 별개로, "claim 검증"이라는
# 용도에 한정해 "어떤 자연어를 넣어야 하는지"와 "언제 이 도구를 먼저 골라야 하는지"를
# 더 구체적으로 안내한다.
TOOL_GUIDE = """\
1. vector_cypher_search(query_text, top_k=5)
   - 무엇: 질문과 의미가 가까운 원문 조각을 벡터 검색으로 찾고, 근거가 된 사건·상태와
     주변 그래프(1-hop)까지 함께 반환한다.
   - 입력: claim의 quote를 거의 그대로, 또는 핵심 사실만 추린 자연어 한 문장. 고유명사가
     없어도 된다.
   - 특징: 의미 유사도 기반이라 표현이 달라도(동의어, 다른 어투) 잘 찾는다. 다만 흔치 않은
     고유명사나 짧은 이름은 놓칠 수 있다.
   - 언제: 일반적인 "무슨 일이 있었는지 / 어떤 상태였는지"류 질문. 첫 시도로 무난하다.

2. hybrid_search(query_text, top_k=5)
   - 무엇: vector_cypher_search와 같은 그래프 확장 로직에, 벡터 검색 + 풀텍스트(키워드)
     검색을 더해 합친다.
   - 입력: vector_cypher_search와 비슷하되, 인물명·아이템명·장소명 등 고유명사를 반드시
     포함시키는 게 유리하다.
   - 특징: 고유명사·정확한 키워드가 있는 질문에서 vector_cypher_search보다 안정적이다.
     벡터 검색 결과가 빈약했을 때 재시도용으로도 좋다.
   - 언제: claim에 특정 인물·아이템·장소 이름이 명시된 경우, 또는 vector_cypher_search가
     빈약한 결과를 줬을 때.

3. entity_state_history(entity_name, up_to_chapter=null)
   - 무엇: 특정 인물의 상태(신분·소속·능력·부상·생사·소유·역할) 변화를 성립 회차 순으로
     전부 조회한다. 자연어 질문이 아니라 "인물 이름" 하나만 받는다.
   - 입력: claim.entities 중 인물 이름 하나(정확한 이름 또는 별칭). 자유 문장을 넣지 말 것.
   - 특징: 검색이 아니라 정형 조회라 결과가 정확하고 시간순 이력이 한 번에 다 나온다.
     LLM·임베딩 호출이 없어 가장 빠르고 저렴하다.
   - 언제: claim의 category가 생사·소유물·능력·관계·소속처럼 "인물의 상태"를 다루면
     가장 먼저 시도한다. 예: "A가 죽었다", "B가 여전히 OO를 갖고 있다".

4. text2cypher_search(query_text)
   - 무엇: 자연어 질문을 LLM이 Cypher로 직접 번역해 실행한다. 결과 컬럼이 질문마다
     달라진다(고정 포맷 없음).
   - 입력: 집계·정렬·조건이 들어간 자연어 질문. 예: "3화 이후 발생한 사건 수", "OO 조직
     소속 인물 목록".
   - 특징: 위 셋과 달리 원문 발췌 없이 "사실만" 구조화해서 준다. 생성된 쿼리가 스키마와
     안 맞으면 결과가 비거나 틀릴 수 있다(넷 중 가장 불안정).
   - 언제: 시점/순서 category처럼 "몇 개 / 언제부터 / 누가 다"류 집계·조건 질문. 다른
     도구로 안 풀릴 때의 최후 수단으로도 유용하다.
"""


# lorekeeper Tool.get_parameters()는 get_search_results 시그니처를 그대로 자동 추론하는데,
# vector_cypher_search/hybrid_search는 query_vector·filters·query_params 같은 내부 파라미터까지
# 다 노출하고(엉뚱한 인자를 채울 위험) query_text가 required에서 빠지며, entity_state_history의
# up_to_chapter는 int|None 타입 추론이 깨져 string으로 나온다. lorekeeper-poc 코드를 고치는 대신,
# 우리가 실제로 쓰길 원하는 파라미터만 우리 쪽에서 직접 명시한다(도구 실행 자체는 그대로
# lorekeeper의 Tool.execute를 쓴다 — 스키마만 우리 것으로 대체).
_PARAMETER_SCHEMAS: dict[str, dict[str, Any]] = {
    "vector_cypher_search": {
        "type": "object",
        "properties": {
            "query_text": {
                "type": "string",
                "description": "검색할 자연어 질의(원문·사건·인물에 대한 질문).",
            },
            "top_k": {"type": "integer", "description": "반환할 상위 결과 개수(기본 5)."},
        },
        "required": ["query_text"],
    },
    "hybrid_search": {
        "type": "object",
        "properties": {
            "query_text": {"type": "string", "description": "검색할 자연어 질의(키워드 포함)."},
            "top_k": {"type": "integer", "description": "반환할 상위 결과 개수(기본 5)."},
        },
        "required": ["query_text"],
    },
    "entity_state_history": {
        "type": "object",
        "properties": {
            "entity_name": {"type": "string", "description": "조회할 인물의 이름 또는 별칭."},
            "up_to_chapter": {
                "type": "integer",
                "description": "이 회차까지 성립한 상태만 조회한다(생략 시 전체 이력).",
            },
        },
        "required": ["entity_name"],
    },
    "text2cypher_search": {
        "type": "object",
        "properties": {
            "query_text": {"type": "string", "description": "그래프에서 답을 찾을 자연어 질의."},
        },
        "required": ["query_text"],
    },
}


def _to_openai_schema(tool: Tool) -> dict[str, Any]:
    name = tool.get_name()
    # 새 도구가 추가돼 목록에 없으면, lorekeeper의 자동 추론 스키마로 폴백한다.
    parameters = _PARAMETER_SCHEMAS.get(name) or tool.get_parameters() or {
        "type": "object",
        "properties": {},
    }
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": tool.get_description(),
            "parameters": parameters,
        },
    }


def build_openai_tools() -> tuple[list[dict[str, Any]], dict[str, Tool]]:
    """lorekeeper 도구 4종을 (OpenAI tools 스키마 리스트, 이름→Tool dict)로 반환한다."""
    tools = build_retrieval_tools()
    schemas = [_to_openai_schema(tool) for tool in tools]
    by_name = {tool.get_name(): tool for tool in tools}
    return schemas, by_name


def format_tool_result(result: RetrieverResult) -> str:
    """RetrieverResult를 검증 에이전트에게 다시 보여줄 텍스트로 직렬화한다."""
    if not result.items:
        return "검색 결과 없음."
    return "\n\n".join(f"[결과 {i}]\n{item.content}" for i, item in enumerate(result.items, 1))
