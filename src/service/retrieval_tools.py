"""lorekeeper의 retrieval 도구 4종을 OpenAI function-calling 스키마로 감싼다.

lorekeeper.build_retrieval_tools()가 반환하는 neo4j_graphrag.tool.Tool 객체를 실행기
(execute)로 그대로 쓰되, OpenAI Chat Completions의 `tools` 파라미터가 요구하는
{"type": "function", "function": {...}} 스키마로 변환해 claim 검증 에이전트(agent.py)에 넘긴다.
"""

from __future__ import annotations

from typing import Any

from src.common.tenant import Tenant
from src.repository.neo4j.retrieval import build_retrieval_tools
from neo4j_graphrag.tool import Tool
from neo4j_graphrag.types import RetrieverResult

# claim 검증 에이전트의 시스템 프롬프트에 그대로 삽입되는 도구 사용 가이드.
# 도구 자체의 name/description(lorekeeper 쪽 Tool.description)과 별개로, "claim 검증"이라는
# 용도에 한정해 "어떤 자연어를 넣어야 하는지"와 "언제 이 도구를 먼저 골라야 하는지"를
# 더 구체적으로 안내한다.
TOOL_GUIDE = """\
세 도구는 **서로 다른 것에 앵커한다**. claim의 어느 자리가 의심스러운지에 따라 고른다.

1. entity_search(entity_name, up_to_chapter=null)
   - 무엇: 인물·아이템·조직·장소를 이름 또는 별칭으로 정확 조회한다. 별칭·설명 같은 속성과
     상태(신분·소속·능력·부상·생사·소유·역할) 이력을 성립 회차 순으로, 참여 사건·근거 원문과
     함께 반환한다. 자연어 질문이 아니라 "이름" 하나만 받는다.
   - 입력: claim.entities 중 이름 하나(정확한 이름 또는 별칭). 자유 문장을 넣지 말 것.
   - 특징: 검색이 아니라 정형 조회라 결과가 정확하고 이력이 한 번에 다 나온다. 임베딩
     호출이 없어 가장 빠르고 저렴하다. 별칭이 그래프에 있으면 그것으로도 찾아진다.
   - 언제: "그 대상의 속성이 무엇인가"를 묻는 claim이면 가장 먼저 시도한다. 예: "A의 소속은
     경찰이다", "B는 스물다섯이다", "C가 죽었다".

2. fact_search(query_text, top_k=5)
   - 무엇: 사건·인물상태를 의미로 검색해 **참가자 목록**과 근거 원문을 반환한다.
   - 입력: 행위·목적어 중심의 짧은 서술. 예: "도깨비에게 수표를 내민 사람", "칸 출입 제한".
   - 특징: 참가자 목록이 닫힌 집합이라 **"그 목록에 없다"는 부재 증명**이 가능하다. 이게
     유사도 검색으로는 원리상 안 되는 일이다.
   - 언제: **주어가 의심스러울 때 주어를 빼고 행위로 묻는다.** "X가 그 일을 했다"는 claim은
     X로 찾으면 안 되고 "그 일을 한 사람"으로 찾아야 한다. 규칙·제약("~해도 되는가")도
     인물 이름을 빼고 규칙 자체로 묻는다.

3. hybrid_search(query_text, top_k=5)
   - 무엇: 벡터 검색과 풀텍스트 검색을 결합해 **원문 조각**을 찾아 반환한다.
   - 입력: 확인하려는 표현을 그대로. 고유명사·숫자·표기를 포함시키는 게 유리하다.
   - 특징: 원문 표현에 앵커하므로 "그 표기가 원문에 있는가"를 가장 잘 확인한다. 반대로
     문장이 길고 여러 주제가 섞이면 판별력이 떨어진다 — 짧게 끊어서 물을 것.
   - 언제: 표기·숫자·식별자·명칭처럼 어휘 그대로 대조해야 하는 claim. 위 둘이 빈약한
     결과를 줬을 때의 보완용으로도 좋다.
"""


# lorekeeper Tool.get_parameters()는 get_search_results 시그니처를 그대로 자동 추론하는데,
# 하이브리드 계열(hybrid_search/fact_search)은 query_vector·ranker·query_params 같은 내부
# 파라미터까지 다 노출하고(엉뚱한 인자를 채울 위험) query_text가 required에서 빠지며,
# entity_search의 up_to_chapter는 `from __future__ import annotations` 때문에 int|None 타입
# 추론이 깨져 string으로 나온다. 그래서 우리가 실제로 쓰길 원하는 파라미터만 여기서 직접
# 명시한다(도구 실행 자체는 그대로 lorekeeper의 Tool.execute를 쓴다 — 스키마만 우리 것으로 대체).
# **신규 도구를 추가하면 반드시 여기에도 등록해야 한다** — 빠지면 아래 폴백이 내부 파라미터를
# 그대로 LLM에 노출한다.
_PARAMETER_SCHEMAS: dict[str, dict[str, Any]] = {
    "hybrid_search": {
        "type": "object",
        "properties": {
            "query_text": {"type": "string", "description": "검색할 자연어 질의(키워드 포함)."},
            "top_k": {"type": "integer", "description": "반환할 상위 결과 개수(기본 5)."},
        },
        "required": ["query_text"],
    },
    "fact_search": {
        "type": "object",
        "properties": {
            "query_text": {
                "type": "string",
                "description": "찾을 사건·상태를 서술한 자연어 질의(행위·목적어 중심).",
            },
            "top_k": {"type": "integer", "description": "반환할 상위 결과 개수(기본 5)."},
        },
        "required": ["query_text"],
    },
    "entity_search": {
        "type": "object",
        "properties": {
            "entity_name": {
                "type": "string",
                "description": "조회할 대상(인물·아이템·조직·장소)의 이름 또는 별칭.",
            },
            "up_to_chapter": {
                "type": "integer",
                "description": "이 회차까지 성립한 사실만 조회한다(생략 시 전체 이력).",
            },
        },
        "required": ["entity_name"],
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


def build_openai_tools(
    tenant: Tenant,
    include_graph: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Tool]]:
    """lorekeeper 도구 3종을 (OpenAI tools 스키마 리스트, 이름→Tool dict)로 반환한다.

    include_graph=True면 검색 결과 뒤에 `[관련 그래프]` 부록이 붙는다. 기본은 끔 —
    판정에 필요한 관계는 이미 본문에 흡수돼 있는데(참가자 목록·근거 원문·[관련인물]/[상위])
    부록은 같은 인물 설명이 결과마다 반복돼 근거 토큰의 2/3를 차지했다. 도달률은 불변이었다.
    """
    tools = build_retrieval_tools(tenant, include_graph)
    schemas = [_to_openai_schema(tool) for tool in tools]
    by_name = {tool.get_name(): tool for tool in tools}
    return schemas, by_name


def format_tool_result(result: RetrieverResult) -> str:
    """RetrieverResult를 검증 에이전트에게 다시 보여줄 텍스트로 직렬화한다."""
    if not result.items:
        return "검색 결과 없음."
    return "\n\n".join(f"[결과 {i}]\n{item.content}" for i, item in enumerate(result.items, 1))
