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


# lorekeeper Tool.get_parameters()는 get_search_results 시그니처를 그대로 자동 추론하는데,
# 하이브리드 계열(hybrid_search/fact_search)은 query_vector·ranker·query_params 같은 내부
# 파라미터까지 다 노출하고(엉뚱한 인자를 채울 위험) query_text가 required에서 빠지며,
# entity_search의 up_to_chapter는 `from __future__ import annotations` 때문에 int|None 타입
# 추론이 깨져 string으로 나온다. 그래서 우리가 실제로 쓰길 원하는 파라미터만 여기서 직접
# 명시한다(도구 실행 자체는 그대로 lorekeeper의 Tool.execute를 쓴다 — 스키마만 우리 것으로 대체).
# **신규 도구를 추가하면 반드시 여기에도 등록해야 한다** — 빠지면 아래 폴백이 내부 파라미터를
# 그대로 LLM에 노출한다.
# 공개 이름이다 — 채팅 도구(src/service/chat/tools.py)도 같은 스키마를 쓴다. 정의가 두 벌이면
# retrieval 쪽 파라미터가 바뀔 때 채팅만 낡은 스키마로 남는다.
PARAMETER_SCHEMAS: dict[str, dict[str, Any]] = {
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
    parameters = PARAMETER_SCHEMAS.get(name) or tool.get_parameters() or {
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
