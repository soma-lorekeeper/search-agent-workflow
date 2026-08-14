"""채팅 에이전트가 쓰는 도구 6종.

KG 4종은 lorekeeper의 retriever를 그대로 실행기로 쓰고(src/contradiction/tools.py와 같은
방식으로 OpenAI function-calling 스키마만 우리가 직접 명시한다), 나머지 2종은 PostgreSQL의
원고/작품 테이블을 직접 읽는다. KG에는 요약·사실만 들어 있어 "16화 그 장면 원문 그대로"를
답할 수 없으므로, 원고 조회는 그래프가 아니라 원본 DB에서 가져와야 한다.

contradiction 쪽 도구와 두 가지가 다르다.
  1. 모든 도구가 (user_id, work_id)를 앞 두 인자로 받는다 — KG 도구는 그 둘로 테넌트를
     해소해 그래프를 좁히고, PostgreSQL 도구는 work_id로 조회한다(그쪽 테이블의 소유권
     검사는 Spring이 요청 전에 끝낸다). 인자 모양을 통일해 두면 에이전트의 주입 코드가
     도구마다 갈라지지 않는다.
  2. 실행 결과와 함께 "화면에 보여줄 한 줄 요약"을 돌려준다 — 채팅 UI가 답변 위에
     "무엇을 찾아봤는지"를 표시해야 작가가 근거의 출처를 납득할 수 있다.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Callable

import psycopg
from src.common.tenant import Tenant
from src.repository.neo4j.retrieval import build_retrieval_tools
from neo4j_graphrag.tool import Tool

from src import config  # noqa: F401 — import 시점에 .env를 로드해 DATABASE_URL을 환경변수로 채운다
from src.service.kg_scope import kg_scope
from src.contradiction.tools import format_tool_result

logger = logging.getLogger("chat.tools")

# 원고 전문을 그대로 넣으면 회차 하나로 컨텍스트가 꽉 차서 이어지는 대화가 망가진다.
# 작가는 보통 "그 장면이 어떻게 쓰였는지" 앞부분만 필요로 하므로 앞에서부터 잘라 쓴다.
MANUSCRIPT_MAX_CHARS = 4000

# 표시용 요약에 넣을 질의 길이 상한. 한 줄로 보여줄 뿐이라 길면 UI가 깨진다.
_SUMMARY_QUERY_CHARS = 24


# 채팅 에이전트의 시스템 프롬프트에 그대로 삽입되는 도구 가이드. 도구 자체의 description과
# 별개로, "작가와의 대화"라는 용도에서 어떤 질문에 어떤 도구를 먼저 골라야 하는지를 적는다.
TOOL_GUIDE = """\
1. kg_hybrid_search(query_text, top_k=5)
   - 무엇: 질문과 의미가 가까운 원문 조각을 벡터+키워드로 찾고, 근거가 된 사건·상태와
     주변 그래프까지 함께 반환한다.
   - 언제: "이런 일이 있었나?", "이 인물은 어떤 상황이었지?" 같은 일반 질문. 무엇부터
     부를지 애매하면 이걸 먼저 부른다. 고유명사가 또렷하면 질의에 반드시 넣어라.

2. kg_fact_search(query_text, top_k=5)
   - 무엇: 원문이 아니라 **정제된 사실**(사건·인물 상태)을 검색한다. 각 사실에는 참가자와
     근거 원문이 함께 딸려온다.
   - 언제: "누가 무엇을 했나", "그때 상태가 어땠나"처럼 사건·상태 자체가 답인 질문.
     원문 조각보다 신호가 정제돼 있어 설정 확인에 유리하다.

3. kg_entity_search(entity_name, up_to_chapter=null)
   - 무엇: 인물·아이템·조직·장소 하나를 이름/별칭으로 정확 조회해 프로필과 관련 사실을
     성립 회차 순으로 낸다. 자연어 문장이 아니라 "이름" 하나만 받는다.
   - 언제: "이 인물 지금 어떤 상태지?", "언제 이렇게 됐지?", "설정 정리해줘"처럼 대상이
     특정된 질문. 검색이 아니라 정형 조회라 가장 정확하고, 닫힌 집합을 돌려주므로
     "그 목록에 없다"는 부재 증명도 된다.

4. episode_manuscript(episode_number)
   - 무엇: 해당 회차의 제목과 원고 본문(앞부분)을 원본 DB에서 그대로 가져온다.
   - 언제: "16화에서 그 장면 어떻게 썼더라?", "직전 화 마지막이 어땠지?"처럼 요약이 아니라
     실제 문장이 필요할 때. KG는 사실만 갖고 있어 원문 표현·문체는 여기서만 확인된다.
     사용자가 "이번 화/직전 화"처럼 상대적으로 말하면 [회차 컨텍스트]를 기준으로 계산해라.
   - 주의: **집필 중인 회차에는 쓰지 마라.** 그 회차의 원고 전문은 [회차 컨텍스트]에 이미
     들어 있고, 이 도구는 앞부분만 잘라서 준다.

5. work_settings()
   - 무엇: 작품의 기본 정보(제목)를 가져온다.
   - 언제: 작품 자체를 확인해야 할 때. 세계관 설정 문서는 아직 저장되는 곳이 없으니, 세계관을
     물으면 이 도구가 아니라 KG 검색으로 답해야 한다.
"""


# ---------- lorekeeper KG 도구 ----------

# build_retrieval_tools()는 Neo4j 드라이버·임베더·text2cypher용 스키마 조회까지 준비해서
# 수백 ms가 든다. 채팅은 요청마다 도구 목록이 필요하므로 프로세스당 한 번만 만들어 재사용한다
# (lorekeeper 내부도 드라이버·임베더를 모듈 싱글턴으로 캐시하므로 중복 생성이 없다).
# 테넌트 id → (도구 이름 → Tool). retriever가 테넌트를 생성 시점에 굳혀 들고 있어
# 캐시도 테넌트별이어야 한다.
_LOREKEEPER_TOOLS: dict[str, dict[str, Tool]] = {}


def _lorekeeper_tool(tenant: Tenant, name: str) -> Tool:
    """테넌트별 retriever 묶음을 만들어 캐시한다.

    retriever는 테넌트를 생성 시점에 굳혀 들고 있으므로(검색 쿼리에 그 값이 박힌다)
    프로세스 하나가 여러 소설을 다루려면 캐시도 테넌트별이어야 한다.
    """
    tools = _LOREKEEPER_TOOLS.get(tenant.id)
    if tools is None:
        tools = {tool.get_name(): tool for tool in build_retrieval_tools(tenant)}
        _LOREKEEPER_TOOLS[tenant.id] = tools
    return tools[name]


def _short(text: str) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= _SUMMARY_QUERY_CHARS else text[:_SUMMARY_QUERY_CHARS] + "…"


def _kg_hybrid_search(
    user_id: int, work_id: int, query_text: str, top_k: int = 5
) -> tuple[str, str]:
    tenant = kg_scope(user_id, work_id)
    result = _lorekeeper_tool(tenant, "hybrid_search").execute(query_text=query_text, top_k=top_k)
    return format_tool_result(result), f"KG 검색 · «{_short(query_text)}»"


def _kg_fact_search(
    user_id: int, work_id: int, query_text: str, top_k: int = 5
) -> tuple[str, str]:
    tenant = kg_scope(user_id, work_id)
    result = _lorekeeper_tool(tenant, "fact_search").execute(query_text=query_text, top_k=top_k)
    return format_tool_result(result), f"KG 사실 검색 · «{_short(query_text)}»"


def _kg_entity_search(
    user_id: int, work_id: int, entity_name: str, up_to_chapter: int | None = None
) -> tuple[str, str]:
    tenant = kg_scope(user_id, work_id)
    result = _lorekeeper_tool(tenant, "entity_search").execute(
        entity_name=entity_name, up_to_chapter=up_to_chapter
    )
    upto = f" (EP.{up_to_chapter:03d}까지)" if up_to_chapter else ""
    return format_tool_result(result), f"KG 조회 · «{_short(entity_name)}»{upto}"


# ---------- PostgreSQL 원고/작품 도구 ----------


def _connect() -> psycopg.Connection:
    """원고 DB 커넥션. 채팅 도구 호출은 드물고 짧아서 풀 없이 매번 새로 연다 — 대신
    connect_timeout을 둬서 DB가 죽었을 때 대화 전체가 멈추지 않게 한다(src/health.py와 동일)."""
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL이 설정되지 않아 원고 DB를 읽을 수 없다.")
    return psycopg.connect(url, connect_timeout=5)


def _episode_manuscript(user_id: int, work_id: int, episode_number: int) -> tuple[str, str]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "select episode_number, title, content from episodes "
            "where work_id=%s and episode_number=%s",
            (work_id, episode_number),
        )
        row = cur.fetchone()

    summary = f"회차 원고 조회 · EP.{episode_number:03d}"
    if row is None:
        return f"{episode_number}화 원고가 아직 저장되어 있지 않습니다.", summary

    number, title, content = row
    content = content or ""
    body = content[:MANUSCRIPT_MAX_CHARS]
    # 잘렸다는 사실을 모델에게 명시한다 — 안 그러면 "이 화는 여기서 끝난다"고 단정해버린다.
    cut = (
        f"\n\n(…이하 생략 — 전체 {len(content)}자 중 앞 {MANUSCRIPT_MAX_CHARS}자만 표시)"
        if len(content) > MANUSCRIPT_MAX_CHARS
        else ""
    )
    return f"[{number}화] {title or '(제목 없음)'}\n\n{body}{cut}", summary


def _work_settings(user_id: int, work_id: int) -> tuple[str, str]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("select title from works where id=%s", (work_id,))
        row = cur.fetchone()

    if row is None:
        return f"work_id={work_id} 작품 정보를 찾을 수 없습니다.", "작품 정보 조회 · 없음"

    title = row[0] or "(제목 없음)"
    text = (
        f"작품 제목: {title}\n"
        "참고: 이 작품의 세계관·설정 문서를 따로 저장하는 곳이 아직 없다. "
        "설정에 관한 질문은 KG 검색 도구로 회차 원문에서 확인해야 한다."
    )
    return text, f"작품 정보 조회 · «{_short(title)}»"


# ---------- 도구 레지스트리 ----------


@dataclass(frozen=True)
class ChatTool:
    """(모델에게 보여줄 스키마) + (실제 실행기) 한 쌍.

    run은 (모델에게 돌려줄 결과 텍스트, 화면에 보여줄 한 줄 요약)을 반환한다.
    """

    name: str
    description: str
    parameters: dict[str, Any]
    run: Callable[..., tuple[str, str]]


# work_id는 일부러 스키마에서 뺐다. 조회 대상 작품은 서버가 요청에서 받아 정하는 값이지
# 모델이 고를 값이 아니다 — 스키마에 넣으면 모델이 엉뚱한 작품 번호를 지어내 남의 작품을
# 읽으려 드는 경로가 열린다. 에이전트가 실행 시점에 직접 주입한다(agent.py).
_TOOLS: tuple[ChatTool, ...] = (
    ChatTool(
        name="kg_hybrid_search",
        description=(
            "벡터 검색과 풀텍스트 검색을 결합해 원문 조각과 관련 그래프를 반환한다. "
            "인물명·아이템명·장소명 같은 고유명사가 들어간 질문에 유리하다."
        ),
        parameters={
            "type": "object",
            "properties": {
                "query_text": {
                    "type": "string",
                    "description": "검색할 자연어 질의(고유명사를 반드시 포함).",
                },
                "top_k": {"type": "integer", "description": "반환할 상위 결과 개수(기본 5)."},
            },
            "required": ["query_text"],
        },
        run=_kg_hybrid_search,
    ),
    ChatTool(
        name="kg_fact_search",
        description=(
            "원문이 아니라 정제된 사실(사건·인물 상태)을 검색한다. 각 사실에 참가자와 근거 "
            "원문이 함께 딸려온다. 사건·상태 자체가 답인 질문에 쓴다."
        ),
        parameters={
            "type": "object",
            "properties": {
                "query_text": {"type": "string", "description": "검색할 자연어 질의."},
                "top_k": {"type": "integer", "description": "반환할 상위 결과 개수(기본 5)."},
            },
            "required": ["query_text"],
        },
        run=_kg_fact_search,
    ),
    ChatTool(
        name="kg_entity_search",
        description=(
            "인물·아이템·조직·장소 하나를 이름/별칭으로 정확 조회해 프로필과 관련 사실을 "
            "성립 회차 순으로 낸다. 이름 하나만 받는다."
        ),
        parameters={
            "type": "object",
            "properties": {
                "entity_name": {"type": "string", "description": "조회할 대상의 이름 또는 별칭."},
                "up_to_chapter": {
                    "type": "integer",
                    "description": "이 회차까지 성립한 사실만 조회한다(생략 시 전체 이력).",
                },
            },
            "required": ["entity_name"],
        },
        run=_kg_entity_search,
    ),
    ChatTool(
        name="episode_manuscript",
        description=(
            "해당 회차의 제목과 원고 본문(앞부분)을 원본 DB에서 그대로 가져온다. 요약이 아니라 "
            "실제로 쓰인 문장·표현을 확인해야 할 때 쓴다."
        ),
        parameters={
            "type": "object",
            "properties": {
                "episode_number": {"type": "integer", "description": "조회할 회차 번호."},
            },
            "required": ["episode_number"],
        },
        run=_episode_manuscript,
    ),
    ChatTool(
        name="work_settings",
        description="작품의 기본 정보(제목)를 가져온다. 세계관 설정 문서는 아직 저장되지 않는다.",
        parameters={"type": "object", "properties": {}},
        run=_work_settings,
    ),
)


def build_chat_tools() -> tuple[list[dict[str, Any]], dict[str, ChatTool]]:
    """채팅 도구 6종을 (OpenAI tools 스키마 리스트, 이름→ChatTool dict)로 반환한다."""
    schemas = [
        {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
            },
        }
        for tool in _TOOLS
    ]
    return schemas, {tool.name: tool for tool in _TOOLS}
