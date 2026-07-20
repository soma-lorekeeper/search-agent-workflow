import json
import logging
from typing import Annotated, TypedDict

from langchain_core.messages import AIMessage, BaseMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from src import mysqldb
from src.config import OPENAI_API_KEY, OPENAI_MODEL
from src.tools import graph_query

MAX_TOOL_ROUNDS = 3  # grade가 "불충분"이라고 판단해도 이 횟수까지만 재검색 허용 (비용 상한)

logger = logging.getLogger("agent.loop")

TOOLS = [graph_query]

AGENT_SYSTEM_PROMPT = """\
너는 소설 세계관에 대해 질문에 답하는 범용 어시스턴트다. 특정 질문 목록에 맞춰진 것이 아니라,
독자가 이 소설 내용에 대해 무엇을 묻든 정확하게 답해야 한다.

행동 지침:
- 관계/소유/사건 시점/능력치처럼 구조화된 사실, '~한 적이 있는가'류 존재 여부 확인, 아이템이나
  사건이 여러 인물을 거쳐 이동/연쇄되는 다단계 추적 질문(그래프 순회가 필요한 경우)에는
  graph_query를 사용하라. 필요하면 여러 번 다른 표현으로 호출해도 된다.
- 사용자의 질문에 "이 사람", "그 캐릭터", "그 물건"처럼 지시어가 있으면, 대화 기록에서 그것이
  가리키는 구체적인 이름을 스스로 파악한 뒤 그 이름으로 도구를 호출하라.
- 도구 결과에 없는 내용은 추측하거나 지어내지 마라. 근거가 없으면 "원문/DB에서 확인되지
  않았다"고 명확히 밝혀라. 그럴듯해 보이는 정보라도 실제로 검색된 근거가 없으면 단정하지 마라.
- 가능하면 몇 화(episode)에서 확인된 사실인지 함께 밝혀라.
- "최종적으로/그 이후/결국 어떻게 됐는지"를 묻는 질문은 첫 검색 결과가 끝이 아닐 수 있다.
  검색 결과에 등장한 아이템/인물/사건이 다른 이름으로 바뀌거나(예: 업그레이드, 진화, 개명,
  파괴 후 대체) 상태가 달라졌을 가능성을 의심하고, 결과에 새로 등장한 이름으로 한 번 더
  검색해 정말 그게 끝인지 확인한 뒤 답하라.
- "비슷한 패턴을 다시 썼는지" 같은 반복 여부를 묻는 질문은 한 번의 검색으로 결론 내리지
  말고, 표현을 바꿔(동의어, 더 구체적인 사건 묘사) 최소 한 번 더 graph_query를 호출한 뒤에
  "없음"이라고 답하라. 인물의 반복 행동 패턴(예: 죽이지 않고 제압하기)을 찾을 때는 그 인물이
  주어인 사실들을 폭넓게 조회해 predicate와 note를 직접 검토하라 (예: defeated/제압 계열
  predicate의 note에 죽였는지 기절시켰는지가 적혀 있을 수 있다).
- 최종 답변은 한국어로, 간결하고 정확하게.
"""

GRADE_SYSTEM_PROMPT = """\
방금 도구 호출 결과가 사용자의 마지막 질문에 답하기에 충분한 근거인지 판단하라.
질문이 "최종적으로/그 이후/결국"을 묻는데 검색 결과가 중간 상태(예: 손상됨, 사라짐,
보류 상태)에서 멈춰 있고 그 뒤에 무슨 일이 있었는지가 없다면, 아직 불충분하다고 판단하라.
질문이 "다시 그런 적 있는지" 같은 반복 여부를 묻는데 한 번의 검색만으로 "없다"고 결론
내렸다면, 다른 표현으로 최소 한 번 더 검색해봤는지 의심하고 불충분하다고 판단하라.
대화 맥락(시스템 프롬프트)에 안내된 "인덱싱된 화별 요약"의 범위를 벗어나는 정보(그
범위 이후의 전개, 결말 등)를 묻는 질문이라면, 도구가 못 찾은 게 아니라 애초에 존재하지
않는 정보다 — 이런 경우는 재검색을 요구하지 말고 충분함으로 판단해서 "정보 없음"으로
바로 답하게 하라.
JSON으로만 답하라: {"sufficient": true|false, "hint": "불충분하다면 다음에 무엇을 다르게
검색해야 하는지에 대한 한 문장 제안, 충분하다면 빈 문자열"}
"""


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    tool_rounds: int


_llm = ChatOpenAI(model=OPENAI_MODEL, api_key=OPENAI_API_KEY)
_llm_with_tools = _llm.bind_tools(TOOLS)

_episode_summaries_block_cache: str | None = None


def _episode_summaries_block() -> str:
    """인덱싱된 화별 요약을 시스템 프롬프트에 항상 주입한다.
    - 거시적 질문("전체 줄거리 요약해줘")에 검색 없이 바로, 근거 있게 답할 수 있게 하고,
    - DB가 다루는 화 범위를 명시해 그 밖의 내용(예: 결말)을 사전학습 지식으로
      지어내는 할루시네이션을 막는다. 화당 요약 몇 줄 수준이라 토큰 비용은 무시할 만하다."""
    global _episode_summaries_block_cache
    if _episode_summaries_block_cache is None:
        try:
            rows = mysqldb.get_all_episode_summaries()
        except Exception:
            rows = []
        if rows:
            episodes = [r["episode"] for r in rows]
            lines = [f"- {r['episode']}화: {r['summary']}" for r in rows]
            _episode_summaries_block_cache = (
                f"[인덱싱된 화별 요약 — 이 DB는 {min(episodes)}~{max(episodes)}화까지만 다룬다. "
                "이 범위를 벗어나는 내용(그 이후 전개, 결말 등)을 묻는 질문에는 절대 사전학습된 "
                "지식으로 채우지 말고, 인덱싱 범위 밖이라 확인할 수 없다고 명확히 밝혀라.]\n"
                + "\n".join(lines)
            )
        else:
            _episode_summaries_block_cache = ""
    return _episode_summaries_block_cache


def _system_prompt() -> str:
    block = _episode_summaries_block()
    return AGENT_SYSTEM_PROMPT + (f"\n\n{block}" if block else "")


def agent_node(state: AgentState) -> dict:
    round_no = state.get("tool_rounds", 0)
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages = [SystemMessage(content=_system_prompt()), *messages]
    response = _llm_with_tools.invoke(messages)

    if response.tool_calls:
        calls = [(c["name"], c["args"]) for c in response.tool_calls]
        logger.info("[round %d] agent -> tool 호출 결정 | %s", round_no, calls)
    else:
        preview = (response.content or "")[:80].replace("\n", " ")
        logger.info("[round %d] agent -> 최종 답변 (도구 호출 없음) | %r...", round_no, preview)

    return {"messages": [response]}


tools_node = ToolNode(TOOLS)


def grade_node(state: AgentState) -> dict:
    tool_rounds = state.get("tool_rounds", 0) + 1

    grade_response = _llm.invoke(
        [SystemMessage(content=GRADE_SYSTEM_PROMPT), *state["messages"]]
    )
    try:
        parsed = json.loads(grade_response.content)
    except (json.JSONDecodeError, TypeError):
        parsed = {"sufficient": True, "hint": ""}

    extra_messages: list[BaseMessage] = []
    if not parsed.get("sufficient", True) and parsed.get("hint"):
        logger.info(
            "[round %d] grade -> 불충분, 재검색 힌트: %s", tool_rounds, parsed["hint"]
        )
        extra_messages.append(
            SystemMessage(content=f"[검색 결과 평가] 아직 불충분함. 힌트: {parsed['hint']}")
        )
    else:
        logger.info("[round %d] grade -> 충분함", tool_rounds)
    return {"tool_rounds": tool_rounds, "messages": extra_messages}


def final_answer_node(state: AgentState) -> dict:
    """도구 호출 한도 도달 시 강제로 호출된다. tools를 bind하지 않은 LLM을 쓰므로
    이 노드는 절대 tool_calls를 만들어낼 수 없다 — 그래프 종료를 물리적으로 보장한다."""
    logger.warning("도구 호출 한도(%d라운드) 도달 -> 강제 최종 답변", MAX_TOOL_ROUNDS)
    history = list(state["messages"])
    if history and isinstance(history[-1], AIMessage) and history[-1].tool_calls:
        history = history[:-1]  # 응답 못 받은 tool_calls는 다음 호출에 포함하면 API 오류 발생

    messages = [
        SystemMessage(
            content=_system_prompt()
            + "\n\n[시스템] 도구 호출 한도에 도달했다. 더 이상 도구를 호출할 수 없다. "
            "지금까지 얻은 정보만으로 최종 답변을 작성하거나, 확인할 수 없다고 명확히 밝혀라."
        ),
        *history,
    ]
    response = _llm.invoke(messages)
    return {"messages": [response]}


def route_after_agent(state: AgentState) -> str:
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and last.tool_calls:
        if state.get("tool_rounds", 0) >= MAX_TOOL_ROUNDS:
            return "final_answer"
        return "tools"
    return "end"


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tools_node)
    graph.add_node("grade", grade_node)
    graph.add_node("final_answer", final_answer_node)

    graph.add_edge(START, "agent")
    graph.add_conditional_edges(
        "agent",
        route_after_agent,
        {"tools": "tools", "final_answer": "final_answer", "end": END},
    )
    graph.add_edge("tools", "grade")
    graph.add_edge("grade", "agent")
    graph.add_edge("final_answer", END)

    return graph.compile(checkpointer=MemorySaver())


_graph = None


def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


def ask(question: str, thread_id: str = "default") -> str:
    logger.info("===== 질문 시작 [thread=%s] %r =====", thread_id, question)
    graph = get_graph()
    # recursion_limit은 이중 안전장치. route_after_agent의 tool_rounds 하드캡이
    # 정상 작동하면 실제로는 절대 도달하지 않아야 한다.
    config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 15}
    result = graph.invoke(
        {"messages": [("user", question)], "tool_rounds": 0}, config=config
    )
    answer = result["messages"][-1].content
    logger.info("===== 질문 종료 [thread=%s] 최종 tool_rounds=%s =====", thread_id, result.get("tool_rounds"))
    return answer
