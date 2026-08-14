"""AI 채팅 API의 요청·응답 모델. 이 API만 snake_case를 유지한다."""

from pydantic import BaseModel


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatEditingEpisode(BaseModel):
    """작가가 지금 고쳐 쓰고 있는 회차. 본문은 발췌가 아니라 **전문**이다.

    number가 없을 수 있다 — API 서버의 DRAFT는 화수가 확정되기 전이라 번호가 null이다.
    """

    # 화수. DRAFT(아직 확정 전)면 없다.
    number: int | None = None
    title: str | None = None
    text: str | None = None


class ChatContext(BaseModel):
    """이번 질문의 회차 컨텍스트.

    회차에 얽힌 개념은 셋인데 여기 실리는 건 둘뿐이다:
      - editing_episode  : 집필 중인 회차(전문 포함). API 서버가 도메인 규칙으로 정한다.
      - viewing_episode_number : 화면에 열어 둔 회차. 프론트만 알 수 있다.
    셋째인 "인덱싱된 회차"는 **일부러 요청에 없다.** 그건 Neo4j 그래프의 사실이고, 요청이
    들고 온 값은 인덱싱이 진행되는 동안 곧바로 낡는다. 에이전트가 매 턴 직접 조회한다
    (src/service/chat/indexed.py).

    셋 다 없어도 대화는 성립한다 — 편집기를 열지 않고 질문만 하는 경우가 있다.
    """

    editing_episode: ChatEditingEpisode | None = None
    viewing_episode_number: int | None = None


class ChatRequest(BaseModel):
    # userId × workId가 KG 테넌트다. 인덱싱·탐지와 같은 키여야 같은 그래프를 본다.
    user_id: int
    work_id: int
    session_id: int
    messages: list[ChatMessage]
    context: ChatContext = ChatContext()


class ChatToolCall(BaseModel):
    name: str
    summary: str
    status: str


class ChatResponse(BaseModel):
    content: str
    tool_calls: list[ChatToolCall] = []
    suggested_title: str | None = None
