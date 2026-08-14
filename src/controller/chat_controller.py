"""AI 채팅 API.

인덱싱/검사와 달리 백그라운드 작업이 아니다 — 작가가 답을 기다리고 있으므로 이 요청 안에서
끝까지 처리해 JSON으로 한 번에 돌려준다(스트리밍 아님). 대화 기록은 서버 메모리에 남기지
않는다: 진실의 원천은 API 서버(Spring)의 chat_messages 테이블이고, 여기는 매 턴 통째로
받아서 답만 만들어 주는 무상태 계산기다(그래야 이 서버가 재시작해도 대화가 안 끊긴다).
"""

from fastapi import APIRouter

from src.service.chat import run_chat
from src.dto.chat_dto import ChatRequest, ChatResponse

router = APIRouter()


@router.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    """작가의 질문 하나에 답한다.

    에이전트가 KG(인물 상태·사건)와 원고 DB를 직접 조회해서 답을 만든다. 어떤 도구를 몇 번
    부를지는 질문마다 다르므로 모델이 스스로 고른다 — 그 내역을 tool_calls로 함께 내려보내
    프론트가 "무엇을 찾아봤는지"를 보여줄 수 있게 한다(근거 없는 답변처럼 보이지 않게 하는 게
    이 필드의 목적이다).

    suggested_title은 대화 첫 턴에만 채워진다. 세션 제목을 저장할지 말지는 API 서버가 정한다.
    """
    result = await run_chat(
        user_id=req.user_id,
        work_id=req.work_id,
        session_id=req.session_id,
        messages=[m.model_dump() for m in req.messages],
        context=req.context.model_dump(),
    )
    return ChatResponse(**result)
