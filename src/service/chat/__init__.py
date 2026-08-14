"""작가용 AI 채팅 모듈.

인덱싱된 KG와 PostgreSQL 원고를 도구로 들고, 작가가 자기 작품에 대해 묻는 말에
"실제로 조회한 근거"로 답한다. 설정 오류 검사(src/service/detect)와 달리 판정이 아니라
대화가 목적이라 응답 형식이 자유롭고, 원고를 절대 고치지 않는다(읽기 전용).
"""

from src.service.chat.agent import run_chat

__all__ = ["run_chat"]
