"""작가용 AI 채팅 모듈.

인덱싱된 KG와 PostgreSQL 원고를 도구로 들고, 작가가 자기 작품에 대해 묻는 말에
"실제로 조회한 근거"로 답한다. 설정 오류 검사(src/service/detect)와 달리 판정이 아니라
대화가 목적이라 응답 형식이 자유롭고, 원고를 절대 고치지 않는다(읽기 전용).

여기서 심볼을 재수출하지 않는다 — 소비자는 detect·index와 마찬가지로 서브모듈을 직접
import한다(`from src.service.chat import agent`). 재수출을 두면 이 패키지를 건드리는
순간 agent·tools·prompts가 통째로 로드되고, 서비스끼리 참조하게 될 때 순환 import가 난다.
"""
