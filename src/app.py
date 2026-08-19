"""FastAPI 앱 조립.

이 서버의 API는 전부 API 서버(Spring)가 호출하는 내부 API다. 인증은 없다 —
호출자가 Spring 하나뿐이고 이 서버는 127.0.0.1에만 떠 있어 외부에서 닿을 수 없다.
작업형 API 둘은 작업 id 발급 주체가 서로 다르다: 인덱싱은 이 서버가 jobId를 발급하고
(요청 하나가 여러 화를 묶는 단위라서), 설정 오류 탐지는 Spring이 발급한 jobId로 일한다.
요청·응답 필드 표기는 전 API 공통으로 camelCase다(LOREKEEPER-273에서 chat까지 통일).
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src import config  # noqa: F401 — import 시점에 .env를 로드해 NEO4J_*/OPENAI_API_KEY를 환경변수로 채운다
from src.controller import (
    chat_controller,
    detect_controller,
    error_handlers,
    health_controller,
    index_controller,
)
from src.service.index import job_service as index_job_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 인덱싱 워커는 프로세스당 하나만 돈다. 여러 개를 띄우면 회차 순차 처리 전제가 깨진다.
    #
    # 반환된 task를 반드시 붙들어야 한다. 이벤트 루프는 task를 약한 참조로만 들고 있어서,
    # 지역 변수조차 없으면 GC가 실행 도중에 가져갈 수 있다. 그러면 큐가 영원히 처리되지
    # 않는데 접수 API는 계속 201을 돌려주므로, 아무 예외 없이 인덱싱만 멈춘다.
    worker = asyncio.create_task(index_job_service.worker())
    try:
        yield
    finally:
        # 종료 시 워커를 정리한다. 남겨두면 테스트가 앱을 여러 번 띄울 때 워커가 쌓인다.
        worker.cancel()


app = FastAPI(lifespan=lifespan)

# 에러 응답(RFC 9457)은 전부 이 핸들러들이 만든다 — service 는 도메인 예외를 던질 뿐이다.
error_handlers.register(app)

app.include_router(health_controller.router)
app.include_router(index_controller.router)
app.include_router(detect_controller.router)
app.include_router(chat_controller.router)
