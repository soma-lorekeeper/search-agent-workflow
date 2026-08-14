"""FastAPI 앱 조립.

이 서버의 API는 전부 API 서버(Spring)가 호출하는 내부 API다. 인증은 없다 —
호출자가 Spring 하나뿐이고 이 서버는 127.0.0.1에만 떠 있어 외부에서 닿을 수 없다.
작업형 API 둘은 작업 id 발급 주체가 서로 다르다: 인덱싱은 이 서버가 jobId를 발급하고
(요청 하나가 여러 화를 묶는 단위라서), 설정 오류 탐지는 Spring이 발급한 job_id로 일한다.
필드 표기도 그래서 다르다 — 인덱싱만 스펙대로 camelCase이고 나머지는 snake_case다.
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src import config  # noqa: F401 — import 시점에 .env를 로드해 NEO4J_*/OPENAI_API_KEY를 환경변수로 채운다
from src.controller import (
    chat_controller,
    detect_controller,
    health_controller,
    index_controller,
)
from src.service.index import job_service as index_job_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 인덱싱 워커는 프로세스당 하나만 돈다. 여러 개를 띄우면 회차 순차 처리 전제가 깨진다.
    asyncio.create_task(index_job_service.worker())
    yield


app = FastAPI(lifespan=lifespan)

app.include_router(health_controller.router)
app.include_router(index_controller.router)
app.include_router(detect_controller.router)
app.include_router(chat_controller.router)
