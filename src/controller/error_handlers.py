"""예외 → HTTP 에러 응답 변환의 단일 관문.

모든 에러 응답은 RFC 9457 Problem Details 다:
  {"type": ..., "title": ..., "status": ..., "detail": ..., ...확장 멤버}
  + Content-Type: application/problem+json

service 는 도메인 예외(src/common/exceptions.py)를 던질 뿐 HTTP 를 모르고, 상태코드·
본문·헤더는 여기서만 정해진다. HTTP 변환은 controller 층 소관이라 이 파일이 여기 있다.

register(app) 하나만 노출한다 — app.py 가 조립 시점에 부른다. 데코레이터가 아니라
등록 함수인 이유: 핸들러는 app 인스턴스에 묶이는데, 모듈 import 만으로 등록되는 구조는
"어디서 등록됐는지"를 조립 코드에서 보이지 않게 한다.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.common.exceptions import DomainError, RateLimited

logger = logging.getLogger("http")


class ProblemResponse(JSONResponse):
    """RFC 9457 응답. 본문이 problem details 임을 Content-Type 으로 알린다."""

    media_type = "application/problem+json"


def _problem(
    status: int,
    title: str,
    detail: str,
    *,
    type: str = "about:blank",
    headers: dict | None = None,
    extensions: dict | None = None,
) -> ProblemResponse:
    """4필드 + 확장 멤버로 problem 본문을 조립한다.

    확장 멤버는 top-level 에 그대로 편다 — 스펙의 429 필드(queuedEpisodes 등)가
    top-level camelCase 로 정의돼 있고, RFC 9457 의 확장 방식도 이것이다.
    """
    return ProblemResponse(
        status_code=status,
        headers=headers,
        content={
            "type": type,
            "title": title,
            "status": status,
            "detail": detail,
            **(extensions or {}),
        },
    )


def register(app: FastAPI) -> None:
    """앱에 예외 핸들러를 단다. app.py 의 조립 시점에 한 번 부른다."""

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError):
        # 4xx 는 정상 운영 이벤트다(거절·미존재·혼잡) — 스택 없이 warning 만 남긴다.
        # 상세한 사유별 로그(거절 카운트 등)는 던지는 쪽이 이미 남기고 있다.
        logger.warning(
            "%d %s | %s %s | %s", exc.status, exc.title, request.method, request.url.path, exc.detail
        )
        # Retry-After 는 RateLimited 만 갖는다. Spring 의 재시도 흐름이 이 헤더에 의존한다.
        headers = {"Retry-After": str(exc.retry_after)} if isinstance(exc, RateLimited) else None
        return _problem(
            exc.status, exc.title, exc.detail, type=exc.type, headers=headers, extensions=exc.extensions
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception(request: Request, exc: StarletteHTTPException):
        # 프레임워크가 던지는 HTTP 예외 — 없는 경로의 404, 허용 안 된 메서드의 405.
        # fastapi.HTTPException 도 이 타입의 하위 클래스라 여기로 온다: service 층은
        # 도메인 예외만 쓰는 것이 규칙이지만, 규칙이 뚫려도 응답 모양은 무너지지 않는다.
        # 상태코드 이상의 의미가 없으므로 type 은 RFC 기본값(about:blank)이다.
        logger.warning(
            "%d http exception | %s %s | %s", exc.status_code, request.method, request.url.path, exc.detail
        )
        return _problem(
            exc.status_code, exc.detail, exc.detail, headers=getattr(exc, "headers", None)
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        # 422 = 요청의 "모양"이 스키마와 다르다. 호출자가 Spring 하나뿐인 내부 API 에서
        # 이건 사용자 시나리오가 아니라 Spring 쪽 요청 조립 코드의 버그다. 예전에는 아무
        # 로그도 안 남아 스키마 불일치가 Spring 쪽에서만 보였다 — 여기서 흔적을 남긴다.
        #
        # pydantic 오류 배열은 errors 확장 멤버로 그대로 보존한다. 어떤 필드가 왜 틀렸는지가
        # 디버깅 단서라 정보량을 줄이지 않는다. jsonable_encoder 를 거치는 이유: 배열 안에
        # ValueError 객체 등 그대로는 직렬화되지 않는 값이 들어올 수 있다(FastAPI 기본
        # 핸들러도 같은 처리를 한다).
        errors = jsonable_encoder(exc.errors())
        logger.warning("422 validation failed | %s %s | %s", request.method, request.url.path, errors)
        return _problem(
            422,
            "Request Validation Failed",
            "Request body failed validation.",
            type="/errors/validation",
            extensions={"errors": errors},
        )

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        # 잡히지 않은 모든 예외의 마지막 그물. 스택트레이스는 로그로만 남기고 응답은 고정
        # 문구다 — 내부 예외 메시지를 밖에 노출하지 않는다(이슈 요구사항).
        logger.exception("500 unhandled | %s %s", request.method, request.url.path)
        return _problem(500, "Internal Server Error", "Internal server error")
