"""도메인 예외 — service 층이 던지는 실패의 공통 어휘.

service 는 HTTP 를 모른다. 여기 예외들은 상태코드가 아니라 "실패의 종류"를 말하고,
HTTP 응답으로의 변환은 controller 층(src/controller/error_handlers.py)이 한 곳에서 한다.
응답은 RFC 9457 Problem Details 라서 각 예외가 그 본문의 재료(type/title/status/detail/
확장 멤버)를 그대로 들고 있다 — 핸들러는 조립만 하고 내용은 던지는 쪽이 정한다.

예전에는 service 가 `fastapi.HTTPException`을 직접 던지고, 429 는 `HTTPException`이
top-level 추가 필드(queuedEpisodes 등)를 못 싣는 제약 때문에 `JSONResponse`를 직접
만들어 return 했다. 그 우회 때문에 service 가 HTTP 타입을 알게 되고 submit()의 반환
타입이 둘로 갈라졌는데, 이 예외 계층이 그 원인을 제거한다.
"""

from __future__ import annotations


class DomainError(Exception):
    """모든 도메인 예외의 뿌리.

    status/title/type 은 클래스 기본값이고, 429 처럼 사유별로 type·title 이 갈리는 경우만
    생성 시점에 덮어쓴다. type 은 RFC 9457 의 "문제 유형" URI reference 다 — 해석되는
    주소가 아니라 기계 판독용 슬러그로 쓴다(예: "/errors/not-found").

    extensions 는 RFC 9457 확장 멤버로, 응답 본문의 top-level 에 키 이름 그대로 실린다.
    스펙의 429 필드가 camelCase 이므로 키도 camelCase 로 넣어야 한다.
    """

    status: int = 500
    title: str = "Internal Server Error"
    type: str = "about:blank"  # RFC 9457 기본값: 상태코드 이상의 의미가 없다는 뜻

    def __init__(
        self,
        detail: str,
        *,
        type: str | None = None,
        title: str | None = None,
        extensions: dict | None = None,
    ):
        # str(exc) == detail 을 보장한다. 이 코드베이스는 실패 사유를 str(exc)로 뽑아
        # 로그·상태에 남기는 관례가 있어(job_service 의 백그라운드 경로), 그 관례와 맞춘다.
        super().__init__(detail)
        self.detail = detail
        if type is not None:
            self.type = type
        if title is not None:
            self.title = title
        self.extensions = extensions or {}


class InvalidRequest(DomainError):
    """400 — 요청의 **내용**이 도메인 규칙을 어겼다(빈 episodes, 회차 불연속 등).

    요청의 **모양**(필드 타입·필수 필드)이 틀린 것은 여기가 아니라 pydantic 검증 422 다 —
    그쪽은 컨트롤러에 도달하기 전에 프레임워크가 거른다.
    """

    status = 400
    title = "Invalid Request"
    type = "/errors/invalid-request"


class NotFound(DomainError):
    """404 — 조회한 것이 없다(모르는 jobId 등)."""

    status = 404
    title = "Not Found"
    type = "/errors/not-found"


class RateLimited(DomainError):
    """429 — 지금은 받을 수 없으니 retry_after 초 뒤에 다시 보내라.

    사유(큐 혼잡·동시 검사 상한·모델 한도)마다 type/title 과 확장 멤버가 다르므로
    던지는 쪽이 전부 지정한다. retry_after 는 핸들러가 Retry-After 헤더로 옮긴다 —
    "본문의 부가 정보"가 아니라 Spring 재시도 흐름이 의존하는 계약이라 필수 인자다.
    """

    status = 429

    def __init__(
        self,
        detail: str,
        *,
        retry_after: int,
        type: str,
        title: str,
        extensions: dict | None = None,
    ):
        super().__init__(detail, type=type, title=title, extensions=extensions)
        self.retry_after = retry_after
