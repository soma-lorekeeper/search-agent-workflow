# LOREKEEPER-272 글로벌 exception handler 도입

| | |
|---|---|
| Jira | [LOREKEEPER-272](https://lorekeepers.atlassian.net/browse/LOREKEEPER-272) |
| 스프린트 | 스프린트4 (2026-08-09 ~ 2026-08-16) · 목표: (없음) |
| 유형 | Subtask (부모: [LOREKEEPER-261](https://lorekeepers.atlassian.net/browse/LOREKEEPER-261) Refactoring python server) |
| 에픽 | LOREKEEPER-172 Lorekeeper MVP |
| 담당자 | Gomdadi |
| 상태 | 해야 할 일 (플랜 작성 시점) |
| 플랜 작성일 | 2026-08-19 |

## Context

에러 처리가 service 층에 흩어져 있다. `src/service/*/job_service.py`가 `fastapi.HTTPException`을 직접 던지고(7곳), 429는 `HTTPException`이 top-level 추가 필드를 못 싣는 제약 때문에 `JSONResponse`를 직접 만들어 return한다(4곳). 이 우회 때문에 service가 HTTP 타입을 알게 됐고, `submit()`이 "정상 DTO 또는 JSONResponse" 두 타입을 반환해 컨트롤러가 반환 타입 주석을 포기했다(`src/controller/detect_controller.py:19-21`에 사정 명시). `src/app.py`에는 exception handler가 하나도 없어 미처리 예외는 FastAPI 기본 500으로 나가며, `/api/chat`은 에러 계약 자체가 없다.

도메인 예외 + 글로벌 핸들러로 "예외 → 응답" 변환을 한 곳에 모으고, 에러 본문을 **RFC 9457 Problem Details**로 전환한다.

## 사용자 확정 결정 사항

1. **429 포함** — `JSONResponse` return 4곳도 도메인 예외(`RateLimited`)로 바꿔 핸들러로 옮긴다.
2. **본문 스키마: RFC 9457 Problem Details** — `{type, title, status, detail, ...확장}` + `Content-Type: application/problem+json`. **파괴적 변경**: Spring 파싱·스펙 문서·기존 에러 assert 테스트 전부 갱신.
3. **detail 메시지 영어 통일** — 한국어였던 404 두 곳을 영어로.
4. **프레임워크 예외 포함** — `StarletteHTTPException`(404/405), `RequestValidationError`(422)도 우리 핸들러로 받아 problem+json으로 통일하고 로그를 남긴다.

## 현황 파악 (코드에서 확인한 것)

- `HTTPException`: service 2개 파일에만 존재. `src/service/index/job_service.py:313,318,326,373,386`(400×5), `:485`(404), `src/service/detect/job_service.py:165`(404). 컨트롤러·테스트에는 없음.
- `JSONResponse` 429: `src/service/index/job_service.py:407`(큐 혼잡, `queuedEpisodes`+`estimatedWaitSeconds`), `:427`(모델 한도, `remainingTpm`), `src/service/detect/job_service.py:123`(동시 상한, `runningDetections`), `:136`(모델 한도). 전부 `Retry-After` 헤더 포함, 필드는 camelCase 리터럴.
- `Retry-After` 값 출처: `src/common/admission.py:46 budget_retry_after()`, 상수(`DETECT_JOB_SECONDS` 등).
- `src/app.py`: 핸들러 등록 0건. L40 `app = FastAPI(lifespan=lifespan)`과 L42 라우터 등록 사이가 등록 지점.
- `src/common/exceptions.py` 없음(신규). 저장소 전체에 `exception_handler` grep 0건 — 그린필드.
- 컨트롤러는 순수 위임(try/except 없음). `submit` 두 개만 반환 타입 주석 없음.
- `src/controller/health_controller.py:15-16` — DB가 죽어도 200을 주는 의도적 정책. 예외를 안 던지므로 이번 작업과 충돌 없음(catch-all은 미처리 예외에만 발동).
- 백그라운드 잡의 실패 기록(`_run_detect`, `_run_index_job`)은 요청 스코프 밖이고 **200 본문**의 `status: ERROR`로 나간다 — 이번 작업 대상 아님. `test_detect_api.py:342`, `test_index_api.py:485` 근처가 이 문자열을 완전일치 assert하므로 건드리면 안 된다.
- 에러 본문 assert 테스트: `test_index_api.py:154-155`(400 dict 완전일치), `:160,175,191,233,760`(400 부분 문자열), `:340,387-394,690-692`(429 필드·헤더), `:405-408`(400에 Retry-After 없음 확인), `:493`(404); `test_detect_api.py:353,484-490,513,535-537`. `test_chat_api.py`에는 에러 테스트 없음.
- 문서 계약: `docs/indexing-api-spec.md:37,106-143,173-179`, `docs/detecting-api-spec.md:37,70-104,187-193`. 500 스키마는 두 문서 모두 없음.
- **이슈와의 차이**: 이슈 본문은 429 경로(JSONResponse 4곳)를 언급하지 않지만 실제로는 이쪽이 레이어 위반의 절반이다 → 사용자 결정으로 범위에 포함. 이슈의 "통일된 에러 본문"은 RFC 9457로 구체화됨.

## 설계

### 에러 응답 형태 (RFC 9457)

```json
// 400
{ "type": "/errors/invalid-request", "title": "Invalid Request", "status": 400,
  "detail": "episodes must not be empty" }

// 404
{ "type": "/errors/not-found", "title": "Not Found", "status": 404,
  "detail": "indexing job 'abc' not found" }

// 429 (큐 혼잡) + Retry-After 헤더
{ "type": "/errors/queue-full", "title": "Queue Full", "status": 429,
  "detail": "Indexing queue is full. Retry after the Retry-After period.",
  "queuedEpisodes": 20, "estimatedWaitSeconds": 2640 }

// 422 (pydantic 검증 실패)
{ "type": "/errors/validation", "title": "Request Validation Failed", "status": 422,
  "detail": "Request body failed validation.", "errors": [ ...pydantic 오류 배열... ] }

// 500
{ "type": "about:blank", "title": "Internal Server Error", "status": 500,
  "detail": "Internal server error" }
```

- `Content-Type: application/problem+json` (모든 에러 응답)
- 확장 필드(`queuedEpisodes` 등)는 지금처럼 top-level camelCase — RFC 9457 확장 방식과 동일
- `type` 레지스트리(URI reference, 해석 안 되는 경로 슬러그): `/errors/invalid-request`, `/errors/not-found`, `/errors/queue-full`, `/errors/model-rate-limit`, `/errors/too-many-detections`, `/errors/validation`. 상태코드 이상의 의미가 없는 것(프레임워크 404/405, 500)은 RFC 기본값 `about:blank`.
- 500의 detail은 고정 문자열 — 스택·예외 메시지는 로그로만 (이슈 할 일 4).

### 신규 파일 1: `src/common/exceptions.py` — 도메인 예외

```python
class DomainError(Exception):
    """서비스 층이 던지는 도메인 실패. HTTP 를 모른다 — 매핑은 error_handlers 가 한다."""
    status: int          # 클래스 기본값
    title: str
    type: str
    def __init__(self, detail, *, type=None, title=None, extensions=None): ...
    # str(exc) == detail 을 보장 (로그·백그라운드 경로에서 str() 을 쓰는 관례와 맞춤)

class InvalidRequest(DomainError):  # 400, "/errors/invalid-request", "Invalid Request"
class NotFound(DomainError):        # 404, "/errors/not-found", "Not Found"
class RateLimited(DomainError):     # 429 — retry_after(초, 필수) 추가
    def __init__(self, detail, *, retry_after, type, title, extensions=None): ...
```

429는 사유별로 type/title이 달라 호출처에서 지정: 큐 혼잡 `/errors/queue-full` "Queue Full", 모델 한도 `/errors/model-rate-limit` "Model Rate Limit Exhausted", 동시 검사 상한 `/errors/too-many-detections` "Too Many Detections".

### 신규 파일 2: `src/controller/error_handlers.py` — 핸들러 + 등록

HTTP 변환은 controller 층 소관이므로 여기 둔다. `register(app)` 하나를 노출하고 `src/app.py`가 부른다.

- `ProblemResponse(JSONResponse)` — `media_type = "application/problem+json"`
- `DomainError` 핸들러 — `{type, title, status, detail, **extensions}`. `RateLimited`면 `Retry-After` 헤더. `logger.warning`(4xx는 정상 운영 이벤트라 스택 불필요).
- `StarletteHTTPException` 핸들러 — 프레임워크 404/405 + 혹시 남은/새로 생길 `fastapi.HTTPException`(하위 클래스라 함께 잡힘)을 problem+json으로. 검증 기준("service에 HTTPException 0건")의 런타임 안전망.
- `RequestValidationError` 핸들러 — 422. pydantic 오류 배열을 `errors` 확장 필드로 보존(스키마 불일치는 Spring 쪽 버그라 정보량을 줄이지 않는다). `logger.warning` — 현재는 422가 아무 로그도 안 남는다.
- `Exception` catch-all — 500 고정 본문, `logger.exception`으로 스택은 로그에만. `/api/chat`이 최대 수혜자(현재 유일한 무방비 500 경로).

로거 이름은 기존 규칙(모듈별 명명)대로 `"http"`.

### 수정: service 층 (HTTP 탈색)

- `src/service/index/job_service.py` — 400×5 → `InvalidRequest`(메시지 원문 유지), 404 → `NotFound(f"indexing job '{job_id}' not found")`(영어 전환), 429×2 → `raise RateLimited(...)`(detail·확장 필드·Retry-After 값 계산은 지금 그대로, 만들던 자리에서 예외에 실음). `from fastapi import ...` 제거. `submit` 반환 타입 `-> IndexAccepted` 복원.
- `src/service/detect/job_service.py` — 404 → `NotFound(f"detection job '{job_id}' not found")`, 429×2 → `RateLimited`. import 제거, `submit -> JobAck` 복원.

### 수정: controller 층

- `src/controller/detect_controller.py:19-21` — "반환 타입을 좁히지 않는 이유" 주석이 무효화되므로 삭제하고 `-> JobAck` 주석 추가.
- `src/controller/index_controller.py:12` — `-> IndexAccepted` 추가.
- chat/health는 코드 변경 없음(chat은 catch-all이 자동 커버, health는 200 정책 유지).

### 수정: 문서

- `docs/indexing-api-spec.md`, `docs/detecting-api-spec.md` — 에러 공통 규약(양쪽 L37)을 problem+json으로 교체, 400/404/429 예시 본문 전면 갱신, 500·422 스키마 신설, Content-Type 명시. **"신규/파괴적 — Spring 파싱 대응 필요"** 표시(detecting 문서의 429 신규 표기 전례를 따름). detecting 문서 L37이 추가 필드를 언급하지 않던 기존 불일치도 이 참에 해소.

### 수정: 테스트

- 기존 assert 갱신: 위 "현황 파악"에 나열한 `test_index_api.py`·`test_detect_api.py`의 에러 본문 assert들을 problem+json 기준으로. `detail` 부분-문자열 assert는 대부분 그대로 살고, dict 완전일치(`:155`)와 429 필드 위치는 수정 필요.
- 신규 `tests/test_error_contract.py` — 이슈 검증 기준 그대로: 400/404/429/422/500 각각에 대해 `{type, title, status, detail}` 4필드 존재 + `status`==HTTP 상태코드 + Content-Type이 `application/problem+json`인지 한 테스트 군으로 고정. 500은 라우트 하나를 monkeypatch로 강제 실패시켜 검증.
- **주의(비자명)**: Starlette `ServerErrorMiddleware`는 커스텀 500 핸들러의 응답을 보낸 **뒤에도 예외를 다시 던진다**. `TestClient` 기본값(`raise_server_exceptions=True`)은 이걸 그대로 올리므로, 500 테스트는 `TestClient(app, raise_server_exceptions=False)`로 만들어야 한다.

### 건드리지 않는 것

- 백그라운드 잡의 실패 문자열 기록(`_run_detect`, `_run_index_job`) — 요청 스코프 밖, 200 본문 계약.
- `/api/health`의 "DB가 죽어도 200" 정책.
- 프레임워크 422·404의 발생 조건 자체(핸들러로 모양만 통일).

## 실행 순서 (Blueprint)

> LOREKEEPER-272는 Subtask라 하위 이슈가 없다. 아래 단계는 Jira에 등록된 것이 아니라 **이 플랜에서 나눈 것**이다. 매 단계가 끝날 때 전체 테스트가 green이 되도록 순서를 짰다.

| 순서 | 단계 | 내용 | 선행 |
|---|---|---|---|
| 1 | 도메인 예외 정의 | `src/common/exceptions.py` 신설 | - |
| 2 | 핸들러 절반 등록 | `error_handlers.py` 신설 + app.py 등록. 단 `DomainError`·422·500만 — `StarletteHTTPException` 핸들러는 아직 뺀다(기존 400/404 본문이 바뀌어 테스트가 깨지므로) | 1 |
| 3 | index service 전환 | HTTPException·JSONResponse 7곳 → 도메인 예외, `test_index_api.py` assert 갱신 | 2 |
| 4 | detect service 전환 | 3곳 → 도메인 예외, `test_detect_api.py` assert 갱신 | 2 |
| 5 | 프레임워크 404/405 편입 | `StarletteHTTPException` 핸들러 추가(이 시점엔 service에 HTTPException이 없어 안전) | 3, 4 |
| 6 | 컨트롤러 정리 | 반환 타입 주석 복원, detect_controller 낡은 주석 교체 | 3, 4 |
| 7 | 문서·최종 검증 | 스펙 2종 갱신, `test_error_contract.py` 마무리, 전체 검증 | 5, 6 |

```
1 ── 2 ──┬── 3 ──┬── 5 ──┐
         └── 4 ──┴── 6 ──┴── 7
```

**순서 근거**: 의존성(예외 없이는 핸들러도 service 전환도 불가)과 "매 단계 green" 유지가 결정했다. `StarletteHTTPException` 핸들러를 5단계로 미룬 것이 핵심 — 2단계에서 함께 등록하면 service가 아직 던지는 `HTTPException`(하위 클래스)의 응답 모양이 바뀌어 3·4단계 전까지 기존 테스트가 깨진 채로 남는다. 3과 4는 서로 독립이라 병렬 가능.

**Jira 등록 순서와의 차이**: 이슈 본문의 할 일 1→5 순서를 따르되, 프레임워크 예외 편입(5단계)을 service 전환 뒤로 미루는 세분화를 더했다.

## 리스크 / 미결정 사항

- **Spring 연동 파괴** (가장 큼): 에러 본문·Content-Type이 바뀌므로 Spring의 에러 파싱(특히 429 재시도 로직이 `Retry-After` 외에 본문을 읽는다면)이 함께 배포되어야 한다. Spring Boot 3+ `ProblemDetail`이 이 포맷의 표준 구현이라 대응 비용은 낮다. 부모 이슈(LOREKEEPER-261)의 "배포 시 주의"에 이미 Spring 대응 항목들이 있으므로 같은 목록에 추가한다.
- **`Retry-After` 헤더 규약은 불변**: Spring의 최소 대응(헤더 보고 재시도)은 깨지지 않는다. 본문 확장 필드도 이름·위치를 유지한다.
- **테스트 갱신 누락**: 에러 assert가 13곳+에 흩어져 있다. 단계 3·4에서 파일 단위로 전량 갱신하고, 7단계에서 전체 스위트로 재확인.

## 검증 방법

이슈의 검증 기준 그대로:

1. `.venv/bin/python -m pytest tests/ -q --ignore=tests/test_llm_smoke.py` 전체 통과
2. `grep -rn "HTTPException\|JSONResponse" src/service/` → 0건
3. `tests/test_error_contract.py` — 400/404/422/429/500 응답이 모두 `{type, title, status, detail}` + `application/problem+json`인지 통과
4. 429 확장 필드(`queuedEpisodes`, `estimatedWaitSeconds`, `runningDetections`, `remainingTpm`)와 `Retry-After` 헤더가 기존 값 그대로인지(갱신된 `test_index_api.py:387-394` 계열로 확인)
