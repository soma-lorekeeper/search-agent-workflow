# LOREKEEPER-273 API 요청·응답 포맷을 camelCase로 고정

| | |
|---|---|
| Jira | [LOREKEEPER-273](https://lorekeepers.atlassian.net/browse/LOREKEEPER-273) |
| 스프린트 | 스프린트4 (2026-08-09 ~ 2026-08-16) · 목표: (없음) |
| 유형 | Subtask |
| 에픽 | 부모 작업: LOREKEEPER-261 Refactoring python server (에픽: LOREKEEPER-172 Lorekeeper MVP) |
| 담당자 | Gomdadi |
| 상태 | 해야 할 일 |
| 플랜 작성일 | 2026-08-19 |

## Context

와이어 포맷이 엔드포인트마다 다르다 — index·detect는 `CamelModel`로 camelCase인데 chat만 맨 `BaseModel`이라 snake_case로 나가고, health는 dict 직반환이라 `latency_ms` 키 하나가 snake_case로 남아 있다. Spring이 이 서버를 호출할 때 엔드포인트별로 다른 규칙을 기억해야 하는 상태를 끝내고, 전 API의 요청·응답 필드를 camelCase 하나로 고정한다.

**뒤집히는 과거 결정**: `.claude/plan/server-restructure-layered-tenancy.md:21`과 `src/dto/chat_dto.py:1`은 "chat만 snake_case 유지(사용자 확정)"라고 기록하고 있다. 이 이슈(LOREKEEPER-273)가 그 결정을 명시적으로 뒤집는 것이므로, 해당 docstring·주석도 함께 갱신한다.

## 현황 파악 (코드에서 확인)

- `src/dto/common.py:12` — `CamelModel`(alias_generator=to_camel, populate_by_name=True)이 이미 있다. 새로 만들 것 없음.
- `src/dto/chat_dto.py` — 6개 모델 전부 `BaseModel` 상속. 여러 단어 필드: `ChatContext.editing_episode/viewing_episode_number`(:36-37), `ChatRequest.user_id/work_id/session_id`(:42-44), `ChatResponse.tool_calls/suggested_title`(:57-58).
- `src/controller/chat_controller.py:17,35` — `response_model=ChatResponse` + `ChatResponse(**result)`. FastAPI 기본값 `response_model_by_alias=True`라서 **DTO만 CamelModel로 바꾸면 응답은 자동으로 camelCase**가 된다.
- `src/controller/chat_controller.py:32-33` — `m.model_dump()` / `req.context.model_dump()`는 by_alias 없이 snake_case dict를 만들어 서비스에 넘긴다. `src/service/chat/agent.py:141,147,309-313`이 그 snake_case 키(`editing_episode` 등)로 읽는다. **여기에 by_alias=True를 붙이면 안 된다** — 붙이면 서비스가 키를 못 찾아 조용히 None 처리된다.
- `src/service/chat/**` — pydantic DTO를 전혀 모르고 dict/kwargs만 쓴다. 영향 반경이 컨트롤러 경계에서 끝난다.
- `src/service/health_service.py:30` — `latency_ms`가 유일한 snake_case 응답 키. health 테스트는 없어서 바꿔도 깨질 테스트 없음.
- `src/controller/error_handlers.py`, `src/common/exceptions.py` — 에러 본문은 RFC 9457 기본 4키(단일어) + 확장 필드는 이미 camelCase 리터럴 규약. **수정 불필요.**
- `src/app.py:5-7` — docstring이 "인덱싱만 camelCase, 나머지 snake_case"라고 낡은 설명. `job_id` 표기(:5-6)도 detect가 camelCase로 전환된 현재와 어긋남.
- `tests/test_chat_api.py` — 요청 페이로드 4곳(:48-57, :76-81, :94-100, :114-119)이 snake_case. **응답 본문 키를 assert하는 테스트는 0건**(전부 status_code만 확인) → camelCase 회귀 테스트 신규 필요. `captured[...]` assert들은 wire가 아니라 내부 kwargs 검증이므로 snake_case 유지.
- `docs/detecting-api-spec.md:34` — "필드 표기: camelCase" 이미 있음. `docs/indexing-api-spec.md` 2절(:38-44)에는 이 줄이 **없음**.
- `tests/test_error_contract.py:95` — snake_case 본문으로 `/api/detect`를 호출하고 통과 중. `populate_by_name=True`라 전환 후에도 snake_case 입력은 계속 수용된다(이슈도 "전환 기간 양쪽 수용"을 의도). camelCase는 **정본 표기**이지 snake_case 거부가 아니다.

## 실행 순서 (Blueprint)

> LOREKEEPER-273은 Subtask라 하위 이슈가 없다. 아래 단계는 Jira에 등록된 것이 아니라 **이 플랜에서 새로 나눈 것**이다.

| 순서 | 단계 | 내용 | 선행 조건 |
|---|---|---|---|
| 1 | chat DTO 전환 | chat_dto.py 6개 모델을 CamelModel 상속으로 | - |
| 2 | health 키 전환 | `latency_ms` → `latencyMs` | - |
| 3 | 테스트 갱신·신설 | 요청 페이로드 camelCase화 + 응답 camelCase 회귀 테스트 추가 | 1, 2 |
| 4 | 주석·문서 정합화 | app.py docstring, readme, 스펙 문서 | 1, 2 |

**순서 근거**: 1·2가 실제 동작 변경이고 서로 독립(병렬 가능). 3이 그 둘을 검증하고, 4는 코드가 확정된 뒤 문장을 맞추는 마무리라 마지막.

## 단계별 상세

### 1. chat DTO를 CamelModel로 전환

**대상**: `src/dto/chat_dto.py`

- import를 `from src.dto.common import CamelModel`로 바꾸고 6개 클래스(`ChatMessage`, `ChatEditingEpisode`, `ChatContext`, `ChatRequest`, `ChatToolCall`, `ChatResponse`) 상속 교체.
- `:1` docstring "이 API만 snake_case를 유지한다" → "전 API 공통으로 camelCase" 취지로 재작성. populate_by_name 덕에 전환기에 snake_case 입력도 받는다는 사실 명시.
- `chat_controller.py:32-33`은 **그대로 둔다** (by_alias 금지 — 내부 dict는 snake_case 계약).

**완료 기준**: `/api/chat` 응답 JSON 키가 `toolCalls`/`suggestedTitle`로 나가고, camelCase 요청(`userId` 등)이 수용된다.

### 2. health 응답 키 전환

**대상**: `src/service/health_service.py:30`

- `"latency_ms"` → `"latencyMs"`.

**완료 기준**: `/api/health` 응답에 `_` 포함 키 0건.

### 3. 테스트 갱신·신설

**대상**: `tests/test_chat_api.py`

- 요청 페이로드 4곳(:48-57, :76-81, :94-100, :114-119)을 camelCase로 (`userId`, `workId`, `sessionId`, `editingEpisode`, `viewingEpisodeNumber`, `indexedEpisodes`).
- `captured[...]` 내부 kwargs assert는 snake_case 유지 (wire 검증이 아님).
- 신규: 응답 JSON을 재귀 순회해 `_` 포함 키가 하나도 없는지 assert하는 테스트 추가 — chat 응답(`toolCalls`/`suggestedTitle` 존재 확인 포함)과 health 응답 두 경로.
- `:23` 주석("이 API만 snake_case") 갱신.

**완료 기준**: `pytest tests/` 전체 통과.

### 4. 주석·문서 정합화

- `src/app.py:5-7` — "인덱싱만 camelCase" 문장을 "전 API camelCase"로, `job_id` → `jobId`.
- `readme.md:18` — "chat만 snake_case" 제거. `readme.md:135-136` health 예시의 `latency_ms` → `latencyMs`. `readme.md:161` 남은 작업 목록에서 해당 항목 정리.
- `src/controller/index_controller.py:1` — "스펙대로 camelCase" 문구를 전역 규칙 기준으로 손질(선택적, 한 줄).
- `docs/indexing-api-spec.md` 2절 공통 규약에 `- **필드 표기**: camelCase` 줄 추가 (detecting 스펙 :34와 같은 형식). `docs/detecting-api-spec.md`는 이미 있으므로 확인만.
- chat 스펙 문서는 **신설하지 않는다** — 이슈 할 일 5는 기존 두 스펙 문서에 명시만 요구.

**완료 기준**: `grep -rn "snake_case" src/ readme.md`에서 "chat이 snake_case"라는 서술이 남아있지 않다.

## 리스크 / 미결정 사항

- **Spring 배포 순서**: 채팅 요청 필드가 camelCase로 바뀌는 계약 변경. `populate_by_name=True` 덕에 이 서버는 전환기에 snake_case도 받지만, **이 서버의 응답**(`toolCalls` 등)은 즉시 camelCase로 바뀌므로 Spring이 chat 응답을 snake_case로 파싱하고 있다면 그쪽 대응 후 배포해야 한다.
- **snake_case 입력 수용 유지**: `test_error_contract.py:95`가 snake_case로 detect를 호출해도 계속 통과한다. "camelCase 고정"을 입력 거부까지로 해석하지 않는다(이슈 본문의 전환기 의도). 엄격 거부가 필요해지면 별도 이슈.

## 검증 방법

1. `pytest tests/` 전체 통과 (특히 test_chat_api의 camelCase 페이로드 + 신규 응답 키 테스트).
2. 신규 테스트가 chat·health 응답 JSON에 `_` 포함 키 0건을 보장.
3. `grep -rn "latency_ms\|snake_case" src/ tests/ readme.md docs/`로 잔재 확인.

## 완료 후 Jira 반영 (플랜 승인 시 함께 진행)

1. 플랜 문서를 `.claude/plan/sprint-스프린트4-api-camelcase-wire-format.md`로 저장.
2. LOREKEEPER-273 설명(description)에 기존 본문을 보존한 채 구분선 아래로 플랜 요약(배경/단계/발견 사항)을 덧붙임.
3. LOREKEEPER-273 상태를 `해야 할 일` → `진행 중`으로 전환 (부모 261은 이미 진행 중이라 건드리지 않음).
4. **구현은 하지 않는다** — 플랜 저장과 Jira 반영까지가 이 작업의 끝.
