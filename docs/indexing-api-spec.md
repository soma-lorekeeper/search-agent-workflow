# Lorekeeper Indexing API Spec

| | |
|---|---|
| 버전 | v1 (2026-08-11) |
| 대상 독자 | Spring 서버 개발팀 |
| 범위 | **Indexing API만.** 탐지는 `docs/detecting-api-spec.md`, 채팅은 `docs/chatting-api-spec.md` 참고 |

## 1. 개요

이 서버는 Spring 서버가 호출하는 **내부 파이썬 워커**다. 외부(브라우저·앱)에 직접 노출되지 않으며, 원고를 받아 지식 그래프(Neo4j)로 인덱싱하는 일을 한다.

동작 모델은 단순 REST다:

```
Spring ──POST /api/index──────────▶ Python    인덱싱 제출
                                              ├─ 여유 있음: 201 즉시 응답 후 비동기 처리
                                              └─ 큐 혼잡/한도 소진: 429 거절 → Spring이 나중에 재제출
Spring ──GET /api/index/jobs/{id}─▶ Python    진행 상태 폴링
```

Spring이 수행하는 폴링은 **두 가지**이며 서로 다르다:

| 폴링 | 트리거 | 동작 |
|---|---|---|
| **재제출 폴링** | `POST`가 429로 거절됨 | `Retry-After` 초 대기 후 같은 요청을 다시 `POST` |
| **상태 폴링** | `POST`가 201로 수리됨 | `GET /api/index/jobs/{jobId}`를 주기적으로 호출해 완료 확인 |

인덱싱은 **한 화당 약 2분** 걸린다. 상태 폴링 간격은 10초를 권장한다.

## 2. 공통 규약

- **소설 식별**: `userId` × `workId` 조합이 소설 한 편을 unique하게 구분한다
- **`episodeId`**: Spring 측 회차 식별자. 파이썬은 의미를 해석하지 않고 상태 응답에 그대로 에코한다
- **`episodeNo`**: 회차 순번(1화, 2화, …). 인덱싱은 이전 회차까지의 누적 컨텍스트 위에서 동작하므로 순번이 필수다 — `TBD: episodeId가 회차 순번과 동일하다면 이 필드는 제거 가능. Spring 팀 확인 필요`
- **필드 표기**: camelCase — 요청·응답 전 필드. 이 서버의 모든 API(indexing·detecting·chat) 공통 규약이다
- **시각**: RFC 3339 UTC (`2026-08-11T03:11:00Z`)
- **에러 본문**: [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457) — 아래 2.1 참고
- **인증**: 별도 인증 없음. Spring이 이미 인증을 마친 요청만 보내므로 파이썬은 이를 신뢰한다 (이 서버는 외부에 노출되지 않는 내부 서버라는 전제)

### 2.1 에러 응답 (RFC 9457 Problem Details)

> **신규/파괴적.** 이전 버전의 에러 본문은 `{ "detail": "사유 문자열", ... }`이었다. 이제 모든 에러 응답(4xx/5xx)이 아래 모양이고 `Content-Type: application/problem+json`으로 나간다. **Spring 쪽 에러 파싱 대응이 필요하다** — 상태코드와 `Retry-After` 헤더만 보는 로직은 영향이 없고, Spring Boot 3+의 `ProblemDetail`이 이 포맷의 표준 구현이다.

```json
{
  "type": "/errors/queue-full",
  "title": "Queue Full",
  "status": 429,
  "detail": "Indexing queue is full. Retry after the Retry-After period.",
  "queuedEpisodes": 20,
  "estimatedWaitSeconds": 2640
}
```

| 필드 | 설명 |
|---|---|
| `type` | 에러 종류의 기계 판독용 식별자. 상태코드 이상의 의미가 없으면 `about:blank` |
| `title` | 에러 종류의 짧은 요약 (`type`별로 고정) |
| `status` | HTTP 상태코드 (응답 라인과 동일한 값) |
| `detail` | 이 발생 건에 대한 사람이 읽는 설명 (영어) |
| 그 외 | 사유별 확장 필드 — `queuedEpisodes`, `remainingTpm` 등 top-level camelCase 유지 |

`type` 목록: `/errors/invalid-request`(400) · `/errors/not-found`(404) · `/errors/queue-full`, `/errors/model-rate-limit`(429) · `/errors/validation`(422) · `about:blank`(그 외 404/405, 500)

**422와 500** (이전 버전에는 스키마가 없던 응답):

- `422` — 요청 본문이 스키마와 다르다(필드 타입 오류, 필수 필드 누락). Spring 쪽 요청 조립 버그를 뜻하며, pydantic 오류 배열이 `errors` 확장 필드에 실린다.
- `500` — 잡히지 않은 서버 오류. `detail`은 고정 문구 `"Internal server error"`이고 내부 예외 메시지는 노출되지 않는다(로그로만 남는다).

## 3. `POST /api/index` — 인덱싱 제출

여러 화를 한 번에 제출할 수 있다. 단건이면 원소 1개짜리 배열로 보낸다.

### Request

```json
{
  "userId": 42,
  "workId": 7,
  "episodes": [
    { "episodeId": 101, "episodeNo": 6, "text": "6화 원고 전문..." },
    { "episodeId": 102, "episodeNo": 7, "text": "7화 원고 전문..." }
  ]
}
```

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| `userId` | number | ✅ | 유저 식별자 |
| `workId` | number | ✅ | 작품 식별자 |
| `episodes` | array | ✅ | 1개 이상. **`episodeNo`가 1씩 증가하도록, 마지막 인덱싱된 화의 다음 화부터 보낼 것** (아래 참고) |
| `episodes[].episodeId` | number | ✅ | Spring 측 회차 식별자 (에코용) |
| `episodes[].episodeNo` | number | ✅ | 회차 순번 |
| `episodes[].text` | string | ✅ | 회차 원고 전문 (평문) |

**처리 순서**: 인덱싱은 이전 회차의 그래프·요약을 배경 컨텍스트로 쓰는 누적 구조라, 배열의 화들은 **순서대로 순차 처리**된다 (병렬 아님).

### 회차 연속성 (v2에서 강제됨)

`episodeNo`는 **빈틈 없이 이어져야 한다.** 어기면 400이다.

| 규칙 | 예 |
|---|---|
| 요청 안에서 1씩 증가 | `[8, 10]` → 400 (9화 없음) |
| 마지막으로 **인덱싱된** 화의 다음 화부터 | 그래프에 4화까지 → `[5, 6]` ✅ / `[7, 8]` → 400 |
| 인덱싱된 화가 없으면 1화부터 | 첫 요청이 `[5]` → 400 |

기준은 **그래프에 실제로 인덱싱된 화**이고, 아직 처리 중인 화도 함께 센다(그래야 앞 화가 도는 동안 다음 화를 미리 보낼 수 있다).

이미 인덱싱된 화가 요청에 섞여 있어도 괜찮다 — 그 화들을 빼고 남은 첫 화로 판정한다. 404 후 묶음을 통째로 재제출하는 흐름(7.3)이 그 경우다.

> **변경 이력**: 예전에는 "오름차순"만 요구했고 그것도 메모리의 접수 이력으로 판정했다. 부등식이라 역순만 막혀 `[1,2]` 뒤의 `[5,6]`이 통과했고(3·4화 없이 5화가 인덱싱된다), 접수 이력이라 그래프의 실제 상태와 갈렸다 — `[5,6,7]` 중 6화가 실패하면 그래프에는 5까지만 있는데 이력은 7이라, 스펙이 약속한 "실패 화부터 재제출"이 400으로 막혔다. 지금은 그래프가 근거이고 등식이라 둘 다 해결된다. 부수적으로 서버 재시작 후에도 판정이 정확하다.

### Response `201 Created` — 수리됨, 비동기 처리 시작

즉시 응답한다. 인덱싱 결과물은 응답에 없으며, 완료 여부는 상태 폴링(4장)으로 확인한다.

```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": 42,
  "workId": 7,
  "episodeIds": [101, 102],
  "requestedAt": "2026-08-11T03:11:00Z",
  "remainingTpm": 143000
}
```

| 필드 | 설명 |
|---|---|
| `jobId` | 파이썬이 발급하는 UUID. 상태 폴링의 키 |
| `episodeIds` | 수리된 회차 목록 (요청 순서대로) |
| `requestedAt` | 수리 시각 |
| `remainingTpm` | OpenAI가 응답 헤더로 알려준 **실측** 잔여 토큰. 예전에는 글자수 기반 추정치였다 |

### Response `429 Too Many Requests` — 거절

요청은 어디에도 저장되지 않는다. 사유는 두 가지이고 본문으로 구분된다.

**(1) 큐 혼잡** — 인덱싱 워커는 하나뿐이라 회차를 순서대로 처리한다(화당 약 2분). 대기 중인 회차가 많으면 새 요청을 받지 않는다.

```
HTTP/1.1 429 Too Many Requests
Retry-After: 240
Content-Type: application/problem+json
```
```json
{
  "type": "/errors/queue-full",
  "title": "Queue Full",
  "status": 429,
  "detail": "Indexing queue is full. Retry after the Retry-After period.",
  "queuedEpisodes": 20,
  "estimatedWaitSeconds": 2640
}
```

**(2) 모델 한도 소진** — 같은 OpenAI 모델을 쓰는 다른 작업(설정 오류 탐지)이 한도를 거의 다 썼다. 새 인덱싱을 시작해도 곧 막히므로 받지 않는다.

```json
{
  "type": "/errors/model-rate-limit",
  "title": "Model Rate Limit Exhausted",
  "status": 429,
  "detail": "Model rate limit is nearly exhausted. Retry after the Retry-After period.",
  "remainingTpm": 1200
}
```

**Spring 대응**: 두 경우 모두 같다 — `Retry-After`(초) 대기 후 **같은 요청을 그대로 다시 POST**한다. 성공(201)할 때까지 주기적으로 반복한다.

> **변경 이력**: 예전에는 429 사유가 "TPM 부족" 하나였고, 접수 시점에 원고 글자수로 토큰을 **추정**해 판단했다. 실측 결과 (a) OpenAI 한도는 60초 창이 아니라 연속 충전 버킷이고 (b) 서버가 호출 단위로 동시성을 제한하게 되어 인덱싱이 구조적으로 그 한도를 넘길 수 없어졌다. 정작 부족한 자원은 워커 처리량이었으므로 판단 기준을 **대기 시간**으로 바꿨다. 바뀐 것은 429 본문의 필드뿐이고 `Retry-After` 규약과 재제출 흐름은 그대로다.

### Response `400 Bad Request` — 검증 실패

`episodes`가 빈 배열, `text` 누락, **회차 연속성 위반**(3장) 등. 재시도해도 같은 결과이므로 요청을 고쳐야 한다.

```json
{
  "type": "/errors/invalid-request",
  "title": "Invalid Request",
  "status": 400,
  "detail": "episodes must not be empty"
}
```

## 4. `GET /api/index/jobs/{jobId}` — 진행 상태 조회

### Response `200 OK`

```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": 42,
  "workId": 7,
  "episodes": [
    { "episodeId": 101, "status": "DONE", "error": null },
    { "episodeId": 102, "status": "RUNNING", "error": null }
  ]
}
```

`status`는 화 단위로 붙는다 — "5화 중 3화 완료" 같은 진행률을 이 배열로 표현한다.

| status | 의미 |
|---|---|
| `QUEUED` | 대기 중 (앞 화가 처리 중) |
| `RUNNING` | 인덱싱 진행 중 (화당 약 2분) |
| `DONE` | 완료 |
| `ERROR` | 실패. `error` 필드에 사유 |

- 모든 화가 `DONE` 또는 `ERROR`가 되면 잡은 종료 상태다. 더 이상 변하지 않는다
- **한 화가 실패하면 그 뒤의 화들은 처리하지 않고 `ERROR`로 표기된다** (`error: "Skipped due to preceding episode (6) failure"`). 누적 컨텍스트 의존 때문이다. 실패 원인 해소 후 **실패 화부터 재제출**하면 된다 — 실패한 화는 그래프에 들어가지 않았으므로 연속성 판정에서 여전히 "다음 화"다

### Response `404 Not Found` — 상태 소실

```json
{
  "type": "/errors/not-found",
  "title": "Not Found",
  "status": 404,
  "detail": "indexing job '550e8400-e29b-41d4-a716-446655440000' not found"
}
```

진행 상태는 파이썬 서버 메모리에만 있다. **서버가 재시작되면 상태가 사라지고, 진행 중이던 작업도 함께 중단된다.** 이것은 계약에 포함된 정상 시나리오다:

**Spring 대응**: 404를 받으면 해당 화들을 **다시 POST로 제출**한다. 이미 인덱싱이 끝난 화는 서버가 감지해(5장) 작업 없이 즉시 `DONE` 처리되므로, 재제출은 항상 안전하다 — 중복 작업 없이 수렴한다.

결과적으로 Spring의 예외 대응은 하나로 통일된다: **429든 404든 재시작이든, 대응은 "POST 재제출"이다.**

## 5. 완료 판정과 재제출 안전성

"이 화가 인덱싱됐는가"의 진실은 메모리가 아니라 **Neo4j**에 있다. 인덱싱 파이프라인의 마지막 쓰기는 `Chapter-[:IN_STORY]->Story` 관계 생성(원자적)이므로, 이 관계가 존재하면 해당 화의 모든 단계가 완주한 것이다.

재제출된 화에 대해 서버는 이 마커를 확인하고:

- 마커 있음 → 작업 없이 즉시 `DONE`
- 마커 없음 → 처음부터 재실행

Spring은 이 동작을 신뢰하고, 완료 여부가 불확실할 때 그냥 재제출하면 된다 — **끝난 화를 두 번 인덱싱하는 일은 없다.**

> ⚠️ **알려진 제약**: "부분 산출물 위에 재실행해도 결과가 수렴한다"는 앞선 설명은 정확하지 않았다. 그래프 노드 쓰기가 upsert가 아니라 신규 생성이고, 중복 제거는 이름 기반 병합이 담당하는데 사건(Event)과 인물 상태(CharacterState)는 서술형 이름이라 **일부러 병합하지 않는다**. 그래서 중간에 실패한 화를 다시 인덱싱하면 그 둘이 중복으로 쌓인다(오류는 나지 않고 검색 결과에만 영향). 재제출 계약 자체는 유효하며, 재인덱싱 전 정리 단계는 별도 과제로 다룬다.

## 6. 접수 판단 (참고)

파이썬 서버는 **대기 중인 회차 수**로 접수 여부를 정한다. 워커가 하나뿐이라 회차를 순서대로 처리하므로(화당 약 2분), 대기 회차 × 처리 시간이 상한을 넘으면 429로 거절한다.

- 그 위에 **모델 한도 안전망**이 하나 더 있다. OpenAI 잔여 토큰이 한도의 10% 아래로 떨어지면(주로 설정 오류 탐지가 같은 모델을 쓸 때) 새 인덱싱을 시작하지 않는다
- `remainingTpm`은 OpenAI 응답 헤더에서 읽은 **실측값**이다. 다만 예약이 아니므로 201을 받았어도 처리 중 일시적 rate limit이 날 수 있다 — 이 경우 서버가 내부에서 백오프 재시도하며 Spring이 대응할 일은 없다
- 429의 `Retry-After`는 (1) 큐 혼잡이면 큐가 상한 아래로 빠지는 예상 시점, (2) 한도 소진이면 토큰이 다시 차는 예상 시점이다

## 7. 예시 시나리오

### 7.1 정상 흐름

```
Spring → POST /api/index (6화, 7화)      → 201 { jobId: "abc" }
Spring → GET /api/index/jobs/abc          → 6화 RUNNING, 7화 QUEUED
  (10초 간격 폴링, 화당 ~2분)
Spring → GET /api/index/jobs/abc          → 6화 DONE, 7화 RUNNING
Spring → GET /api/index/jobs/abc          → 6화 DONE, 7화 DONE   ← 종료
```

### 7.2 429 거절 → 재제출

```
Spring → POST /api/index (6화)            → 429, Retry-After: 60
  (60초 대기)
Spring → POST /api/index (6화)            → 201 { jobId: "def" }
  (이후 상태 폴링)
```

### 7.3 서버 재시작 → 재제출 수렴

```
Spring → POST /api/index (6화, 7화)      → 201 { jobId: "abc" }
  (6화 DONE, 7화 RUNNING 중 파이썬 서버 재시작)
Spring → GET /api/index/jobs/abc          → 404
Spring → POST /api/index (6화, 7화)      → 201 { jobId: "ghi" }
Spring → GET /api/index/jobs/ghi          → 6화 DONE (즉시, 스킵됨), 7화 RUNNING
```

## 8. 미확정 사항 (TBD)

| 항목 | 내용 |
|---|---|
| `episodeNo` 필드 | `episodeId`가 회차 순번과 동일하다면 제거 가능. Spring 팀 확인 필요 |
| 서버 주소 | 배포 환경별 base URL |
