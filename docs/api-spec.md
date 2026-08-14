# Lorekeeper Indexing API Spec

| | |
|---|---|
| 버전 | v1 (2026-08-11) |
| 대상 독자 | Spring 서버 개발팀 |
| 범위 | **Indexing API만.** 충돌탐지(설정 오류 검사) API는 완성 후 별도 문서로 제공 |

## 1. 개요

이 서버는 Spring 서버가 호출하는 **내부 파이썬 워커**다. 외부(브라우저·앱)에 직접 노출되지 않으며, 원고를 받아 지식 그래프(Neo4j)로 인덱싱하는 일을 한다.

동작 모델은 단순 REST다:

```
Spring ──POST /api/index──────────▶ Python    인덱싱 제출
                                              ├─ TPM 여유: 201 즉시 응답 후 비동기 처리
                                              └─ TPM 부족: 429 거절 → Spring이 나중에 재제출
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
- **시각**: RFC 3339 UTC (`2026-08-11T03:11:00Z`)
- **에러 본문**: `{ "detail": "사유 문자열", ... }`
- **인증**: 별도 인증 없음. Spring이 이미 인증을 마친 요청만 보내므로 파이썬은 이를 신뢰한다 (이 서버는 외부에 노출되지 않는 내부 서버라는 전제)

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
| `episodes` | array | ✅ | 1개 이상. **`episodeNo` 오름차순으로 정렬해 보낼 것** |
| `episodes[].episodeId` | number | ✅ | Spring 측 회차 식별자 (에코용) |
| `episodes[].episodeNo` | number | ✅ | 회차 순번 |
| `episodes[].text` | string | ✅ | 회차 원고 전문 (평문) |

**처리 순서**: 인덱싱은 이전 회차의 그래프·요약을 배경 컨텍스트로 쓰는 누적 구조라, 배열의 화들은 **순서대로 순차 처리**된다 (병렬 아님). 6화가 인덱싱되지 않은 상태에서 7화만 보내면 6화 컨텍스트 없이 처리되므로, 회차 순서를 지켜 제출해야 한다.

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
| `remainingTpm` | 이 잡 수리 후 남은 TPM **추정치** (6장 참고) |

### Response `429 Too Many Requests` — TPM 부족, 거절

이번 요청을 처리할 TPM 여유가 없다. 요청은 어디에도 저장되지 않는다.

```
HTTP/1.1 429 Too Many Requests
Retry-After: 60
```

```json
{
  "detail": "TPM limit exceeded. Retry after the Retry-After period.",
  "remainingTpm": 12000
}
```

**Spring 대응**: `Retry-After`(초) 대기 후 **같은 요청을 그대로 다시 POST**한다. 성공(201)할 때까지 주기적으로 반복한다.

### Response `400 Bad Request` — 검증 실패

`episodes`가 빈 배열, `text` 누락, `episodeNo` 정렬 위반 등. 재시도해도 같은 결과이므로 요청을 고쳐야 한다.

```json
{ "detail": "episodes must not be empty" }
```

## 4. `GET /api/index/jobs/{jobId}` — 진행 상태 조회

### Response `200 OK`

```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": 42,
  "workId": 7,
  "episodes": [
    { "episodeId": 101, "status": "done", "error": null },
    { "episodeId": 102, "status": "running", "error": null }
  ]
}
```

`status`는 화 단위로 붙는다 — "5화 중 3화 완료" 같은 진행률을 이 배열로 표현한다.

| status | 의미 |
|---|---|
| `waiting` | 대기 중 (앞 화가 처리 중) |
| `running` | 인덱싱 진행 중 (화당 약 2분) |
| `done` | 완료 |
| `error` | 실패. `error` 필드에 사유 |

- 모든 화가 `done` 또는 `error`가 되면 잡은 종료 상태다. 더 이상 변하지 않는다
- **한 화가 실패하면 그 뒤의 화들은 처리하지 않고 `error`로 표기된다** (`error: "Skipped due to preceding episode (6) failure"`). 누적 컨텍스트 의존 때문이다. 실패 원인 해소 후 실패 화부터 재제출하면 된다

### Response `404 Not Found` — 상태 소실

진행 상태는 파이썬 서버 메모리에만 있다. **서버가 재시작되면 상태가 사라지고, 진행 중이던 작업도 함께 중단된다.** 이것은 계약에 포함된 정상 시나리오다:

**Spring 대응**: 404를 받으면 해당 화들을 **다시 POST로 제출**한다. 이미 인덱싱이 끝난 화는 서버가 감지해(5장) 작업 없이 즉시 `done` 처리되므로, 재제출은 항상 안전하다 — 중복 작업 없이 수렴한다.

결과적으로 Spring의 예외 대응은 하나로 통일된다: **429든 404든 재시작이든, 대응은 "POST 재제출"이다.**

## 5. 완료 판정과 재제출 안전성

"이 화가 인덱싱됐는가"의 진실은 메모리가 아니라 **Neo4j**에 있다. 인덱싱 파이프라인의 마지막 쓰기는 `Chapter-[:IN_STORY]->Story` 관계 생성(원자적)이므로, 이 관계가 존재하면 해당 화의 모든 단계가 완주한 것이다.

재제출된 화에 대해 서버는 이 마커를 확인하고:

- 마커 있음 → 작업 없이 즉시 `done`
- 마커 없음 → 처음부터 재실행 (중간에 죽었던 화의 부분 산출물 위에 재실행해도 결과가 수렴한다)

Spring은 이 동작을 신뢰하고, 완료 여부가 불확실할 때 그냥 재제출하면 된다.

## 6. TPM 판단 (참고)

파이썬 서버는 LLM 토큰 사용량을 분 단위 슬라이딩 윈도우로 추적한다. 새 요청의 예상 토큰이 남은 여유를 넘으면 429로 거절한다.

- `remainingTpm`은 **추정치**다. 예약이 아니므로, 201을 받았어도 실제 처리 중 일시적 rate limit이 발생할 수 있다 — 이 경우 서버 내부에서 백오프 재시도하며, Spring이 대응할 일은 없다
- 429의 `Retry-After`는 윈도우가 회복되는 예상 시점이다

## 7. 예시 시나리오

### 7.1 정상 흐름

```
Spring → POST /api/index (6화, 7화)      → 201 { jobId: "abc" }
Spring → GET /api/index/jobs/abc          → 6화 running, 7화 waiting
  (10초 간격 폴링, 화당 ~2분)
Spring → GET /api/index/jobs/abc          → 6화 done, 7화 running
Spring → GET /api/index/jobs/abc          → 6화 done, 7화 done   ← 종료
```

### 7.2 TPM 거절 → 재제출

```
Spring → POST /api/index (6화)            → 429, Retry-After: 60
  (60초 대기)
Spring → POST /api/index (6화)            → 201 { jobId: "def" }
  (이후 상태 폴링)
```

### 7.3 서버 재시작 → 재제출 수렴

```
Spring → POST /api/index (6화, 7화)      → 201 { jobId: "abc" }
  (6화 done, 7화 running 중 파이썬 서버 재시작)
Spring → GET /api/index/jobs/abc          → 404
Spring → POST /api/index (6화, 7화)      → 201 { jobId: "ghi" }
Spring → GET /api/index/jobs/ghi          → 6화 done (즉시, 스킵됨), 7화 running
```

## 8. 미확정 사항 (TBD)

| 항목 | 내용 |
|---|---|
| `episodeNo` 필드 | `episodeId`가 회차 순번과 동일하다면 제거 가능. Spring 팀 확인 필요 |
| 서버 주소 | 배포 환경별 base URL |
