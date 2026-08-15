# Lorekeeper Detection API Spec

| | |
|---|---|
| 버전 | v1 (2026-08-15) |
| 대상 독자 | Spring 서버 개발팀 |
| 범위 | **설정 오류 탐지 API만.** 인덱싱 API는 `docs/indexing-api-spec.md` 참고 |

## 1. 개요

작가가 새로 쓴 회차가 기존 설정과 어긋나는지 검사한다. 인덱싱과 같은 내부 파이썬 워커가 담당하고, 동작 모델도 같은 REST + 폴링이다.

```
Spring ──POST /api/detect──────────▶ Python    검사 제출 (202 즉시 응답 후 비동기)
Spring ──GET /api/detect/jobs/{id}─▶ Python    진행 상태 폴링
                                     Python ──▶ PostgreSQL  결과를 detection_jobs/findings에 기록
```

인덱싱과 두 가지가 다르다.

| | 인덱싱 | 탐지 |
|---|---|---|
| `jobId` 발급 | **파이썬**이 UUID 발급 | **Spring**이 발급해 요청에 실어 보냄 |
| 결과 저장 | 없음(그래프가 결과) | **파이썬이 Spring의 테이블에 직접 기록** |

jobId를 Spring이 발급하는 이유는 검사 요청 하나가 회차 하나라 호출자가 부여한 id를 그대로 쓰는 편이 단순하고, 파이썬이 재시작해 진행 중이던 검사를 잃어도 "무엇을 언제 맡겼는지"가 Spring 쪽에 남기 때문이다.

한 회차 검사는 **1~2분** 걸린다. 상태 폴링 간격은 5초를 권장한다.

## 2. 공통 규약

- **소설 식별**: `userId` × `workId` 조합이 소설 한 편을 unique하게 구분한다(인덱싱과 같은 규약). 지식 그래프가 이 조합을 테넌트 키로 삼아 소설별로 격리돼 있으므로, **인덱싱할 때와 같은 값을 보내야 그 그래프를 찾는다.**
- **`episodeNumber`**: 검사 대상 회차의 순번. 이 회차 **직전까지**의 설정만 대조 대상이 된다 — 없으면 회차가 자기 자신이 만든 사실과 대조돼 "일치"로 자평하고, 뒤에 나올 반전을 이미 심어둔 모순으로 읽는다.
- **필드 표기**: camelCase
- **status 어휘**: `QUEUED` / `RUNNING` / `DONE` / `ERROR` (DB의 `detection_jobs.status`와 같은 값)
- **시각**: RFC 3339 UTC
- **에러 본문**: `{ "detail": "사유 문자열" }`
- **인증**: 없음(인덱싱과 동일한 전제)

## 3. `POST /api/detect` — 검사 제출

### Request

```json
{
  "jobId": "8f2c1e6a-3d5b-4a91-9c77-1b0e4d2a6f38",
  "userId": 42,
  "workId": 7,
  "episodeNumber": 6,
  "text": "6화 원고 전문…"
}
```

| 필드 | 타입 | 설명 |
|---|---|---|
| `jobId` | string(36) | Spring이 발급한 UUID. 상태 폴링과 결과 기록의 키 |
| `userId` | int | 소설 소유자 |
| `workId` | int | 작품 |
| `episodeNumber` | int | 검사 대상 회차 순번 |
| `text` | string | 회차 원고 전문 |

### Response `202 Accepted`

```json
{ "jobId": "8f2c1e6a-…", "status": "QUEUED" }
```

즉시 응답하고 검사는 백그라운드로 진행된다. **같은 `jobId`로 다시 보내면 재실행하지 않고** 현재 상태를 그대로 돌려준다 — 회차 하나 검사에 LLM을 수십 번 부르므로 중복 실행 비용이 크다.

### Response `429 Too Many Requests` — 거절

> **신규.** 이전 버전에는 없던 응답이다. Spring 쪽 대응 구현이 필요하다.

검사를 시작할 여유가 없다. 요청은 어디에도 저장되지 않으며, 그 `jobId`는 조회해도 404다(접수된 적이 없다).

**중복 제출은 이 검사를 거치지 않는다.** 이미 접수한 `jobId`를 다시 보내면 서버가 아무리 바빠도 202로 현재 상태를 돌려준다 — 재제출은 자원을 쓰지 않기 때문이다. 429는 **새 검사**에만 나온다.

사유는 두 가지이고 본문으로 구분된다.

**(1) 동시 검사 초과** — 검사는 큐 없이 전부 동시에 돌기 때문에 진행 중인 수 자체를 제한한다.

```
HTTP/1.1 429 Too Many Requests
Retry-After: 90
```
```json
{
  "detail": "Too many detections in progress. Retry after the Retry-After period.",
  "runningDetections": 4
}
```

**(2) 모델 한도 소진** — 같은 OpenAI 모델을 쓰는 인덱싱이 한도를 거의 다 썼다.

```json
{
  "detail": "Model rate limit is nearly exhausted. Retry after the Retry-After period.",
  "remainingTpm": 1200
}
```

**Spring 대응**: `Retry-After`(초) 대기 후 **같은 요청을 그대로 다시 POST**한다. 인덱싱 API의 429와 같은 규약이다(`docs/indexing-api-spec.md` 3장).

> **왜 생겼나**: 탐지는 원래 접수 제한이 없어 요청이 오는 대로 전부 동시에 실행했다. 검사 하나가 claim 수십 건 × 4채널의 검색과 27k 토큰짜리 판정을 하므로, 몇 건만 겹쳐도 OpenAI 한도를 순식간에 먹는다. 게다가 탐지와 인덱싱이 **같은 모델**을 쓰게 되면서 한쪽만 자제하고 다른 쪽은 무제한인 비대칭이 생겼다.

## 4. `GET /api/detect/jobs/{jobId}` — 진행 상태 조회

### Response `200 OK` — 진행 중

```json
{
  "jobId": "8f2c1e6a-…",
  "status": "RUNNING",
  "phase": "RETRIEVE",
  "claimCount": 116,
  "contradictionCount": 0,
  "findings": null,
  "detail": null
}
```

`phase`는 검사가 어느 단계인지 알린다.

| phase | 하는 일 |
|---|---|
| `EXTRACT` | 원고에서 검증 대상 주장(claim)을 뽑는다 |
| `RETRIEVE` | claim마다 그래프에서 대조할 근거를 찾는다 |
| `JUDGE` | 근거와 대조해 판정한다 |

`claimCount`는 `EXTRACT`가 끝나야 값이 생긴다(그 전에는 `null`).

**`findings`가 `null`인 것과 `[]`인 것은 다르다.** `null`은 "아직 판정 전", `[]`는 "검사했고 오류가 없다"이다. 진행 중에 빈 배열을 받으면 폴링을 멈추고 "오류 0건"으로 확정하게 되므로 이 구분이 중요하다.

### Response `200 OK` — 완료

```json
{
  "jobId": "8f2c1e6a-…",
  "status": "DONE",
  "phase": null,
  "claimCount": 116,
  "contradictionCount": 2,
  "findings": [
    {
      "claimId": "P37",
      "quote": "이현우는 왼팔의 낙인을 문질렀다",
      "axis": "이현우의 왼팔 상태",
      "value": "낙인이 있음",
      "lineIds": [91],
      "isError": true,
      "reason": "3화에서 이현우는 왼팔을 잃었다(C012). 낙인을 문지를 팔이 없다.",
      "cited": [{ "episodeNo": 3, "chunkIndex": 12 }]
    }
  ],
  "detail": null
}
```

**`findings`에는 오류로 판정된 claim만 담긴다.** 일치하거나 근거가 없어 판단할 수 없는 claim은 오지 않는다 — 회차당 claim이 100건을 넘는데 전부 내보내면 화면이 쓰지 않는 데이터가 대부분이 된다. 검사한 총량은 `claimCount`가 말해준다.

| 필드 | 설명 |
|---|---|
| `claimId` | `P1`~`PN`. 번호는 **원고 등장 순서**라 화면 정렬에 그대로 쓸 수 있다 |
| `quote` | 원고에서 문제가 된 서술 그대로 |
| `axis` / `value` | 무엇에 대한 주장이고 그 값이 무엇인지 |
| `lineIds` | 원고 줄 번호. 화면이 본문 위에 하이라이트를 건다 |
| `isError` | 지금은 항상 `true`. 임계값을 옮기거나 "의심" 등급을 더해도 계약이 안 바뀌도록 필드로 둔다 |
| `reason` | 판정 근거 문장 |
| `cited` | 근거 원문의 좌표 `{episodeNo, chunkIndex}`. 화면이 그 조각을 열 수 있다 |

`contradictionCount`는 `findings`의 길이와 같다. 목록 화면이 상세를 받지 않고도 개수를 보여줄 수 있게 함께 싣는다.

### Response `200 OK` — 실패

```json
{
  "jobId": "8f2c1e6a-…",
  "status": "ERROR",
  "phase": null,
  "claimCount": null,
  "contradictionCount": 0,
  "findings": null,
  "detail": "OpenAI 크레딧 소진"
}
```

### Response `404 Not Found` — 상태 소실

```json
{ "detail": "'8f2c1e6a-…' 탐지 작업 기록이 없습니다." }
```

진행 상태는 파이썬 프로세스 메모리에만 있다. 재시작하면 사라지고 진행 중이던 검사도 함께 끊긴다. **404는 계약된 정상 시나리오다** — Spring은 자기 `detection_jobs` 행을 보거나, 같은 `jobId`로 다시 `POST`하면 된다(재실행이 이전 결과를 덮어쓴다).

## 5. 결과 저장 — 파이썬이 Spring의 테이블에 직접 쓴다

검사가 끝나면 파이썬이 `detection_jobs` / `detection_findings`에 직접 기록한다. **테이블과 작업 행은 Spring이 만든다** — 파이썬은 자기 `jobId`의 행을 갱신하고 결과를 넣기만 하고, 행을 새로 만들지 않는다.

| 시점 | 파이썬이 하는 일 |
|---|---|
| 검사 시작 | `detection_jobs.status = 'RUNNING'` |
| 완료 | 기존 findings 삭제 → 새 findings INSERT → `status='DONE'`, `claim_count`, `contradiction_count`, `completed_at` (한 트랜잭션) |
| 실패 | `status='ERROR'`, `detail`, `completed_at` |

완료 시 findings를 먼저 지우는 것은 **같은 `jobId` 재실행** 때문이다. 파이썬이 재시작해 Spring이 404를 보고 다시 보내면, 이전 실행이 남긴 행 때문에 `(job_id, seq)` UNIQUE 제약이 INSERT를 거부한다.

DB 쓰기가 실패해도 검사 자체는 실패로 만들지 않는다 — 결과는 메모리에 있고 조회로 받아갈 수 있다.

### `detection_findings` 스키마 변경 필요

새 파이프라인의 결과 모양이 기존 컬럼과 다르다. 아래 마이그레이션이 **배포 전에** 적용돼야 한다.

```sql
-- 오류만 저장하므로 finding_count는 contradiction_count와 항상 같다 — 한쪽은 중복이다.
ALTER TABLE detection_jobs
    DROP CONSTRAINT ck_detection_jobs_contradiction_within_findings,
    DROP CONSTRAINT ck_detection_jobs_counts_not_negative,
    DROP CONSTRAINT ck_detection_jobs_counts_by_status,
    DROP COLUMN finding_count;
ALTER TABLE detection_jobs
    ADD CONSTRAINT ck_detection_jobs_contradiction_count_not_negative
        CHECK (contradiction_count >= 0),
    ADD CONSTRAINT ck_detection_jobs_counts_by_status
        CHECK (status = 'DONE' OR (contradiction_count = 0 AND claim_count IS NULL));

-- FK를 UUID로 통일한다(지금은 bigint FK라 이름이 같은 job_id가 서로 다른 것을 가리킨다).
ALTER TABLE detection_findings DROP COLUMN job_id;
ALTER TABLE detection_findings
    ADD COLUMN job_id varchar(36) NOT NULL
        REFERENCES detection_jobs (job_id) ON DELETE CASCADE;
DROP INDEX ux_detection_findings_job_id_seq;
CREATE UNIQUE INDEX ux_detection_findings_job_id_seq ON detection_findings (job_id, seq);

-- 결과 컬럼 교체. 오류만 저장하므로 label은 전 행이 CONTRADICTION이 되어 무의미하다.
ALTER TABLE detection_findings
    DROP CONSTRAINT ck_detection_findings_label,
    DROP COLUMN category, DROP COLUMN label,
    DROP COLUMN established_fact, DROP COLUMN source_episode;
ALTER TABLE detection_findings RENAME COLUMN explanation TO reason;
ALTER TABLE detection_findings
    ADD COLUMN axis     varchar(128) NOT NULL,
    ADD COLUMN value    text         NOT NULL,
    ADD COLUMN line_ids jsonb        NOT NULL,
    ADD COLUMN cited    jsonb        NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN score    integer;
```

`score`는 내부 진단용이라 API 응답에는 싣지 않는다.

## 6. 검사 품질 (참고)

평가셋(오류를 심은 초고 5편, 총 25건)에서 잰 값이다.

| 지표 | 값 |
|---|---|
| 검출 | 25건 중 22건 |
| 오탐(오류를 심지 않은 회차) | 0건 (4회 반복, claim 700건 중 임계값 이상 0) |
| 편당 비용 | 약 $0.03 |

검출되지 않는 3건은 근거가 그래프에 아예 없는 유형이다(인덱싱이 그 사실을 담지 못한 경우). 임계값 곡선이 2~8 구간에서 평평해 운영점은 7로 잡았다.

## 7. 예시 시나리오

### 7.1 정상 흐름

```
Spring: POST /api/detect {jobId:"8f2c…", userId:42, workId:7, episodeNumber:6, text:"…"}
Python: 202 {jobId:"8f2c…", status:"QUEUED"}

  (5초 후) GET /api/detect/jobs/8f2c… → {status:"RUNNING", phase:"EXTRACT", claimCount:null}
  (30초 후)                           → {status:"RUNNING", phase:"RETRIEVE", claimCount:116}
  (80초 후)                           → {status:"DONE", claimCount:116, contradictionCount:2, findings:[…]}
```

### 7.2 서버 재시작 → 재제출 수렴

```
Spring: POST /api/detect {jobId:"8f2c…", …} → 202
        (파이썬 재시작 — 진행 중이던 검사 유실)
Spring: GET /api/detect/jobs/8f2c… → 404
Spring: POST /api/detect {jobId:"8f2c…", …}  ← 같은 jobId로 그대로 재제출
Python: 202 → 처음부터 다시 검사, 결과가 같은 행을 덮어쓴다
```

## 8. 미확정 사항 (TBD)

- **배포 순서**: `detection_findings` 마이그레이션과 파이썬 배포가 함께 움직여야 한다. 마이그레이션 전에 새 엔진이 쓰면 컬럼 불일치로 INSERT가 실패한다.
- **기존 그래프 재인덱싱**: 테넌트 격리를 넣으면서 Chunk의 식별 체계가 바뀌었다. 기존에 인덱싱된 그래프는 테넌트 표시가 없어 검색에 걸리지 않으므로, 전환 시 그래프를 비우고 재인덱싱해야 한다.
