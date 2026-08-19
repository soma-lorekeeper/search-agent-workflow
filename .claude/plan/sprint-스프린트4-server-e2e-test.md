# lorekeeper-ai 서버 End-to-End 테스트 플랜

| | |
|---|---|
| 부모 이슈 | [LOREKEEPER-261](https://lorekeepers.atlassian.net/browse/LOREKEEPER-261) Refactoring python server |
| Jira 반영 | 플랜 승인 시 **서브태스크 신규 생성** → `진행 중` 전환. 테스트 완료 시 결과 요약 기재 + `완료` 전환 |
| 작성일 | 2026-08-19 |
| 갱신 | 2026-08-19 1차 실행 완료([LOREKEEPER-277](https://lorekeepers.atlassian.net/browse/LOREKEEPER-277), 보고서 `.claude/docs/e2e-test-report-2026-08-19.md`). 이후 버그①(@validate_call)·②(chat responses 전환+reasoning high)·③(entity_search 별칭 형식) 수정과 env 키 개명(`CHAT_MODEL`/`EXTRACTION_MODEL`), chat 도구 `kg_` 접두 제거를 반영해 4-3 기대값 수정 + 4-4 회귀 케이스 신설 |

## Context

리팩터링(레이어 분리·테넌트 격리·camelCase 통일·글로벌 예외 핸들러)이 끝났지만 검증은 전부 인프로세스 테스트(LLM·DB 가짜)였다. 실서버 + 실 Neo4j/PostgreSQL + 실 LLM으로 세 API(index·detect·chat)와 health의 정상/에러 계약을 처음으로 끝까지 확인한다. 부하 테스트(세마포어 포화)는 이번 범위에서 제외. LLM 호출은 최소화한다 — 사용자 확정: **ch1~2 2화만 인덱싱 + 자작 모순 초고로 detect**(GT 품질 대조는 하지 않음, 기능 계약 검증이 목적).

## 사전 조사 핵심 사실 (코드 확인)

- **입력**: `data/input_ch{1..32}.txt` = 파일 하나가 회차 하나(1행 `[N화]`). ch1이 2,966자로 가장 짧고 ch2는 4,949자.
- **Postgres 스키마는 레포에 없다**(Spring 소유, CREATE TABLE 0건). 파이썬이 닿는 테이블은 4개뿐: `detection_jobs`/`detection_findings`(쓰기, `src/repository/postgres/detection.py`), `episodes`/`works`(chat 도구가 읽기, `src/service/chat/tools.py:151,186`). E2E용 최소 스키마를 직접 만든다(탐사에서 컬럼·타입 도출 완료, `docs/detecting-api-spec.md:224-256`의 새 스키마 반영).
- **detect는 DB 없이도 끝까지 돈다** — repository가 예외를 전부 삼키고 로그만 남긴다(`detection.py:85-97`). DB 기록 검증은 행을 미리 INSERT하고 완료 후 SELECT + `탐지 작업 행이 없다` 경고 부재로 확인해야 한다.
- **미검증 경로**: `line_ids`/`cited`를 `json.dumps` 문자열로 jsonb 컬럼에 INSERT하는 경로는 실 DB로 한 번도 확인된 적 없음(기존 테스트 전부 DB 가짜) — 이번에 반드시 확인.
- **에러를 LLM 0회로 유발하는 노브**(전부 모듈 로드 시점에 읽으므로 서버 기동 전 env로 주입, 쉘 env가 .env를 이김):
  - detect 429 too-many-detections: `MAX_CONCURRENT_DETECTS=0` → 첫 요청부터 429
  - index 429 queue-full: `INDEX_EPISODE_SECONDS=1` + `INDEX_MAX_WAIT_SECONDS=1` + 2건 연속 POST (선행 1건은 `OPENAI_API_KEY=sk-invalid`로 401 즉사 → 과금 0, 덤으로 인덱싱 ERROR 전이 검증)
  - index/detect 429 model-rate-limit: `ADMISSION_MIN_HEADROOM_RATIO=1.1` (항상 거절, 단 정상 경로도 막히므로 별도 프로세스)
  - `INDEX_TPM_LIMIT`은 죽은 변수(코드가 안 읽음) — 노브로 쓸 수 없고, `.env.example`의 낡은 문서화로 보고서에 기재할 발견 사항. **(2026-08-19 해소: `.env.example`에서 삭제 + 전 변수 참조 대조 완료)**
- index 400 5종은 전부 LLM 0회: 빈 episodes / 빈 text / 비연속 episodeNo(중복 포함) / 그래프 다음 화 불일치 / 단일 요청 자체가 대기 상한 초과(기본 노브로 21화 묶음).
- 인덱싱 LLM 비용: 회차당 완결 3회(추출+회차요약+전역요약, 길이 무관 고정) + 병합 α + 임베딩(100자 청크 수 + Fact 수). 인덱싱 동안 서버 전체가 블로킹됨(동기 드라이버) — 폴링 타임아웃 여유 필요.
- chat: KG 도구를 부르게 하려면 인덱싱 선행 필수(인덱싱 0화면 시스템 프롬프트가 KG 조회를 금지). 첫 턴은 LLM +1회(제목). `episode_manuscript`/`work_settings`는 Postgres 시드 필요.
- Neo4j 초기화: 삭제 코드 없음. **테넌트(workId)를 시나리오마다 새로 쓰는 방식**으로 격리(삭제 불필요). 단 서버 메모리 큐 카운트는 테넌트 무관 전역.

## 실행 단계

### 0. Jira 서브태스크 생성 (승인 직후)

- LOREKEEPER-261 하위에 Subtask **"서버 end-to-end 테스트"** 생성 — 설명에 테스트 범위(3 API 정상+에러, 부하 제외, LLM 최소화)와 이 플랜 문서 경로 기재.
- 상태 `해야 할 일` → `진행 중` 전환.
- 플랜 문서를 `.claude/plan/sprint-스프린트4-server-e2e-test.md`로 저장.

### 1. 환경 준비 (LLM 0회)

- 실행 중인 8000 서버는 그대로 사용(기본 노브 + 실키). Neo4j·Postgres 컨테이너는 이미 healthy.
- Postgres에 E2E 스키마 생성 + 시드(psql):
  - `detection_jobs`(job_id varchar(36) PK, status, claim_count, contradiction_count, detail, updated_at, completed_at + CHECK `status='DONE' OR (contradiction_count=0 AND claim_count IS NULL)`)
  - `detection_findings`(9컬럼 + `(job_id, seq)` UNIQUE + FK CASCADE) — detecting 스펙 마이그레이션 반영 형태
  - `works(id, title)` + `episodes(work_id, episode_number, title, content)` — chat 도구용. ch1·ch2 원문을 content로 시드
- 검사용 자작 초고 작성: ch1·ch2를 읽고 명시 설정 2~3개를 뒤집는 3화 초고(약 800~1,200자)를 scratchpad에 작성(자작이므로 `data/`에 넣지 않음).

### 2. 무료 에러·계약 케이스 — 메인 서버(:8000), LLM 0회

| # | 케이스 | 기대 |
|---|---|---|
| F1 | `GET /api/health` | 200, `status:ok`, neo4j/postgres `ok:true`, `latencyMs` 키 |
| F2 | index/detect/chat 필수 필드 누락 3건 | 422 problem+json `/errors/validation`, `errors[].loc`이 camelCase |
| F3 | index 빈 `episodes` | 400 `/errors/invalid-request`, "must not be empty" |
| F4 | index 빈 `text` | 400 "must have text" |
| F5 | index episodeNo 비연속(`[1,3]`)·중복(`[1,1]`) | 400 "must be consecutive" |
| F6 | 빈 그래프에 2화부터 제출 | 400 "expected episodeNo 1" |
| F7 | 21화 묶음 한 방(기본 노브 21×120>2400) | 400 "can never be accepted", **Retry-After 헤더 없음** |
| F8 | `GET /api/index/jobs/{모르는id}` / `GET /api/detect/jobs/{모르는id}` | 404 `/errors/not-found` |
| F9 | 없는 경로 GET / `DELETE /api/chat` | 404/405 `type: about:blank` |

### 3. 노브 서버 에러 케이스 — 별도 프로세스 2개, LLM 0회 (401 1회 제외)

**서버 A(:8001)** — `MAX_CONCURRENT_DETECTS=0 INDEX_EPISODE_SECONDS=1 INDEX_MAX_WAIT_SECONDS=1 OPENAI_API_KEY=sk-invalid`:

| # | 케이스 | 기대 |
|---|---|---|
| K1 | detect 제출 | 429 `/errors/too-many-detections`, `runningDetections`, `Retry-After` |
| K2 | index 1화 POST(짧은 텍스트) 직후 2화 POST | 두 번째가 429 `/errors/queue-full`, `queuedEpisodes`+`estimatedWaitSeconds` |
| K3 | K2의 첫 잡 폴링 | 잘못된 키로 401 즉사 → `episodes[].status: ERROR` + error 문자열(실패 전이 검증) |

**서버 B(:8002)** — `ADMISSION_MIN_HEADROOM_RATIO=1.1`:

| # | 케이스 | 기대 |
|---|---|---|
| K4 | index 정상 모양 제출 | 429 `/errors/model-rate-limit`, `remainingTpm`, `Retry-After` |
| K5 | detect 정상 모양 제출 | 429 `/errors/model-rate-limit` |

종료 후 두 프로세스 정리.

### 4. 실 LLM 경로 — 메인 서버(:8000), 테넌트 userId=42/workId=7

**4-1 인덱싱** (완결 ~6-8회 + 임베딩 ~150회, 약 4~5분)
- `POST /api/index` — ch1+ch2를 **한 요청 2화 묶음**으로: 201, `jobId/requestedAt/remainingTpm` 필드 확인
- 폴링(10초 간격): episodes[].status `QUEUED→RUNNING→DONE` 전이, 인덱싱 중 health 응답 지연(서버 블로킹) 관찰만
- 완료 후 cypher-shell로 `tenant_id='42:7'` 노드 존재·Chapter 2개 확인
- **덤 에러 케이스**: 같은 테넌트에 ch1 재제출 → 400(다음 화는 3화), ch4 제출 → 400
- chat 사전조건 확보: indexed episodes = [1,2]

**4-2 detect** (추출 1 + 판정 1 + 임베딩 수십 회, 약 1~2분)
- `detection_jobs`에 job 행 사전 INSERT(`status='QUEUED'`)
- `POST /api/detect`(자작 3화 초고, episodeNumber=3) → 202 → 폴링: `phase` EXTRACT→RETRIEVE→JUDGE, `claimCount` 등장 시점, `findings` null↔[] 구분
- DONE 후: findings 스키마(camelCase 8필드, `score` 미노출, `cited[].episodeNo ≤ 2`), 심어둔 모순 검출 여부(참고용, 실패로 안 침)
- **DB 검증**: `detection_jobs` 행이 DONE/claim_count/contradiction_count로 갱신됐는지, `detection_findings`에 행이 실제 INSERT됐는지(jsonb 캐스팅 경로), 서버 로그에 `탐지 작업 행이 없다`/`저장 실패` 경고가 없는지
- 같은 jobId 재제출 → 202 + 현재 상태(재실행 안 함)

**4-3 chat** (LLM 5회: 첫 턴 3 + 둘째 턴 2)
- 턴 1(첫 턴): "이 작품 제목이 뭐야?" → `work_settings` 호출 + `suggestedTitle` 채워짐 확인
- 턴 2(messages 3개): "1화에서 무슨 일이 있었는지 요약해줘" → KG 도구 사용(`hybrid_search`/`fact_search`/`entity_search` — 2026-08-19 후속 수정으로 `kg_` 접두 제거됨) + `toolCalls[].status: DONE`, `suggestedTitle: null`
- `entity_search`가 포함되면 `FAILED`가 아니어야 한다(버그③ 수정 후 기대값 — 최초 실행 때는 FAILED가 알려진 결함이었음)
- 응답 키 전부 camelCase 확인

**4-4 수정 회귀 케이스** (2026-08-19 버그 수정 반영 — **실행 완료, R1~R3 전부 통과**)

| # | 케이스 | 기대 | 비용 | 결과 |
|---|---|---|---|---|
| R1 | 인덱싱 완주 자체 (4-1과 동일 실행) | `TenantTaggingWriter`의 `@validate_call` 회귀 감지 — 추출 후 `'dict' object has no attribute 'nodes'`류 ERROR가 없어야 함 (버그①) | 4-1에 포함 | ✅ 새 테넌트 42×8에 ch1 인덱싱 DONE(3분 30초), 60노드·미태깅 0·traceback 0 |
| R2 | `CHAT_MODEL=gpt-5.6-luna`로 서버 재기동 후 도구 사용 chat 1턴 | 200 (버그② 회귀 — chat/completions 시절엔 이 조합이 항상 500). responses + `reasoning high` 경로 검증 | LLM 2~3회 | ✅ 200 / 10.9초, `entity_search` DONE, camelCase 유지 |
| R3 | `entity_search` 도구 직접 실행(서버·LLM 불필요, `build_retrieval_tools`로 직접 호출): ① 본명 ② **혼종 별칭**(리스트 원소 안 쉼표 나열, 예: `["독자님, 독자 씨", "독자님"]`의 "독자 씨") ③ 없는 이름 | ① ② 결과 반환, ③ 예외 없이 0건 (버그③ 회귀 — 별칭 저장 3형식: 쉼표 문자열/리스트/혼종 전부 매칭돼야 함) | 0회 | ✅ 4케이스(문자열 별칭 `작가님` 추가) 전부 통과, 예외 0 |

> ⚠️ **실행 시 주의**: 회귀 케이스는 반드시 **수정 코드로 기동한 서버**에 쏜다. 2차 실행 1차 시도에서 서버가 `retrieval.py` 수정 이전에 기동해 있어 `entity_search`가 `FAILED`로 나왔다 — 수정 실패로 오판하기 쉽다. 대상 파일 mtime과 서버 기동 시각을 먼저 대조할 것.

### 5. 보고서 + Jira 완료 처리

- 보고서: `.claude/docs/e2e-test-report-2026-08-19.md` — 케이스별 표(케이스/기대/실제/판정), **발견된 오류는 API·케이스 단위로 원인 분석과 함께 기재**(현시점 알려진 후보: `.env.example`의 죽은 `INDEX_TPM_LIMIT` 문서화). 오류 0건이면 0건이라고 명시.
- Jira 서브태스크: 설명에 결과 요약(통과/실패 수, 발견 오류) 덧붙이고 `완료` 전환. **발견 오류의 수정은 이번 범위 밖** — 별도 요청을 기다림.
- 노브 서버 프로세스 정리 확인. 메인 서버(:8000)는 유지.

## 리스크 / 주의

- **인덱싱 중 서버 블로킹**: 폴링이 한동안 응답을 못 받는 것은 알려진 제약(readme 명시)이지 오류가 아님 — 보고서에 관찰 사실로만 기재.
- **detect findings 0건 가능성**: 2화 근거로는 모순이 안 잡힐 수 있음. 계약 검증(스키마·상태 전이·DB 기록)이 목적이므로 검출 실패 자체는 오류로 기재하지 않되 참고로 남김.
- **비용**: 완결 호출 ~15회 + 임베딩 ~200회 + 401 1회 — 대략 $0.2~0.5 예상.
- 테스트 도중 서버 재시작이 필요한 발견(예: 메모리 상태 오염)이 나오면 테넌트를 새 workId로 올려 재시도.

## 검증 방법 (플랜 자체의 완료 기준)

1. 위 표의 전 케이스가 실행되고 결과가 보고서에 기재됨
2. 보고서에 각 API별 발견 오류(또는 "없음")가 명시됨
3. Jira 서브태스크가 결과 요약과 함께 `완료` 상태
