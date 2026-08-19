# lorekeeper-ai 서버 E2E 테스트 보고서 (2026-08-19)

| | |
|---|---|
| Jira | [LOREKEEPER-277](https://lorekeepers.atlassian.net/browse/LOREKEEPER-277) (부모: LOREKEEPER-261) |
| 환경 | 로컬 uvicorn(:8000) + docker Neo4j 2026.07.1 + PostgreSQL 17.11(Spring 실스키마 존재) + 실 OpenAI |
| 범위 | index·detect·chat·health 정상/에러 계약. 부하 테스트 제외 |
| 입력 | `data/input_ch1.txt`(1화), `data/input_ch2.txt`(2화), 자작 모순 초고(3화, 모순 3개 심음) |
| LLM 비용 | 완결 호출 ~15회(인덱싱 2화 + detect 1건 + chat 2턴) + 임베딩 ~200회 + 의도된 401 1회 |
| 결과 | **28개 케이스 중 27개 계약대로 동작. 버그 3건 발견(1건은 진행을 위해 수정 적용, 2건 미수정), 문서 결함 1건** |
| 2차(회귀) | 2026-08-19 저녁, 버그①②③ 수정 후 회귀 케이스 R1~R3 실행 — **전부 통과**. 4장 참고 |

## 1. 케이스별 결과

### 1-1. 무료 계약 케이스 — 기본 노브 서버 (LLM 0회)

| # | 케이스 | 기대 | 결과 |
|---|---|---|---|
| F1 | GET /api/health | 200 ok, `latencyMs` | ✅ neo4j 4.2ms / postgres 5.6ms |
| F2 | index/detect/chat 필수 필드 누락 | 422 `/errors/validation`, `errors[].loc` camelCase | ✅ 3건 모두 (loc가 `workId`/`sessionId` 등 alias로 보고됨) |
| F3 | index 빈 episodes | 400 `/errors/invalid-request` | ✅ |
| F4 | index 빈 text | 400 | ✅ |
| F5 | index episodeNo 비연속 `[1,3]`·중복 `[1,1]` | 400 "must be consecutive" | ✅ 2건 (중복도 같은 메시지로 걸림) |
| F6 | 빈 그래프에 2화부터 | 400 "expected episodeNo 1" | ✅ |
| F7 | 21화 묶음(자체 대기 상한 초과) | 400, Retry-After 없음 | ✅ (2520s > 2400s 계산 정확) |
| F8 | index/detect 모르는 jobId 조회 | 404 `/errors/not-found` | ✅ 2건 |
| F9 | 없는 경로 / DELETE /api/chat | 404·405 `about:blank` | ✅ (405에 `Allow: POST` 헤더 포함) |

전 에러 응답의 `Content-Type: application/problem+json` 확인.

### 1-2. 노브 서버 에러 케이스 (LLM 0회, 의도된 401 1회)

서버 A: `MAX_CONCURRENT_DETECTS=0 INDEX_EPISODE_SECONDS=1 INDEX_MAX_WAIT_SECONDS=1 OPENAI_API_KEY=sk-invalid`

| # | 케이스 | 기대 | 결과 |
|---|---|---|---|
| K1 | detect 첫 제출 | 429 `/errors/too-many-detections` + `runningDetections` + Retry-After 90 | ✅ |
| K2 | index 연속 2건 | 두 번째 429 `/errors/queue-full` + `queuedEpisodes`/`estimatedWaitSeconds` | ✅ (주의: 잘못된 키의 401이 ~50ms로 빨라 순차 요청으로는 레이스에서 짐 — 완전 동시 발사로 재현) |
| K3 | 401로 죽은 인덱싱 잡 폴링 | `episodes[].status: ERROR` + error 메시지 | ✅ (OpenAI 401 원문이 error에 실림) |
| K4/K5 | `ADMISSION_MIN_HEADROOM_RATIO=1.1` 서버에 index/detect | 429 `/errors/model-rate-limit` + `remainingTpm` + Retry-After 10 | ✅ 2건 |

### 1-3. 실 LLM 경로 (테넌트 42×7)

| 케이스 | 기대 | 결과 |
|---|---|---|
| index ch1+ch2 묶음 제출 | 201 + jobId/requestedAt/remainingTpm | ✅ |
| index 상태 전이 | QUEUED→RUNNING→DONE (화 단위 순차) | ✅ ⚠️ **단 1차 시도는 버그①로 전원 ERROR — 수정 후 재시도 성공** |
| Neo4j 테넌트 기록 | 전 노드 `tenant_id='42:7'` | ✅ 148노드(Chunk 93/Fact 43/Character 3 등), 미태깅 0 |
| 완료 화 재제출 | (기대를 400으로 잘못 세움) | ✅ **계약은 멱등 201** — 일 없이 즉시 DONE, LLM 0회 (`job_service.py:330` 주석에 명시된 의도) |
| 다음 화 건너뛰기(ch4) | 400 "expected episodeNo 3" | ✅ |
| detect 제출·중복 제출 | 202, 중복은 재실행 없이 현재 상태 | ✅ |
| detect phase 전이 | EXTRACT→RETRIEVE(claimCount 등장)→JUDGE→DONE, 진행 중 findings null | ✅ (26 claims, 76초 소요) |
| detect findings 계약 | camelCase 8필드, `score` 미노출, `cited[].episodeNo ≤ 2` | ✅ 6건 전부 준수 |
| detect 검출 품질(참고) | 심은 모순 3개 | ✅ 3/3 검출(총 회차·유상아 소속·나이) + 타당한 파생 모순 3건, 오탐 0 |
| detect DB 기록 | jobs 행 DONE/26/6 갱신 + findings 6행 INSERT | ✅ **jsonb 캐스팅 경로(기존 미검증) 실증**, `탐지 작업 행이 없다`/`저장 실패` 경고 0건 |
| chat 턴1(첫 턴) | work_settings 도구 + suggestedTitle | ⚠️ **버그②로 500 → 채팅 모델 교체(gpt-5.4) 후 ✅** |
| chat 턴2(KG 질문) | kg 도구 + suggestedTitle null | ✅ kg_fact/hybrid/episode_manuscript DONE, 내용 정확 — 단 **kg_entity_search FAILED(버그③)**. 도구 실패 시 대화 지속 계약은 확인 |

## 2. 발견된 오류

### 버그① [치명·수정 적용됨] 인덱싱 전면 실패 — `TenantTaggingWriter.run`의 `@validate_call` 누락

- **증상**: 모든 회차 인덱싱이 추출(LLM 완료) 후 `'dict' object has no attribute 'nodes'`로 ERROR.
- **원인**: neo4j-graphrag 오케스트레이터는 컴포넌트 결과를 `model_dump()`한 dict로 다음 컴포넌트에 넘기고(orchestrator.py:127), 라이브러리의 `Neo4jWriter.run`은 `@validate_call`로 dict를 `Neo4jGraph`로 되살린다. 오버라이드(`src/repository/neo4j/kg_writer.py:39`)가 이 데코레이터를 빠뜨려 dict가 그대로 들어옴.
- **왜 지금까지 안 잡혔나**: 유닛 테스트가 파이프라인을 통째로 가짜로 대체해 이 경로가 실행된 적이 없음. **프로덕션이었다면 인덱싱이 100% 실패하는 수준.**
- **조치**: E2E 진행을 위해 `@validate_call` 추가(주석 포함). `pytest` 176개 전부 통과 확인. **커밋 여부는 리뷰 후 결정 필요.**

### 버그② [높음·**수정됨**] 채팅 모델이 gpt-5.6-luna면 /api/chat이 항상 500

- **증상**: 두 턴 모두 500. OpenAI가 `Function tools with reasoning_effort are not supported for gpt-5.6-luna in /v1/chat/completions` 400을 반환하고, 서버는 이를 잡히지 않은 예외로 500 변환.
- **원인**: 현재 `.env`가 `OPENAI_MODEL=gpt-5.6-luna`(추출 모델과 동일)로 설정됨. luna는 기본 추론이 켜진 모델이라 chat/completions에서 tools와 함께 못 씀. `.env.example` 기본값(`gpt-5.4`)이나 config 기본값(`gpt-5.6-terra`)이면 발생하지 않음.
- **검증**: `OPENAI_MODEL=gpt-5.4`로 재기동하니 정상 동작.
- **대응 선택지**(당시): (a) 채팅 모델을 luna로 두지 않는다는 제약을 `.env.example`에 명시, (b) `agent.py`에서 `reasoning_effort='none'` 명시, (c) 기동 시 모델 조합 검증.
- **최종 조치**: 위 선택지 대신 **채팅을 `/v1/responses`로 전환**했다(사용자 결정). responses는 tools + reasoning 조합을 지원하므로 모델을 제약하는 대신 추론을 켠 채(`effort=high`) 어떤 모델로도 돈다. 함께 모델 env 를 역할별로 개명(`CHAT_MODEL`/`EXTRACTION_MODEL`, 구키 fallback 유지)했다.
- **회귀 검증**: R2 — `CHAT_MODEL=gpt-5.6-luna`로 도구 사용 대화 1턴 → **200**. 4장 참고.

### 버그③ [중간·**수정됨**] `kg_entity_search` 도구가 별칭 여러 개인 엔티티에서 항상 실패

- **증상**: `kg_entity_search(entity_name='김독자')` → `Neo.ClientError.Statement.TypeError: Expected a string value for split(), but got: StringArray[독자님, 독자 씨, 독자님]`.
- **원인**: retrieval 쿼리가 별칭 property를 문자열로 전제하고 `split()`을 호출하는데, 추출기는 배열로 저장. 별칭이 배열인 엔티티(사실상 주요 인물 전부)는 조회 불능.
- **부수 확인**: 도구 실패가 `toolCalls[].status: FAILED`로 표면화되고 대화가 계속되는 계약은 정상.
- **최종 조치**: 수정 도중 별칭이 **3형식**으로 저장돼 있음을 발견했다 — 쉼표 문자열(`"작가님"`), 리스트, 그리고 **리스트 원소 안에 쉼표가 또 들어간 혼종**(`["독자님, 독자 씨", "독자님"]`). 단순 CASE 분기로는 혼종의 "독자 씨"가 안 잡혀서, `_ENTITY_PROFILE_QUERY`를 `reduce`로 평탄화한 뒤 `trim` 비교하도록 고쳤다(`src/repository/neo4j/retrieval.py`). 표시용 `_format_profile`도 리스트를 문자열로 합치도록 보완.
- **회귀 검증**: R3(도구 직접 실행 4케이스) + R2(API 경로에서 `entity_search` **DONE**). 4장 참고.
- **남은 개선(별건)**: 별칭 저장 자체의 정규화·중복 제거는 추출기 몫으로 남았다 — 조회 결과에 `[별칭: 독자님, 독자 씨, 독자님]`처럼 중복이 그대로 보인다. 조회는 정상 동작하므로 오류가 아니라 후속 과제.

### 문서 결함④ [낮음·**해소됨**] `.env.example`의 `INDEX_TPM_LIMIT`은 죽은 변수

- 코드 어디에서도 읽지 않음(접수 게이트가 토큰→대기시간 기준으로 바뀔 때 제거됨).
- **조치**: `.env.example`에서 해당 블록 삭제. 겸사겸사 파일에 등장하는 변수 18개(주석 처리분·구키 fallback 포함)를 `src/` 참조와 전수 대조했고, 죽은 변수는 이것 하나뿐이었다. 이어서 파일을 **필수 값(위) / 선택 설정(아래)** 두 구역으로 재배치했다.

## 3. 관찰 사항 (오류 아님)

- **인덱싱 중 서버 블로킹**: 회차당 ~3분 인덱싱 동안 health 포함 전 API가 간헐 무응답(polling에서 8초 타임아웃 수 회). readme에 명시된 알려진 제약과 일치.
- **suggestedTitle 20자 컷이 단어 중간을 자름**: "멸망한 세계에서 살아남는 세 가지 방" — `TITLE_MAX_CHARS` 사양대로지만 UX상 아쉬움.
- **완료 화 재제출은 멱등 201**: 스펙(`docs/indexing-api-spec.md`)의 "안전한 재제출" 계약 그대로. 테스트 설계 시 400으로 오해하기 쉬움.
- **429 queue-full 재현성**: 로컬에서는 선행 잡의 실패가 매우 빨라 순차 재현이 어려움 — 부하 테스트 설계 시 참고.
- Postgres에 Spring 실스키마가 이미 존재했고(마이그레이션 반영본), 파이썬의 쓰기가 그 CHECK 제약들(`ck_detection_jobs_counts_by_status` 등)을 전부 통과함 — 스키마 계약 양쪽이 실제로 맞물림을 확인.

## 4. 수정 회귀 검증 (2026-08-19 2차)

버그①②③ 수정 이후, 플랜 4-4의 회귀 케이스를 실행했다. **R1~R3 전부 통과.**

| # | 케이스 | 기대 | 결과 |
|---|---|---|---|
| R1 | 새 테넌트(42×8)에 ch1 인덱싱 완주 | `'dict' object has no attribute 'nodes'` 류 ERROR 없이 DONE (버그①) | ✅ QUEUED→RUNNING→DONE, 약 3분 30초. 60노드(Chunk 34/Fact 17/Organization 3/Character 2/Item 2/Chapter 1/Story 1), 미태깅 노드 0, 서버 로그 traceback 0건 |
| R2 | `CHAT_MODEL=gpt-5.6-luna` 서버에 도구 사용 대화 1턴 | 200 (버그② — 구 경로에선 항상 500) | ✅ **200 / 10.9초**, `entity_search` 단독 호출로 답변 완성. `toolCalls`/`suggestedTitle`(null) 계약 유지, 응답 전 키 camelCase, 서버 로그 오류 0건 |
| R3 | `entity_search` 도구 직접 실행 4케이스 (서버·LLM 불필요) | 별칭 3형식 전부 매칭, 없는 이름은 예외 없이 0건 (버그③) | ✅ ① 본명 `김독자` 11,626자 ② **혼종 별칭** `독자 씨` → 김독자 반환 ②-b 문자열 별칭 `작가님` → tls123 반환 ③ 없는 이름 → `검색 결과 없음.` 예외 0 |

유닛 테스트도 함께 확인: **179 passed, 4 skipped**(skip은 실호출 스모크).

### 2차 실행에서 새로 배운 것

- **구 프로세스로는 회귀 검증이 성립하지 않는다.** R2 1차 시도에서 `entity_search`가 `FAILED`로 나와 수정이 안 먹은 것처럼 보였는데, 원인은 서버가 `retrieval.py` 수정(19:21) **이전**(19:17)에 기동해 옛 코드를 들고 있었기 때문이다. 재기동 후 같은 요청이 `DONE`. 앞으로 회귀 케이스를 돌릴 때는 **대상 파일 mtime과 서버 기동 시각을 먼저 대조**한다.
- **도구가 고쳐지면 대화도 짧아진다.** 수정 전에는 `entity_search` 실패 → 모델이 `hybrid_search`·`fact_search`로 3회 우회(23.2초). 수정 후에는 `entity_search` 한 번으로 종결(10.9초). 도구 실패는 500이 아니라 **지연·토큰 비용**으로 나타난다 — 계약상 200이라 모니터링에서 놓치기 쉬운 종류의 손해다.

## 5. 재현 자산

- 자작 모순 초고: job 디렉터리 scratchpad `e2e_draft_ep3.txt` (3화, 모순 3개: 총 회차 1200/유상아 재무팀/김독자 32세)
- 시드: users(42)/works(7)/episodes(1·2 LOCKED + 3 DRAFT), detection_jobs(`e2e00000-…-03`)
- 노브: 서버 A `MAX_CONCURRENT_DETECTS=0 INDEX_EPISODE_SECONDS=1 INDEX_MAX_WAIT_SECONDS=1 OPENAI_API_KEY=sk-invalid`, 서버 B `ADMISSION_MIN_HEADROOM_RATIO=1.1`
