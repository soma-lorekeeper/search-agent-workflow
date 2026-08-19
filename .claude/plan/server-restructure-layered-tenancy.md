# 파이썬 서버 전면 재구성 — lorekeeper 병합 + 레이어드 아키텍처 + KG 멀티테넌시 + detect 엔진 교체

## Context

평가 하네스에서 확정된 설정 오류 탐지 파이프라인(line-3000 + qav2 + route qav + p4, 검출 22/25·오탐 0)을 서비스로 옮기는 작업이, 서버 전체 재구성으로 확장됐다. 현재 구조의 문제:

- **두 코드베이스로 갈라져 있다** — 서버(src/)와 별도 clone(lorekeeper-poc, gitignore됨). 게다가 lorekeeper-poc repo 안 복사본에는 **어디에도 커밋 안 된 로컬 수정**(retrieval 3종 재설계 776줄 + facts.py 109줄)이 있어 유실 위험.
- **구 탐지 아키텍처**(claim별 LLM tool-calling agent)가 하네스에서 검증한 새 파이프라인과 다르다 — 측정한 성능이 서비스에서 재현되지 않음.
- **KG가 소설을 구분하지 않는다** — Story 노드 하나(`{id:'main'}`), resolver·검색·컨텍스트 전부 전역 스캔. `kg_scope(work_id)`는 경고만 내는 스텁, `KG_INDEXED_WORK_ID`로 "작품 하나" 전제 강제.
- `src/chat/tools.py`가 **이미 삭제된 구 도구 이름 3개를 조회** → LLM이 고르면 KeyError (chat만 마이그레이션 누락).
- 데모 프론트(`static/`, `/`, `/library`, `/api/episodes*`)는 Spring이 호출하지 않는 개발용 잔재.

## 확정 결정 (사용자)

| 항목 | 결정 |
|---|---|
| lorekeeper-poc | repo 안 복사본(로컬 수정 포함본)을 src로 병합, **레이어로 완전 해체** |
| 아키텍처 | controller / dto / service / repository / config / common 레이어드 |
| contradiction/ | 전부 제거. API 의존 코드(tools.py→retrieval_tools, usage.py)만 이전 |
| 기존 API | index·chat·health 유지(리팩터링), detect POST 경로 그대로 + GET은 `/api/detect/jobs/{id}`로 재설계 |
| JSON 표기 | index·detect는 camelCase(detect는 snake_case에서 전환, Spring 조율). **chat만 snake_case 유지** |
| status 어휘 | **index·detect 모두 DB 어휘인 대문자로 통일** — `QUEUED`/`RUNNING`/`DONE`/`ERROR`. index의 `waiting`도 `QUEUED`로(테이블 주석이 명시한 Spring의 waiting→QUEUED 매핑 코드가 사라진다). Spring 조율 |
| 데모 프론트 | static/·/·/library·/api/episodes* 전부 삭제 |
| 결과 저장 | **Python이 Spring 공유 PostgreSQL(AWS RDS)에 직접 쓴다** — 접속 설정만 준비, 환경변수 값은 사용자가 입력(`DATABASE_URL` 기존 키 재사용). **테이블은 이미 존재**(Spring 소유 `detection_jobs`/`detection_findings`) — Python은 테이블을 만들지 않고, Spring이 만든 job 행을 UPDATE + findings INSERT. `detection_findings`는 **새 파이프라인 결과 모양으로 스키마 마이그레이션**(사용자 확정), **오류 판정만 저장** |
| detect 엔진 | 하네스 확정 파이프라인(extract→retrieve→judge) 이식 포함 |
| 멀티테넌시 | userId+workId 조합 tenant로 한 Neo4j DB에서 소설별 그래프 분리 |
| Neo4j 버전 | **community latest(2026.x)로 업그레이드** — 2026.02 GA 벡터 인-인덱스 필터를 테넌트 격리에 활용. community는 2026.x에도 단일 DB만 지원 → property 분리 유지 |
| 테스트 | LLM API 호출 없이 서버 end-to-end 테스트 가능하게 스텁 재설계 |
| 문서 | `docs/detect-api-spec.md` 신설 — 기존 `docs/api-spec.md`(Indexing)와 같은 포맷으로 Detecting API 스펙 작성 |

---

## 1. 새 디렉터리 구조

```
src/
├── app.py                          # FastAPI 조립 + lifespan(인덱스 워커, tenant 인덱스 보장)
├── config/__init__.py              # 구 src/config.py + EMBEDDING_MODEL/EXTRACTION_MODEL 흡수(역참조 제거)
├── common/
│   ├── tenant.py                   # Tenant 값 객체 — user_id·work_id → tenant_id("{u}:{w}") 해소 단일 지점
│   ├── usage.py                    # 구 contradiction/usage.py (하네스도 소비)
│   └── openai_client.py            # AsyncOpenAI 싱글턴 + create_completion(백오프,
│                                   #   insufficient_quota 즉시 전파) — detect·chat 공용 **LLM 유일 관문**
│                                   #   (chat_service의 자체 _create_with_retry는 삭제, 중복 해소)
├── controller/                     # health / index / detect / chat 라우터 4개
├── dto/                            # index·detect는 CamelModel(camelCase) — detect는 snake_case에서 전환.
│                                   #   **chat만 snake_case 유지**(사용자 확정). detect엔 userId, chat엔 user_id 추가
├── service/
│   ├── health_service.py           # 구 health.py (모듈 레벨 env 읽기 → 함수 내부로)
│   ├── index/                      # job_service(큐·워커·TPM·워터마크·마커) + 구 lorekeeper의
│   │                               #   indexing_service, context_service, extraction_pipeline,
│   │                               #   extractor, extraction_examples, graph_schema, resolver, splitters
│   ├── detect/                     # job_service(오케스트레이션+DB저장), extract_service, retrieve_service,
│   │                               #   docstore, judge_service, prompts(qav2/p4 동결본), entity_nodes
│   ├── chat/                       # chat_service(구 agent.py), tools(신 3종 래핑으로 버그 해소), prompts, indexed
│   └── retrieval_tools.py          # 구 contradiction/tools.py — build_openai_tools/format_tool_result
└── repository/                     # DB별 하위 디렉터리로 분류(사용자 확정), _repository 접미사 없음
    ├── neo4j/
    │   ├── client.py               # 구 lorekeeper/client.py — get_driver + DATABASE 단일 출처
    │   ├── tenant_bootstrap.py     # 라벨×tenant_id 조합 인덱스 보장(idempotent)
    │   ├── kg_writer.py            # TenantTaggingWriter(Neo4jWriter 서브클래스)
    │   ├── chunk.py                # 구 chunks.py — uid·MERGE 키·인덱스에 tenant 반영
    │   ├── evidence.py             # 구 evidence.py — tenant 스코프
    │   ├── fact.py                 # 구 facts.py — tenant 스코프
    │   └── retrieval.py            # 구 retrieval.py 776줄 통째(쿼리↔포매터 결합) + tenant/회차 필터
    └── postgres/
        ├── client.py               # DATABASE_URL 커넥션 팩토리 (chat/health/detection 공용)
        ├── manuscript.py           # episodes/works SELECT (구 chat/tools.py의 SQL)
        └── detection.py            # Spring 소유 detection_jobs UPDATE + detection_findings INSERT/조회
```

의존 방향: `controller → dto/service → repository → config/common`.

**lorekeeper 해체 기준**: 쿼리만 있는 모듈(chunks/evidence/facts/client)은 repository로, LLM·정책과 쿼리가 얽힌 모듈(context/resolver)은 service에 남기고 쿼리 인라인 유지. retrieval.py는 retrieval_query Cypher와 result_formatter가 한 몸이라 776줄 통째로 repository에. `src/chat/kg_scope.py`·`KG_INDEXED_WORK_ID`·`require_indexed_work`는 **소멸**(Tenant로 승격).

삭제: `src/contradiction/{agent,prompts,pipeline}.py`, `static/`, `/`·`/library`·`/api/episodes*` 라우트, `_write_episode_files`(뷰어 전용), `test_detect_pipeline.py`(구 파이프라인 테스트).

---

## 2. KG 멀티테넌시

- **키**: `tenant_id = f"{user_id}:{work_id}"` 단일 property. int 검증 후 조합이라 resolver의 raw WHERE 절(filter_query)에도 안전. `common/tenant.py`의 `Tenant.of(user_id, work_id)` → `.id` / `.params()` / `.filter_literal()`이 유일한 해소 지점.
- **Neo4j를 `neo4j:2026.07-community`로 업그레이드**(조사·실측 완료). community는 2026.x에서도 표준 DB 1개만 지원 → 멀티 DB 분리 불가, property 분리 확정.
- **2026.02부터 벡터 인-인덱스 필터(SEARCH 절)가 GA이고 Community도 지원**. 게다가 **neo4j-graphrag 1.18.0이 서버가 2026.01+면 자동으로 SEARCH 절 쿼리로 전환**한다 — 우리가 라이브러리를 쓰는 한 벡터 축은 알아서 인-인덱스 필터를 탄다.
- **★ 함정 두 개(둘 다 조용히 깨진다)**:
  1. **Cypher 언어 버전.** SEARCH 절은 Cypher 25 전용이다. 2026.x 이미지는 *빈 볼륨*에 새로 뜨면 25가 기본이지만 *기존 볼륨을 살려 올리면* 그 DB가 Cypher 5를 유지한다 → `42I67: parsable in CYPHER 25, but run in CYPHER 5`. 게다가 라이브러리는 `CYPHER 25` 프리픽스를 붙이지 않아 **기존에 잘 돌던 검색까지 문법 에러로 죽는다.** compose에 `NEO4J_db_query_default__language: CYPHER_25`로 못박는다(반영 완료).
  2. `db.index.vector.queryNodes`는 2026.04 deprecated(제거는 아님). 풀텍스트 프로시저는 deprecated 아님.
- Cypher 25에서 깨지는 구문 중 우리 코드에 해당하는 것은 **없다**(실측: `SET n = r` 하나만 깨지고 우리는 안 씀. 구형 `CALL { WITH n ... }`도 그대로 동작).

### 쓰기 지점 (전수)

| 지점 | 변경 |
|---|---|
| extractor→writer 도메인 노드 | **TenantTaggingWriter** — `run(graph)`에서 각 노드 properties에 tenant_id 주입 후 super().run(). 노드 생성과 tenant 기록이 같은 upsert에 원자적 — 고아 노드 창 자체가 없음 (~15줄, DAG 무변경). 후처리 일괄 SET안은 크래시 시 고아를 다른 테넌트가 흡수하는 교차 오염 위험으로 기각 |
| Chunk (chunks.py) | **uid `chunk-{tenant}-{chapter}-{index}`로 변경**(현재 `chunk-{c}-{i}` — 테넌트 간 upsert 덮어쓰기 치명 지점), metadata에 tenant_id |
| Chapter/IN_CHAPTER | MERGE 키 `{number, tenant_id}` |
| evidence.link_evidence | 전역 MATCH → `fact.tenant_id=$t` (두 테넌트 동시 인덱싱 시 깨지는 전역 전제 제거) |
| facts.ensure_fact_layer | tenant 스코프 |
| Story/IN_STORY·요약 (context) | `MERGE (s:Story {id:'main', tenant_id:$t})`, 모든 쿼리 tenant 필터 |
| **resolver 4종 병합 스캔** | `filter_query=tenant.filter_literal()` 주입 — **라이브러리 훅 기존재 확인**(resolver.py:66-68·202-204). 누락 시 소설 간 인물 병합이라는 최악 버그 |

관계(edge)에는 tenant 불필요 — 양끝 노드 필터로 충분하고, resolver가 스코프 안에서만 병합하면 테넌트 횡단 관계가 생기지 않는다.

### 읽기 지점 (전수)

- retrieval.py hybrid/fact — **두 축을 다르게 필터**한다:
  - **벡터 축**: retrieval_query의 `WHERE node.tenant_id = $tenant_id` 후필터 + `effective_search_ratio=4` 오버샘플. 라이브러리가 2026.x 서버에서 SEARCH 절로 자동 전환하므로 앵커 단계의 이득은 얹혀 받되, 우리 코드는 서버 버전에 의존하지 않는 형태로 둔다(5.26에서도 그대로 동작).
  - **풀텍스트 축**: Lucene **필드 한정 쿼리로 인-인덱스 필터**. 근거 — 풀텍스트 인덱스는 property마다 **별개 Lucene 필드**를 만들고 `<property>:<term>` 한정 검색이 공식 지원된다(Neo4j 문서 예: `queryNodes('MovieIndex', 'title:dream')`). 즉 tenant 토큰이 본문 토큰과 한 자루에 섞이지 않는다. 구현 3점 세트:
    1. 인덱스에 tenant 필드 포함: `CREATE FULLTEXT INDEX ... FOR (n:Chunk) ON EACH [n.text, n.tenant_ft]`
    2. **tenant 토큰은 analyzer-safe 형태 `u{user}w{work}`**(예: `u1w1`). cjk analyzer의 StandardTokenizer가 `"1:1"`은 `:`에서 쪼개 `["1","1"]`로 만들어 `12:34`와 `34:12`가 구분 불가가 된다. 영숫자 연속은 한 토큰으로 남고 CJKBigramFilter도 라틴을 건드리지 않는다. 인덱싱·쿼리 양쪽에 같은 analyzer가 걸리므로 형태만 맞으면 정확 매칭. Cypher/벡터용 `tenant_id`(`"{u}:{w}"`)와 별개 property `tenant_ft`로 저장(같은 값을 두 형태로 — 하나로 합치면 어느 한쪽이 깨진다).
    3. **이스케이프 순서가 함정** — `_LUCENE_SPECIAL`(retrieval.py:93)이 `+ ( ) :`를 전부 이스케이프하므로, 필터를 붙인 뒤 이스케이프하면 `\+tenant_id\:u1w1 \+\(...\)`가 되어 **필터가 리터럴 검색어로 죽는다**(에러 없이 조용히). 반드시 `f"+tenant_ft:{token} +({_escape_lucene(query_text)})"` — 사용자 검색어만 이스케이프하고 필터는 그 바깥에서 조합. 자리는 `_EscapedHybridCypherRetriever.get_search_results`(retrieval.py:112-122)의 그 한 줄.
    - 부수효과: 필드 미지정 검색어는 인덱스의 모든 필드를 훑으므로 본문 검색어가 tenant 필드에도 매칭을 시도한다(한국어 검색어가 `u1w1`에 걸릴 일은 없어 실질 무해). 회차 상한은 Lucene 범위 쿼리가 불편하므로 후필터에 남긴다.
  - retrieval_query의 tenant 후필터는 **두 축 모두 방어선으로 유지**(인-인덱스 필터 회귀나 인덱스 재생성 시 tenant 필드 누락 같은 조용한 무력화 방지). entity_search 두 쿼리도 WHERE 추가. 오버샘플(`effective_search_ratio`)은 주 방어가 아니라 보조로 격하.
  - 구현 착수 시 스모크 확인 1회: 2026.x community 이미지에서 SEARCH 절·기존 `db.index.vector.queryNodes` 프로시저·neo4j-graphrag 호출 경로가 모두 동작하는지(서버 메이저 업그레이드에 따른 프로시저 deprecation 여부 포함).
- context.dump_graph_text/load_summaries, `_already_indexed` 마커, chat indexed.py, entity_nodes: 전부 tenant 필터. 인덱스 워터마크 dict도 tenant 키로(`dict[str, int]`).
- detect 회차 격리: 구 `_ChapterBoundedDriver`는 이식하지 않고 `$max_chapter` 파라미터가 대체 (dump_graph_text 결과 모양에 결합된 필터라 retriever에 애초에 동작 안 함).

### API 계약 변경 + 마이그레이션

- **detect 요청에 `userId`, chat 요청에 `user_id` 추가** (인덱싱은 이미 userId 받음) + **detect의 기존 snake_case 필드를 camelCase로 전환, chat은 snake_case 유지**(사용자 확정) — Spring 조율 필요. workId 단독 유니크 가정은 채택하지 않음: api-spec.md:33이 "userId×workId 조합이 유니크"를 공통 규약으로 명문화했고, 세 API의 테넌트 키가 같아야 해소 지점이 하나로 성립.
- 기존 데이터는 **재인덱싱으로 갈음** — uid 체계·Story 병합 키가 바뀌어 일괄 SET으로 못 따라감. 전환 배포 시 Neo4j 비우고 시작(현재 작품 1개·수 화라 비용 작음). 마커 없는 그래프는 Spring이 재-POST하는 기존 계약이 자연 복구 경로.

---

## 3. detect 서비스 (엔진 교체)

```
POST /api/detect {jobId, userId, workId, episodeNumber, text} → 202, create_task  (camelCase 전환)
1. extract:  split_lines→number_lines 전역 1회 → 라인 경계 3000자 청킹(eval_claims.py:1663-1675)
             system = qav2 criteria+few-shot+SCOPE(cap_for(3000) 고정)+entity_nodes(tenant, <episode)
             asyncio.gather 병렬, 캐시 없음 → 병합 → assign_claim_ids(P1~PN)
2. retrieve: claim별 route_qav(코드 라우팅) → ThreadPoolExecutor.map(순서 보존)
             query_params={tenant_id, max_chapter, final_k}, items 전량 유지·dedupe는 content만
3. judge:    build_docstore → p4(치환 체인 결과 문자열로 동결) + render_docstore/render_claim_refs
             배치 1회 → _parse_verdicts → claimId·lineIds는 코드가 채움(P순번, claim.lines)
             score>=7 → isError. cited: C###→(episodeNo, chunkIndex), F###→근거 청크로 펼침
             폴백 3종(claim_id_unknown/line_fallback/cited_unknown)은 로그만
4. 저장:     repository/postgres/detection — findings INSERT + detection_jobs 행 완료 UPDATE
             → 메모리 done 원자 갱신(한 번의 update)
```

숨은 계약 4종(주석+테스트로 고정): (a) claim 순서==문서고 refs 순서(map 순서 보존), (b) cap_for는 설정값 3000 고정(프롬프트 캐시), (c) 라인 번호 전역 1회, (d) items 무-dedupe.

`openai_client.create_completion`: 429 백오프 + **`insufficient_quota`는 즉시 전파** 분기. chat_service도 이 관문을 쓴다(자체 `_create_with_retry` 삭제 — 하네스 주석이 지적한 중복의 해소).

### 결과 저장 — Spring 소유 기존 테이블에 직접 쓰기

**테이블은 이미 존재한다**(Spring schema): `episodes 1─0..1 detection_jobs 1─N detection_findings`. Spring이 job 행을 만들고 job_id(UUID 36자)를 발급해 POST로 넘긴다 — Python은 테이블·행을 만들지 않고 **자기 job_id의 행을 UPDATE + findings INSERT**만 한다.

**Python의 쓰기 프로토콜** — **재탐지는 Spring이 새 jobId로 새 행을 만든다**(episode_id UNIQUE라 이전 행은 대체됨). 그래서 Python이 보는 행은 항상 `QUEUED`에서 시작하고, DONE→RUNNING 되돌림이 없어 상태 리셋이 필요 없다.

```sql
-- 1) 시작
UPDATE detection_jobs SET status='RUNNING', updated_at=now() WHERE job_id=$uuid;

-- 2) 완료(한 트랜잭션)
DELETE FROM detection_findings WHERE job_id=$uuid;      -- 같은 job 재실행 시 seq 충돌 방지(아래 ★)
INSERT INTO detection_findings (job_id, seq, quote, axis, value, line_ids, cited, reason, score)
     VALUES ($uuid, ...);
UPDATE detection_jobs
   SET status='DONE', claim_count=$n, contradiction_count=$m, completed_at=now(), updated_at=now()
 WHERE job_id=$uuid;

-- 3) 실패: 카운트·claim_count는 0/NULL이어야 한다(ck_detection_jobs_counts_by_status)
UPDATE detection_jobs
   SET status='ERROR', detail=$msg, claim_count=NULL, contradiction_count=0,
       completed_at=now(), updated_at=now()
 WHERE job_id=$uuid;
```

- **★ 같은 jobId 재실행은 여전히 가능하다** — 우리가 재시작해 메모리를 잃으면 Spring이 404를 보고 같은 jobId로 재POST한다. 그때 이전 실행의 findings가 남아 있으면 `ux_detection_findings_job_id_seq`(job_id, seq UNIQUE)가 INSERT를 거부한다. 그래서 INSERT 전 DELETE가 방어가 아니라 **필수**다.
- **`job_id`를 두 테이블 모두 varchar(36) UUID로 통일한다**(아래 마이그레이션) — 지금은 `detection_findings.job_id`가 bigint FK(→`detection_jobs.id`)라 이름은 같은데 가리키는 게 달라 혼동을 부르고, INSERT마다 UUID→id 조회가 한 번 더 든다.
- **`updated_at`은 Python이 명시적으로 갱신한다** — 트리거가 없고 default now()는 INSERT에만 걸린다.
- **`QUEUED`는 Spring이 행을 만든 직후**의 상태다. Python은 큐가 없어(POST 즉시 create_task) `RUNNING`부터 쓴다.
- **ERROR에는 claim_count를 남길 수 없다**(제약). 추출까지 성공하고 판정에서 실패해도 DB엔 NULL — 그 정보는 메모리 응답과 로그에만 산다.
- 행이 없으면(UPDATE 0 rows) **경고 로그만 남기고 검사는 계속한다** — DB 쓰기 실패가 검사를 죽이지 않는다는 원칙과 일관. 결과는 메모리로 응답된다.
- `job_id`는 varchar(36)이므로 DTO에서 길이 검증 → DB 거부 전에 400으로 거른다.

**스키마 마이그레이션** (새 파이프라인 결과 모양으로 — 사용자 확정, Spring schema 변경 조율):

```sql
-- (1) detection_jobs: finding_count 삭제. 오류만 저장하므로 contradiction_count와 항상 같아
--     한쪽은 중복이다. "검사한 claim 수"의 자리는 claim_count가 이미 맡고 있다.
ALTER TABLE detection_jobs
    DROP CONSTRAINT ck_detection_jobs_contradiction_within_findings,  -- finding_count 참조
    DROP CONSTRAINT ck_detection_jobs_counts_not_negative,
    DROP CONSTRAINT ck_detection_jobs_counts_by_status,
    DROP COLUMN finding_count;
ALTER TABLE detection_jobs
    ADD CONSTRAINT ck_detection_jobs_contradiction_count_not_negative
        CHECK (contradiction_count >= 0),
    ADD CONSTRAINT ck_detection_jobs_counts_by_status
        CHECK (status = 'DONE' OR (contradiction_count = 0 AND claim_count IS NULL));

-- (2) detection_findings: FK를 UUID로 통일 + 결과 컬럼 교체
ALTER TABLE detection_findings DROP COLUMN job_id;                    -- bigint FK 폐기
ALTER TABLE detection_findings
    ADD COLUMN job_id varchar(36) NOT NULL
        REFERENCES detection_jobs (job_id) ON DELETE CASCADE;         -- job_id에 UNIQUE가 있어 FK 가능
DROP INDEX ux_detection_findings_job_id_seq;
CREATE UNIQUE INDEX ux_detection_findings_job_id_seq ON detection_findings (job_id, seq);

ALTER TABLE detection_findings
    DROP CONSTRAINT ck_detection_findings_label,                      -- 구 label 3종 전제
    DROP COLUMN category, DROP COLUMN label,
    DROP COLUMN established_fact, DROP COLUMN source_episode;
ALTER TABLE detection_findings RENAME COLUMN explanation TO reason;
ALTER TABLE detection_findings
    ADD COLUMN axis     varchar(128) NOT NULL,          -- claim의 설정 축
    ADD COLUMN value    text         NOT NULL,          -- 초고가 주장하는 값
    ADD COLUMN line_ids jsonb        NOT NULL,          -- 원고 라인 번호 [34, 35] — 하이라이트용
    ADD COLUMN cited    jsonb        NOT NULL DEFAULT '[]'::jsonb,  -- [{"episodeNo":3,"chunkIndex":14}]
    ADD COLUMN score    integer;                        -- 내부 진단용(화면 비노출)
```

- **오류 판정만 저장**(사용자 확정)이므로 label 컬럼은 전 행이 CONTRADICTION이 되어 무의미 — 제거(화면이 등급을 다시 나누게 되면 그때 복원).
- 아직 운영 데이터가 없다면 위 ALTER 대신 두 테이블을 **DROP+CREATE로 재작성**하는 편이 읽기 쉽다 — 실행 시점에 Spring 팀과 결정.
- quote NOT NULL·seq 양수 등 나머지 제약은 새 계약과 그대로 호환(seq = claimId의 P순번).
- indexing_jobs/indexing_job_episodes는 Spring 전유 — Python은 건드리지 않는다(status 어휘 대문자 통일만 API 계약으로 조율).

### GET /api/detect/jobs/{jobId} (camelCase)

**status는 DB 어휘(대문자 4종)로 통일**하고, **`requestedAt`·`completedAt`을 응답에 추가**한다(테이블이 이미 갖고 있고 인덱싱 API의 `requestedAt`과도 일관 — 화면이 "N분 전 완료"를 그릴 수 있다).

- 진행: `{jobId, status: "QUEUED"|"RUNNING", phase: "EXTRACT"|"RETRIEVE"|"JUDGE", claimCount, contradictionCount: 0, findings: null, detail: null, requestedAt, completedAt: null}`
- 완료: `{jobId, status: "DONE", phase: null, claimCount, contradictionCount, findings: [...], detail: null, requestedAt, completedAt}`
- 에러: `{jobId, status: "ERROR", phase: null, claimCount, contradictionCount: 0, findings: null, detail, requestedAt, completedAt}`
- 404: 메모리에 없음(재시작 등) — Spring은 자기 detection_jobs를 보거나 재POST
- `contradictionCount`는 **DB 컬럼과 같은 의미로 항상 실린다**(NOT NULL default 0 → DONE 전에는 0, DONE에서 오류 수). `findings` 배열 길이와 같은 값이지만, 호출자가 배열을 세지 않고 바로 쓸 수 있고 응답이 테이블과 1:1로 대응해 대조하기 쉽다.
- `phase`는 DB에 없다(메모리 전용) — 배치 판정이라 중간 단위가 없는 대신 어느 단계인지만 알린다. status와 같은 열거형이라 표기도 대문자로 맞춘다. `claimCount`는 ERROR 응답에도 실을 수 있다(메모리 기준) — DB엔 제약상 못 남긴다.
- 구 claim별 실시간 진행 목록 폐기(배치 1회 판정이라 중간 단위 없음) — phase+claimCount로 대체.
- 역할 분담: **메모리 = 진행 상태와 GET 응답의 출처, DB(Spring 테이블) = 영속 진실**(Python이 직접 기록하므로 별도 폴백 저장 불필요). 재시작으로 메모리를 잃으면 GET은 404 — Spring은 자기 detection_jobs를 보거나 재POST(기존 계약). POST 중복: 메모리에 있으면 재실행 없이 202 기존 status, 없으면 실행(detection_jobs 행 대체는 Spring 소관). DB 쓰기 실패는 검사를 죽이지 않음(메모리 결과 유지 + 에러 로그 — DB 상태는 /api/health가 노출).

---

## 4. 테스트 재설계 — LLM 호출 없는 end-to-end

**스텁 경계는 4개의 관문 함수** — 외부 라이브러리 내부는 스텁하지 않고, 이 네 모듈 공개 이름이 테스트 계약이다(리네임하면 테스트가 즉시 깨져 알려준다):

1. **`openai_client.create_completion`** — extract·judge·chat의 LLM 유일 관문. 여기 한 곳만 fake하면 셋 다 LLM 없이 돈다. fake는 **system 프롬프트 내용으로 분기**(추출기 문구 → qav2 claims JSON, 판정기 문구 → verdicts JSON, 그 외 → chat 응답) — 호출 순서 의존보다 병렬 호출에 안전. 고정 JSON은 하네스 아티팩트(data/eval_claims의 실제 응답)에서 축약해 만들고 출처를 주석으로.
2. **`retrieval.build_retrievers(tenant_id)`** — Neo4j 검색 관문. 인메모리 테넌트별 스토어(dict[tenant_id, items], 하네스 evidence 스키마와 같은 metadata 모양)로 fake — docstore·cited 해소 계약이 실 DB 없이 검증됨. repository 자체 쿼리(마커·indexed·entity_nodes)는 기존 FakeDriver 패턴 계승(쿼리 조각 판별 + **파라미터 기록으로 tenant_id 도달 단언**).
3. **`postgres.detection`의 update/insert**(+postgres.manuscript) — 인메모리 dict fake가 **실제 제약을 흉내내서**(completed_at 동치, 카운트는 DONE에서만, (job_id, seq) UNIQUE) 쓰기 프로토콜을 검증한다: QUEUED→RUNNING→DONE 전이, **같은 jobId 재실행 시 DELETE 없이는 seq 충돌로 거부되는지**, findings DELETE→INSERT→완료 UPDATE가 한 트랜잭션, ERROR가 claim_count NULL·contradiction_count 0을 지키는지, 행이 없을 때 검사가 죽지 않는지.
4. **`indexing_service.indexing`** — 인덱싱만 통짜 fake(neo4j-graphrag가 자체 LLM/임베더를 내부에서 불러 관문 통합 불가, 현행 `run_indexing` monkeypatch 패턴 계승). 인덱싱 내부(TenantTaggingWriter의 tenant 주입, resolver filter_query 조립)는 fake graph를 넣는 단위 테스트로.

**주입 방식**: **monkeypatch 표준 유지** — 새 구조도 모듈 함수 기반이고 controller가 Depends 없이 서비스를 직접 import하는 현행 스타일의 연장이라, dependency_overrides는 쓸 자리가 없고 생성자 주입은 소비자가 하나뿐인 클래스를 테스트 때문에 만드는 과잉. 기존 conftest의 `OPENAI_API_KEY` placeholder·RecordingDict(원자 갱신 검증)·TestClient 이벤트 루프 패턴(루프 귀속 전역은 fixture에서 재생성)은 유지.

**e2e 시나리오 최소 세트** (tests/ 재구성):
- `test_index_api.py`: 201 계약·400·429·상태 전이·연쇄 스킵·404 (기존 이관) + **테넌트별 워터마크**(A의 7화 뒤 B의 3화가 400이 아님)
- `test_detect_api.py`: 202 → 폴링 → done — findings 계약 전수(isError=score≥7만, claimId=P순번, lineIds=claim.lines, cited의 (episodeNo, chunkIndex) 해소, F###의 근거 청크 펼침, score 비노출, **`contradictionCount == len(findings)`**) + **폴백 3종이 각각 발동하는 fake 응답 케이스**(①없는 claimId ②후보 밖 lineIds ③미지 별칭 — 500이 아니라 정상 응답+로그) + RecordingDict로 "done인데 findings null" 순간 부재 + 메모리 소실 후 GET 404(Spring은 자기 DB 조회 또는 재POST — 기존 계약) + fake detection 저장소에 쓰기 프로토콜 순서 검증
- `test_detect_pipeline.py`(재작성): extract 청킹·전역 라인·cap_for 고정, route_qav 라우팅표, docstore 별칭·자연키, _parse_verdicts 단위, 회차 격리 필터
- `test_chat_api.py`: 1턴 — fake LLM이 신 3종 도구를 고르는 응답 → KeyError 없이 실행(버그 회귀). require_indexed_work의 400 거부 테스트는 삭제(관문 소멸)
- `test_health_api.py`: fake DB 체크로 ok/degraded, 항상 200
- `test_tenancy.py`: fake 그래프에 테넌트 A만 데이터 → B의 detect는 근거 0건, B의 chat 도구는 빈 결과 + FakeDriver 파라미터 기록으로 마커·indexed·entity 쿼리에 tenant_id 도달 단언. 실제 Neo4j 교차 병합 0건 검증은 `@pytest.mark.integration`으로 분리(로컬 docker에서만)

**인프라**: `requirements-dev.txt`(pytest, httpx — TestClient가 fastapi 전이 의존에 기대는 현황 명시화) 신설, ci.yml에 pytest 단계 추가(현재 CI는 import check만 하고 테스트를 안 돌림).

---

## 5. 작업 순서 (커밋마다 서버 기동 유지)

lorekeeper-poc는 gitignore된 별도 clone이라 반입은 신규 add(히스토리 없음, 불가피). **반입을 1커밋으로 먼저** 끝내고 이후 재배치는 전부 git mv로 히스토리를 잇는다.

1. **lorekeeper 반입(로직 무변경)**: `poc/src/*` 14개 → `src/lorekeeper/` 복사 — 미커밋 retrieval.py·facts.py가 처음 버전관리에 들어감. 소비자 import 갱신(webapp.py:18-24, chat/tools.py:22, chat/indexed.py:22-23, contradiction/pipeline.py:30-33, eval_claims.py:33·913). requirements: `-e` 제거 + 의존성 8종 직접 선언(neo4j>=6.2.0, neo4j-graphrag[openai]>=1.18.0, langchain-text-splitters>=0.3.0, kiwipiepy>=0.15.0, kss>=4.0.0, python-mecab-ko>=1.3.7, rapidfuzz>=3.14.5). ci.yml clone 단계 제거, deploy.yml tar 제외 항목 정리.
2. **데모 프론트 삭제**: static/, `/`·`/library`·`/api/episodes*`, EpisodeSummary/Detail, `_write_episode_files`.
3. **webapp 해체**: app.py+controller/+dto/+service(index job·health·chat) git mv. detect는 구 엔진인 채 controller만 분리. 테스트 monkeypatch 경로 갱신.
4. **lorekeeper 레이어 분배**: git mv로 repository/·service/index/ 최종 위치, 상대→절대 import, DATABASE·모델 상수 일원화(현재 두 곳 중복). eval_claims 경로 갱신.
5. **KG 멀티테넌시 — 두 커밋으로** (각 커밋에서 서버 기동·동작 보존):
   - **5a 쓰기 경로**: **Neo4j 2026.x 업그레이드 먼저**(docker-compose 이미지 교체 + 프로덕션 EC2 — 인-인덱스 필터가 2026.02+ 전제. neo4j-graphrag 호출 경로 스모크 확인 포함), common/tenant.py, TenantTaggingWriter, Chunk uid·Chapter/Story 병합 키, resolver filter_query(생성자에 tenant 필수 인자 — 빠뜨리면 TypeError로 즉사), tenant_bootstrap 인덱스 DDL. 읽기는 아직 전역(단일 테넌트라 무해). **이 시점에 Neo4j 초기화 + 재인덱싱.**
   - **5b 읽기 경로 + 계약**: retrieval — 벡터 축 인-인덱스 필터(SEARCH 절)·풀텍스트 축 후필터+오버샘플·`$max_chapter`, context·마커·indexed·entity_nodes 필터, 워터마크·활성 화 집합 테넌트별 dict, kg_scope 승격·KG_INDEXED_WORK_ID·require_indexed_work 제거, detect DTO camelCase 전환+userId 추가, chat DTO에 user_id 추가(snake_case 유지), **index·detect status 어휘 대문자 통일**(index의 `waiting`→`QUEUED` 포함, test_index_api 기대값도 함께), test_tenancy.py 신설. **Spring 배포(필드 추가·detect camelCase·status 어휘) 동기화 지점.**
6. **detect 엔진 교체 + contradiction 제거**: service/detect/* 신설(하네스에서 함수·프롬프트 복사, tag/VARIANTS/디스크캐시/n>1/채점기 제거 — 처음부터 tenant 전제로 작성), common/openai_client(chat도 이 관문으로 통합), repository/postgres/{client,manuscript,detection} + **detection_findings 마이그레이션 SQL 산출·적용(Spring schema 변경 조율)**, detect_controller 재작성(GET /api/detect/jobs/{jobId} — Spring 동기화), contradiction 삭제, tools→retrieval_tools·usage→common git mv, chat/tools.py KeyError 버그 해소(신 3종 래핑+TOOL_GUIDE 갱신), test_detect_pipeline.py 재작성·폴백 3종 케이스.
7. **마무리**: readme 구조도, **`docs/detect-api-spec.md` 작성** — 기존 `docs/api-spec.md`(Indexing API Spec)와 같은 포맷(버전·범위·시퀀스 다이어그램·엔드포인트별 요청/응답 표·오류 코드·폴링 규약·TBD 절)으로 Detecting API 문서화: POST /api/detect(202, camelCase, jobId는 Spring 발급)·GET /api/detect/jobs/{jobId}(진행 phase/claimCount·완료 findings 계약·404=재POST 규약)·detection_jobs/findings 쓰기 프로토콜과 마이그레이션된 findings 스키마·userId/workId 테넌트 규약. requirements-dev.txt + ci에 pytest 단계·import check `import src.app`, .gitignore lorekeeper-poc/ 정리(로컬 clone 제거 후), 하네스 1회 실행 회귀 확인.

---

## 6. 위험 (요지)

1. **resolver tenant 스코프 누락 = 소설 간 인물 병합** — filter_query 주입을 생성자 한 곳에서 강제 + 교차 병합 0건 통합 테스트.
2. **Chunk uid 충돌** — uid에 tenant 미포함 시 다른 소설 같은 (회차,조각)이 덮어씀.
3. **풀텍스트 필터의 조용한 무력화 — 3중 실패 경로** (전부 에러 없이 결과만 이상해지는 유형):
   (a) 이스케이프 순서를 틀리면 필터가 리터럴 검색어가 됨, (b) tenant 토큰에 `:` 등 구분자가 들어가면 analyzer가 쪼개 필터가 헐거워짐, (c) 인덱스 재생성 시 tenant 필드를 빠뜨리면 필터 자체가 무효.
   방어: retrieval_query 후필터를 **두 축 모두에 유지**(리콜은 잃어도 격리는 지킨다) + 필터 탈락률 로그 + `test_tenancy.py`에 "B의 검색에 A 노드 0건"을 풀텍스트 축 단독으로도 검증하는 케이스.
4. **Chunk metadata→property 전파는 라이브러리 동작 전제** — 커밋 5 착수 시 실검증 1회, 실패 시 write 직후 SET 폴백.
4b. **Neo4j 2026.x 업그레이드 호환성** — SEARCH 절의 community 지원, 기존 `db.index.vector.queryNodes`·APOC·neo4j-graphrag(>=1.18)·python driver(>=6.2) 경로가 2026.x 서버에서 그대로 도는지 5a 착수 시 스모크 확인. 프로덕션 EC2 Neo4j 업그레이드는 배포 조율 필요.
5. **detect/chat user_id 추가·GET 경로 변경** — 커밋 5·6 각각 Spring 배포와 동기화 창 필요.
6. **숨은 계약 4종**(map 순서/cap_for 고정/라인 전역 1회/items 무-dedupe) — 소리 없이 틀리는 유형, 주석+단위 테스트.
7. **완료 마커 계약** — "인덱싱의 마지막 쓰기 = IN_STORY MERGE" 순서를 이동·테넌시 수정 중 유지.
8. **미커밋 코드가 유일본** — 커밋 1을 다른 어떤 작업보다 먼저.
9. **kss/mecab** — `backend="mecab"` 명시 유지, arm64/cp312 휠 확인이 커밋 1 배포 전 필수.
10. **Spring 소유 테이블에 쓰기** — 스키마 마이그레이션(finding_count 삭제, findings FK를 UUID로, 결과 컬럼 교체)과 배포 타이밍을 Spring 팀과 조율(마이그레이션 전에 새 엔진이 쓰면 컬럼 불일치로 실패). **같은 jobId 재실행 시 findings의 (job_id, seq) UNIQUE가 INSERT를 거부**하므로 INSERT 전 DELETE가 필수 — 재시작 후 Spring이 재POST하는 경로에서만 터진다. status 어휘 대문자 통일(index 포함)과 requestedAt/completedAt 응답 추가도 Spring 조율 항목. DATABASE_URL 부재 환경에서도 기동 유지.
11. **p4 프롬프트 동결** — 하네스 치환 체인의 결과 문자열로 넣고 자구 대조로 검증.
12. **fake LLM의 거짓 안심** — fake 응답이 실제 모델 응답 형태와 어긋나면 테스트가 통과해도 의미 없음. 고정 JSON은 하네스 아티팩트(data/eval_claims 실제 응답)에서 축약하고 출처 주석.
13. **구 uid 잔존** — Chunk uid 체계가 바뀌므로 구 노드가 남은 채 재인덱싱하면 upsert가 수렴하지 않음. 5a에서 반드시 초기화 후 재인덱싱.
14. **회차 상한 후필터의 수축** — 테넌트는 인-인덱스로 걸러지지만 회차 상한(`chapter < N`)은 여전히 후필터라, 검사 회차가 앞쪽일수록(뒤 회차 데이터가 많을수록) top_k가 줄 수 있음. 오버샘플 보조 + 탈락률 로그로 감시.

## 검증

1. 커밋마다: `python -c "import src.app"` + pytest green (커밋 3부터 e2e 스텁 세트 가동).
2. 커밋 5a 착수 시: 2026.x community 이미지 스모크 — (a) 벡터 SEARCH 절 인-인덱스 필터 실행, (b) **풀텍스트 필드 한정 쿼리 실증**: tenant 필드를 포함해 인덱스를 만들고 `+tenant_ft:u1w1 +(한국어검색어)`가 다른 테넌트 노드를 0건으로 거르는지, cjk analyzer가 `u1w1`을 한 토큰으로 유지하는지 직접 확인, (c) 기존 벡터/풀텍스트 프로시저·neo4j-graphrag 검색 경로 동작 확인.
3. 커밋 5 후: 로컬 docker Neo4j(2026.x)에 테넌트 2개 인덱싱 → 교차 병합 0건, B의 검색에 A 노드 0건(integration 마크).
4. 커밋 6 후: 하네스 초고 5편+클린 6화를 새 API로 검사 → 검출 GT 집합·클린 오탐 0 재현(확률적이라 완전 일치는 기대하지 않음), 폴백 3종 인위 발동 시 500이 아니라 정상 응답, `(episodeNo, chunkIndex)`로 Neo4j에서 청크 실조회.
5. `PYTHONPATH=. .venv/bin/python scripts/eval_claims.py --stage score ...`로 하네스가 새 import 경로에서 여전히 도는지.

---

# 실행 기록 (2026-08-15 완료)

계획과 달라진 점과 실행 중 확인한 사실을 남긴다.

## 계획과 달라진 것

| 계획 | 실제 | 이유 |
|---|---|---|
| 커밋 5a/5b 분리 | **한 커밋으로 합침** | `kg_scope` 시그니처가 쓰기·읽기의 경첩이라, 반쪽만 바꾸면 서버가 아예 안 뜬다 |
| 벡터 축 SEARCH 절 직접 조립 | **후필터 + 오버샘플 유지** | neo4j-graphrag 1.18이 서버가 2026.01+면 **자동으로** SEARCH 절로 전환한다. 우리가 직접 조립하면 서버 버전에 코드가 묶인다 |
| 커밋 7 문서·CI 별도 | 커밋 6에 포함 | 분량이 작고 같은 계약을 설명하는 것이라 나누는 이득이 없었다 |

## 실측으로 확인한 것 (neo4j:2026.07.1-community, 임시 컨테이너)

- 풀텍스트 필드 한정 쿼리 `+tenant_ft:u1w1 +(김독자)`가 cjk analyzer에서 **테넌트를 정확히 거른다** — 다른 테넌트 0건, 없는 테넌트 0건
- `SEARCH n IN (VECTOR INDEX ... WHERE n.tenant_id='1:1' LIMIT 10)`가 **Community에서 동작한다**
- `apoc.refactor.mergeNodes` 동작, 기본 언어가 `CYPHER 25`(빈 볼륨 + 환경변수 명시 시)

## 실행 중 잡은 버그

1. `retrieval.py`의 entity facts 쿼리가 `$tenant_id`를 참조하는데 파라미터를 안 넘김 — 정적 대조 스크립트로 발견
2. `check_new_episode`가 인자를 밀어 넣어 `up_to_chapter`가 tenant 자리로 들어감
3. `_FUTURE_NODES_CYPHER`가 전 테넌트를 스캔
4. 판정기가 범위 밖 claim 번호를 지어내면 `claims[idx]`에서 IndexError로 **판정 전체가 죽음**
5. **사실 별칭(F###)을 근거 청크로 펼치는 코드가 evidence 구조를 잘못 읽음** — 오류를 찾아낸 검사일수록 실패하는 모양이었다. 테스트가 잡았다
6. 추출 시작 시 Neo4j 조회가 이벤트 루프를 막음

## 남은 일 (사용자 확인 필요)

- **로컬 Neo4j 업그레이드와 재인덱싱** — 기존 5.26 컨테이너에 인덱싱된 그래프가 있어 건드리지 않았다. compose는 2026.07.1로 바뀌어 있으므로, 볼륨을 비우고 다시 올린 뒤 재인덱싱해야 새 코드가 그 그래프를 찾는다.
- **`detection_findings` 마이그레이션** — `docs/detect-api-spec.md` 5절의 SQL. 배포 전에 적용돼야 한다.
- **Spring 계약 변경 조율** — detect·chat에 userId 추가, detect camelCase 전환, 조회 경로 `/api/detect/jobs/{jobId}`, status 대문자.
- **배포 스크립트** — 기동 진입점이 `src.webapp:app`에서 `src.app:app`으로 바뀌었다. 원격 배포 스크립트가 이 레포 밖(S3/mvp-infra-iac)에 있어 함께 고쳐야 한다.
- **인덱싱 경로의 이벤트 루프 블로킹**(이전부터 있던 문제) — `indexing()`이 async인데 내부는 동기 드라이버라, 한 화 인덱싱 2분 동안 서버 전체가 멈춘다.
