# 구현 요약 (2026-07-12)

`plan.md`의 계획에 따라, 『전지적 독자 시점』 1~6화를 대상으로 소설 세계관 질의응답
에이전트를 구현하고 검증했다.

## 아키텍처

```
episode1~6.txt → [인덱싱, src/indexer.py] → Qdrant(청크 임베딩) + MySQL(entities/facts/episode_summaries)
                                                      │
                              [LangGraph 에이전트, src/agent.py] ← vector_search / structured_query
                                                      │
                              FastAPI(src/webapp.py) + static/index.html (채팅 UI)
```

- **VectorDB**: Qdrant. 화 원문을 문단 단위로 그리디하게 묶어 ~1200자 청크로 저장(오버랩 없음).
- **구조화 저장소**: MySQL. Neo4j 대신 `entities`(인물/아이템/장소 등)와
  `facts`(주어-서술어-목적어 + 화 번호 + 유효구간)로 그래프 성격 데이터를 표현.
- **인덱싱**: 화당 LLM 호출 1회로 청크 임베딩 대상 텍스트 + entities/facts JSON을 동시에 추출.
  이전 화 전체를 다시 프롬프트에 넣지 않고, 알려진 엔티티 이름 목록만 넘겨 비용 절감.
- **툴 2개**: `vector_search`(의미 검색), `structured_query`(자연어 질문 → LLM이 SQL SELECT
  생성 → 실행, Text2SQL). SELECT 이외 문장은 정규식으로 차단.
- **에이전트 루프**: `agent ↔ tools ↔ grade` (Self-RAG 스타일 충분성 평가 + 재검색), 최대
  3라운드. 라운드 초과 시 tools를 bind하지 않은 `final_answer` 노드로 강제 이동해 종료를
  물리적으로 보장(초기엔 소프트 캡이라 무한루프 위험이 있었음, 이후 하드캡으로 수정).
- **멀티턴**: 별도 rewrite 노드 없이, 시스템 프롬프트에 "지시어를 대화 기록에서 스스로
  해석하라"는 지침으로 처리. `MemorySaver`로 `thread_id` 기준 대화 상태 유지(인메모리).
- **로깅**: `src/logging_config.py`, 각 라운드의 tool 호출 인자/생성 SQL/grade 판정을
  `logs/agent.log`에 기록.

## 검증

- 최초 target query 7개(정답지는 `plan.md`) + 이후 추가한 held-out 쿼리 6개, 총 13개 모두
  핵심 사실 포함 + 할루시네이션 없음 기준으로 통과.
- 부정 케이스(예: "김독자가 유중혁과 정식 계약 맺었어?", "이길영이 어룡을 처치했어?")에서
  실제로 없는 사실을 지어내지 않고 정확히 "없음"으로 답변.

## 발견하고 고친 버그 (전부 특정 쿼리 전용이 아닌 일반 버그)

1. Text2SQL이 엔티티 이름을 주어(subject) 쪽만 필터링 → 목적어(object)로 등장하는 사건을
   놓침 (예: "천인호가 누구 손에 죽었나" 계열 질문이 비어서 나옴).
2. Text2SQL이 컬럼에 `killer_name`/`victim_name` 같은 의미론적 별칭을 붙이며 주어-목적어를
   반대로 라벨링 → 가해자/피해자가 뒤바뀔 뻔함. 중립적 컬럼명(`subject_name`/`object_name`)
   강제로 해결.
3. 이름으로 필터링한 뒤에도 predicate로 추가 필터링 → 동의어 predicate(`owned_by` vs
   `gave`)로 기록된 관련 사실을 놓침.
4. **에이전트 루프 무한 재시도 위험**: `MAX_TOOL_ROUNDS` 캡이 라우팅 함수에서 실제로
   강제되지 않고 LLM에게 "그만하라"고 텍스트로만 부탁하는 소프트 제약이었음. 라우팅
   함수가 `tool_rounds`를 직접 확인해 캡 도달 시 도구 호출이 불가능한 `final_answer`
   노드로 강제 이동하도록 수정, `recursion_limit`도 이중 안전장치로 추가.

## 알려진 한계

- **"소설 전체 요약해줘" 같은 전역 질문에서 사전학습 지식으로 할루시네이션 발생**:
  인덱싱 범위(1~6화)를 넘어서는 정보를 벡터 검색이 못 찾자, grade가 "더 찾아보라"고
  반복 요구하다 3라운드 소진 후 에이전트가 실제 원작(551화 완결작)의 결말·후반부
  내용을 자기 지식으로 지어내 답변함. 시스템 프롬프트에 "DB가 1~6화로 한정됨을
  명시하라"는 지침 부재, `episode_summaries` 테이블(화별 요약이 이미 인덱싱되어 있음)을
  `structured_query`가 활용하도록 유도하는 설명 부재가 원인. **미해결 — 다음 세션 과제.**
- 에디터 통합, 인덱싱 버튼/증분 인덱싱, 레트콘 대응, "다음 작성할 내용 추천" 기능 없음
  (readme.md 원 비전 대비 Q&A 코어만 구현됨).
- 대화 상태가 인메모리(`MemorySaver`)라 서버 재시작 시 소실. 멀티테넌시/인증 없음.
- 질문 1건당 비용은 라운드 수와 `vector_search` top_k에 따라 크게 달라짐 (구조화 조회
  위주 1라운드 시 약 $0.03~0.04, 2라운드+대량 벡터검색 시 $0.15~0.30까지). 인덱싱
  자체에 이미 약 $1 소모.

## 실행 방법

```bash
cd agentic-workflow
docker compose up -d                                   # Qdrant(6333) + MySQL(3306)
.venv/bin/python -m src.indexer                         # 최초 1회 인덱싱
.venv/bin/uvicorn src.webapp:app --host 127.0.0.1 --port 8000
# 브라우저에서 http://127.0.0.1:8000
```

로그: `logs/agent.log` (실시간 `tail -f`로 확인 가능).
