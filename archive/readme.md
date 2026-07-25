# search-agent-workflow — 결

신규 웹소설 회차를 올리면, 기존 회차들과 내용이 어긋나는 부분(설정 오류)을 정확한 표현·근거
화수와 함께 짚어주는 게 핵심 목표인 PoC. 저장은 Neo4j(GraphDB) 하나로 통일했고, MySQL은
화별 원문/요약 같은 "기본정보"만 보관한다. VectorDB(Qdrant)는 더 이상 쓰지 않는다.

개발 계획과 target query 정답지는 [`plan.md`](plan.md) (예전 VectorDB 포함 아키텍처 기준 —
현재와 다름, 히스토리 참고용), 저장소/에이전트 동작 방식을 자세히 알고 싶다면
[`ARCHITECTURE.md`](ARCHITECTURE.md) 참고.

## 원래 비전

소설 집필 어시스턴트 서비스. 에디터가 있고, 우측에는 챗 형태의 소설 집필 에이전트가 있어서,
에이전틱하게 자기가 적어두었던 소설 내용을 물어볼 수도 있고, 다음 작성할 것을 추천받을 수도
있다.

에이전틱하게 구축을 위해 langGraph를 사용할 것이며, tool call을 통해 RAG 등을 지원해야 할
것이다. (주로 retrieval과 관련된 툴이 잇어야 하지 않을까?)

저장 자체는, 에디터에서 뭔가 다 작성 후에, 다른 버튼을 누르면 별도의 인덱싱 파이프라인이
돌아간다 던가 하는 등으로 해야 할 것 같음.

소설에 대한 데이터는 graphDB + vectorDB 에 함께 저장. 따라서 에이전트가 적절하게 멀티턴을
수행하며 예를 들어
"1화에서는 ~했던 것 같은데, 3화에서 ~와 관련된 내용이 있는지 찾아 줄레?"
"이 캐릭터가 예전에 이 물건을 소유하고 있었나?"
이런 질문에 답할 수 있어야 한다.

원래 비전 중 지금 초점은 **설정 오류 리포트**(신규 회차 ↔ 기존 회차 정합성 검사)다.
자유 질의응답(Q&A 챗)은 백엔드(`src/agent.py`)에는 남아 있지만, 지금 프론트엔드에는
페이지가 없다 (아래 "현재 상태" 참고).

## 현재 상태 (모듈화/리팩터링 진행 중)

지금 프론트엔드 두 페이지는 **mock 데이터로 동작한다 — 실제 백엔드를 호출하지 않는다**
(API 비용 없음). 화면 흐름과 리포트 형식을 먼저 굳히고, 이후 실제 파이프라인과 연결할
예정이다.

- **`/` — 원고 접수**: 화 파일을 여러 개 한 번에 올리고, 접수(읽기) 진행 상황과 완료 후
  요약(인물/아이템/장소/사건 수)을 보여준다.
- **`/report` — 정합성 리포트**: 신규 회차 파일 하나를 올리면, 정확히 어느 표현이 어떤
  기존 설정과 왜 어긋나는지를 카드로 보여준다. 발견된 모순뿐 아니라 확인됨/확인 불가
  항목도 접어서 함께 보여준다.

백엔드(`src/agent.py`의 Q&A, `src/contradiction_check.py`의 실제 검사 파이프라인)는
Neo4j 전용으로 리팩터링되어 있고 `/chat`, `/check_episode` API로 여전히 호출 가능하지만,
새 프론트엔드 페이지에서는 아직 연결하지 않았다.

## 아키텍처

```
episode*.txt (원문, 여러 화 일괄)
      │
      ▼  src/indexer.py — 화당 LLM 호출 1회 (entities/facts + 요약 동시 추출)
      │
      ├─▶ Neo4j: (Character/Item/Location/Faction/Skill)         src/graphdb.py
      │          -[:HAS_FACT]->(Fact)-[:ABOUT]->(엔티티)
      └─▶ MySQL: episodes(원문, 화별 요약) — "기본정보"만          src/mysqldb.py

                           │
                           ▼
                  graph_query(question)
                  (자연어 → Cypher → Neo4j, Text2Cypher)   src/tools.py
                           │
        ┌──────────────────┴──────────────────┐
        ▼                                       ▼
  src/agent.py                         src/contradiction_check.py
  LangGraph Q&A 루프                    claim 추출 → graph_query로 근거
  (agent↔tools↔grade,                   검색(재검색 루프 포함) → 모순 판정
   /chat API, 프론트 미연결)             → 리포트 (/check_episode API,
                                          프론트 미연결 — 지금은 mock)

        src/webapp.py (FastAPI)
        + static/upload.html (원고 접수, mock)
        + static/report.html (정합성 리포트, mock)
```

## 핵심 코드 위치

| 무엇 | 파일 |
|---|---|
| Q&A 에이전트 루프 (그래프 조립, 라운드 상한, 종료 로직) | `src/agent.py` |
| 설정 오류 검사 파이프라인 (claim 추출 → 검색 → 판정 → 리포트) | `src/contradiction_check.py` |
| Tool 정의 (`graph_query`, Text2Cypher) | `src/tools.py` |
| Neo4j 연결/쿼리 가드/통계 | `src/graphdb.py` |
| MySQL 연결 ("기본정보": episodes 원문+요약) | `src/mysqldb.py` |
| 인덱싱 파이프라인 (원문 → Neo4j/MySQL 동시 기록) | `src/indexer.py`, `src/llm_extractor.py` |
| API 서버 | `src/webapp.py` |
| 프론트엔드 (mock) | `static/upload.html`, `static/report.html` |
| 로깅 설정 (tool 호출/생성 Cypher/grade 판정 기록) | `src/logging_config.py` |
| 평가 스크립트 | `src/evaluate.py`, `src/evaluate_v2.py`, `src/evaluate_contradiction.py` |
| 설정오류(모순) 탐지용 평가 데이터셋 | `eval/contradiction_test_set.json`, `eval/draft_episode_test.txt` |

### Q&A 에이전트 루프 요약

- `agent` 노드가 질문을 보고 `graph_query`를 호출한다 (필요하면 여러 번).
- 도구 호출 결과는 `grade` 노드가 "충분한지" 판단(Self-RAG 스타일). 불충분하면 힌트를 남기고
  `agent`로 돌아가 재검색한다.
- `MAX_TOOL_ROUNDS`(기본 3)에 도달하면 `route_after_agent`가 도구를 아예 바인딩하지 않은
  `final_answer` 노드로 강제 이동시켜 종료를 물리적으로 보장한다(무한 루프 방지).
- 멀티턴은 별도 rewrite 노드 없이 시스템 프롬프트 지침 + `MemorySaver`(thread_id 기준)로 처리.

## 실행 방법

```bash
# 1. DB 기동 (MySQL + Neo4j)
docker compose up -d

# 2. .env 작성 (OPENAI_API_KEY 등 — .env.example 참고)

# 3. 의존성 설치
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 4. 인덱싱 (최초 1회, episode*.txt는 별도로 data/ 에 준비 필요 — 저작권상 저장소에는 미포함)
.venv/bin/python -m src.indexer

# 5. 서버 실행
.venv/bin/uvicorn src.webapp:app --host 127.0.0.1 --port 8000
# 브라우저에서 http://127.0.0.1:8000 (원고 접수), http://127.0.0.1:8000/report (정합성 리포트)
```

로그: `logs/agent.log` (라운드별 tool 호출 인자, 생성된 Cypher, grade 판정 기록).

## 알려진 한계

자세한 내용은 [`ARCHITECTURE.md`](ARCHITECTURE.md) 참고. 요약하면:

- 새 프론트엔드(원고 접수/정합성 리포트)는 아직 mock 데이터로만 동작 — 실제 백엔드
  미연결.
- 에디터 통합, 증분 인덱싱, 레트콘 대응, "다음 작성 추천" 기능 없음.
- 대화 상태가 인메모리(`MemorySaver`)라 서버 재시작 시 소실.
- 설정 오류 판정 정확도는 (리팩터링 전 VectorDB 포함 버전 기준) 15개 정답셋에서 93% —
  Neo4j 단독 구조로 바뀐 뒤 재검증 필요.
