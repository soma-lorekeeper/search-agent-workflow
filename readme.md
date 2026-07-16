# search-agent-workflow

소설 세계관에 대해 질문하면 VectorDB(Qdrant)와 GraphDB(Neo4j)를 상황에 맞게 조합해 조회하고
답변하는 LangGraph 기반 질의응답 에이전트 PoC. 『전지적 독자 시점』 1~6화 원문을 인덱싱
대상으로 검증했다.

개발 계획과 target query 정답지는 [`plan.md`](plan.md), 저장소/에이전트 동작 방식을 자세히
알고 싶다면 [`ARCHITECTURE.md`](ARCHITECTURE.md) 참고.

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

이 PoC는 이 비전 중 **Q&A 코어**(에디터 통합, 인덱싱 버튼, "다음 작성 추천" 기능은 제외)만
구현한 것이다.

## 아키텍처

```
episode1~6.txt (원문)
      │
      ▼  src/indexer.py  (화당 LLM 호출 1회: 청크 임베딩 + entities/facts 동시 추출)
      │
      ├─▶ Qdrant (청크 임베딩)              src/vectordb.py
      └─▶ MySQL (entities/facts, 1차 저장)  src/mysqldb.py
                │
                ▼  src/graph_migrate.py (LLM 재호출 없이 그대로 이관)
              Neo4j: (Character/Item/Location/Faction/Skill)
                     -[:HAS_FACT]->(Fact)-[:ABOUT]->(엔티티)

                          │
        ┌─────────────────┴─────────────────┐
        ▼                                     ▼
  vector_search(query)                 graph_query(question)
  (Qdrant 의미 검색)                    (자연어 → Cypher → Neo4j, Text2Cypher)
        └─────────────────┬─────────────────┘
                           ▼
              src/agent.py — LangGraph 에이전트 루프
              agent ↔ tools ↔ grade (Self-RAG 스타일), 라운드 상한 하드캡
                           │
                           ▼
        src/webapp.py (FastAPI) + static/index.html (채팅 UI)
```

## 핵심 코드 위치

| 무엇 | 파일 |
|---|---|
| 에이전트 루프 (그래프 조립, 라운드 상한, 종료 로직) | `src/agent.py` |
| Tool 정의 (`vector_search`, `graph_query`) | `src/tools.py` |
| Neo4j 연결/쿼리 가드 | `src/graphdb.py` |
| Qdrant 연결 | `src/vectordb.py` |
| 인덱싱 파이프라인 (원문 → 청크+엔티티 추출) | `src/indexer.py`, `src/llm_extractor.py` |
| MySQL → Neo4j 마이그레이션 | `src/graph_migrate.py` |
| API 서버 / 채팅 프론트 | `src/webapp.py`, `static/index.html` |
| 로깅 설정 (tool 호출/생성 쿼리/grade 판정 기록) | `src/logging_config.py` |
| 평가 스크립트 (target query, held-out query) | `src/evaluate.py`, `src/evaluate_v2.py` |
| 설정오류(모순) 탐지용 평가 데이터셋 | `eval/contradiction_test_set.json` |

### 에이전트 루프 요약

- `agent` 노드가 질문을 보고 `vector_search`/`graph_query` 중 필요한 도구를 선택해 호출한다
  (둘 다 호출 가능).
- 도구 호출 결과는 `grade` 노드가 "충분한지" 판단(Self-RAG 스타일). 불충분하면 힌트를 남기고
  `agent`로 돌아가 재검색한다.
- `MAX_TOOL_ROUNDS`(기본 3)에 도달하면 `route_after_agent`가 도구를 아예 바인딩하지 않은
  `final_answer` 노드로 강제 이동시켜 종료를 물리적으로 보장한다(무한 루프 방지).
- 멀티턴은 별도 rewrite 노드 없이 시스템 프롬프트 지침 + `MemorySaver`(thread_id 기준)로 처리.

## 실행 방법

```bash
# 1. DB 기동 (Qdrant + MySQL + Neo4j)
docker compose up -d

# 2. .env 작성 (OPENAI_API_KEY 등 — .env.example 참고)

# 3. 의존성 설치
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 4. 인덱싱 (최초 1회, episode1~6.txt는 별도로 data/ 에 준비 필요 — 저작권상 저장소에는 미포함)
.venv/bin/python -m src.indexer
.venv/bin/python -m src.graph_migrate   # MySQL → Neo4j (LLM 재호출 없음)

# 5. 서버 실행
.venv/bin/uvicorn src.webapp:app --host 127.0.0.1 --port 8000
# 브라우저에서 http://127.0.0.1:8000
```

로그: `logs/agent.log` (라운드별 tool 호출 인자, 생성된 Cypher/SQL, grade 판정 기록).

## 알려진 한계

자세한 내용은 [`ARCHITECTURE.md`](ARCHITECTURE.md) 8절 참고. 요약하면:

- 에디터 통합, 증분 인덱싱, 레트콘 대응, "다음 작성 추천" 기능 없음 (Q&A 코어만 구현).
- 대화 상태가 인메모리(`MemorySaver`)라 서버 재시작 시 소실.
- `structured_query`(MySQL/Text2SQL)는 `graph_query`(Neo4j/Text2Cypher)로 대체되어 현재
  에이전트에는 연결되어 있지 않다 (코드와 데이터는 남아 있음).
- 설정오류(모순) 탐지는 미구현 — 평가 데이터셋(`eval/contradiction_test_set.json`)만 준비됨.
