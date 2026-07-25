# search-agent-workflow — 결

소설 업로드 → 검색(Q&A) → 설정 오류(모순) 탐지 리포트, 3가지를 제공하는 PoC.

**2026-07-25 재구성**: DB/스키마/인덱싱을 자체 구현 대신 팀원의 [`lorekeeper-poc`](../lorekeeper-poc)를
라이브러리로 가져다 쓰는 구조로 바꿨다. 이 문서 시점 기준으로 **아직 스캐폴딩 단계**이고
(webapp이 정적 mock 페이지만 서빙), 검색/모순탐지 로직을 lorekeeper 위에 다시 구현하는
작업이 남아있다.

## 디렉토리 구조

```
agentic-workflow/
├─ lorekeeper-poc/        # 팀원 레포를 이 폴더 안에 clone (gitignore 대상, 우리 레포에 커밋 안 됨)
│                         # → requirements.txt가 pip install -e 로 편집 가능 설치
│                         # → 내부 로직(poc/src/*.py)은 절대 수정하지 않음, import만
├─ src/                   # 우리 오케스트레이션 레이어 (지금은 최소 스캐폴딩만)
│  ├─ config.py           # .env 로드 (NEO4J_*, OPENAI_API_KEY 등)
│  └─ webapp.py           # FastAPI — 지금은 정적 페이지 서빙만, 백엔드 API 없음
├─ static/                # 프론트 mock 페이지 (archive 이전 세션에서 디자인한 것 그대로 유지)
│  ├─ upload.html         # 원고 접수
│  ├─ library.html        # 원고 목록 + 뷰어
│  ├─ report.html          # 정합성(모순) 리포트
│  └─ chat.html           # Q&A 챗봇
├─ data/                  # 원문(episode*.txt, 저작권상 미커밋) — lorekeeper.indexing()에 넣을 입력
├─ docker-compose.yml     # lorekeeper-poc와 동일한 Neo4j 5.26+APOC 설정 (인증정보 반드시 일치)
├─ requirements.txt
├─ .env / .env.example
└─ archive/               # 이전 세션의 자체 구현(Qdrant→MySQL+Neo4j→Neo4j단독 등 여러 버전 거침).
                           # 참고용으로 보존, 더 이상 실행 대상 아님. 상세: archive/ARCHITECTURE.md, archive/SUMMARY.md
```

## lorekeeper 패키지 사용법

`lorekeeper-poc/poc`를 editable 설치하면 `lorekeeper` 패키지로 import된다 (자세한 스펙은
`lorekeeper-poc/README.md` 참고, 이 레포에서 그 파일을 수정하지 않는다):

```python
from lorekeeper import indexing                          # async def indexing(chapter: int, text: str) -> dict
from lorekeeper import build_retrievers, build_retrieval_tools

retrievers = build_retrievers()   # {"vector_cypher", "hybrid_cypher", "entity_state_history", "text2cypher"}
tools = build_retrieval_tools()   # neo4j_graphrag.tool.Tool 리스트 — LangGraph 등에 배선
```

## 실행 방법

```bash
# 1. Neo4j 기동 (lorekeeper-poc와 동일 인증정보 neo4j/lorekeeper 사용)
docker compose up -d

# 2. .env 확인 (.env.example 참고) — OPENAI_API_KEY, NEO4J_URI/USER/PASSWORD

# 3. 의존성 설치 (lorekeeper-poc를 editable install)
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 4. 서버 실행 (지금은 정적 mock 페이지만 서빙)
.venv/bin/uvicorn src.webapp:app --host 127.0.0.1 --port 8000
```

## 남은 작업 (다음 단계)

- `lorekeeper.indexing()`을 실제로 호출하는 인덱싱 진입점 (원고 접수 페이지와 연결)
- `build_retrieval_tools()` 기반 LangGraph Q&A 에이전트 재구현 (archive의 `agent.py` 대체)
- lorekeeper 스키마(`Character/Location/Event/CharacterState/Organization/Item`) 위에서
  동작하는 설정 오류 탐지 파이프라인 재구현 (archive의 `contradiction_check.py` 대체,
  `entity_state_history` 리트리버 활용 검토)
- 프론트 4페이지를 mock에서 실제 API 연결로 전환
