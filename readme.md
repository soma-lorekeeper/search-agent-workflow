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
├─ static/                # 개발용 데모 페이지 (제품 화면 아님 — 제품 프론트는 API 서버 쪽이다)
│  ├─ upload.html         # 원고 접수 (POST /api/index — 여러 화를 한 번에, jobId는 서버가 발급)
│  └─ library.html        # 원고 목록 + 뷰어
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

## 이 서버의 위치

**API 서버(`lorekeeper-backend`)가 호출하는 쪽이다.** 스스로 외부 요청을 받지 않는다.

프로덕션에서는 API 서버와 **같은 EC2**에 뜨고 `127.0.0.1:8000`에만 바인딩한다.
보안 그룹에도 열려 있지 않아서 EC2 주소를 알아도 외부에서 접근할 수 없다.

```
CloudFront ──/api/*──▶ Spring (:8080) ──▶ 이 서버 (127.0.0.1:8000)
                          │                    │
                          └──────┬─────────────┘
                                 ▼
                    PostgreSQL / Neo4j  (두 서버가 같은 DB를 본다)
```

## 로컬 실행

```bash
# 1. 별도 레포를 라이브러리로 클론 (.gitignore 대상이라 이 레포에 없다)
git clone https://github.com/Gomdadi/lorekeeper-poc.git

# 2. DB 기동 — Neo4j + PostgreSQL
docker compose up -d

# 3. .env 구성 (.env.example 참고)
cp .env.example .env    # OPENAI_API_KEY 를 채운다

# 4. 의존성 설치 (lorekeeper-poc 를 editable install)
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 5. 서버 실행
.venv/bin/uvicorn src.app:app --host 127.0.0.1 --port 8000
```

> **파이썬은 3.12로 고정한다.** 의존성 `python-mecab-ko`의 aarch64 휠이 cp312까지만
> 제공된다. CI와 프로덕션도 같은 버전을 쓴다.

> **포트 충돌 주의** — `lorekeeper-backend`의 `compose.dev.yml`도 5432와 7687을 쓴다.
> 두 레포는 **같은 DB를 공유하는 것이 정상**이므로, 둘 중 한쪽만 띄우고 나머지는 그걸
> 그대로 쓰면 된다.

### 확인

```bash
curl localhost:8000/api/health | python3 -m json.tool
```

## 환경 분리

| | 로컬 | 프로덕션 |
|---|---|---|
| 설정 | `.env` | `/opt/agent/agent.env` — 배포 시 SSM/Secrets Manager 에서 생성 |
| Neo4j | `docker compose` 컨테이너 | 전용 EC2 (사설 IP) |
| PostgreSQL | `docker compose` 컨테이너 | RDS. 비밀번호는 RDS가 Secrets Manager에서 회전 |
| OpenAI 키 | `.env` | SSM SecureString `/mono/openai_api_key` |

**프로덕션 자격증명은 이 레포에 없다.** 인스턴스가 자기 IAM 역할로 읽어간다.
전체 그림은
[deploy-local-and-prod.md](https://github.com/soma-lorekeeper/mvp-infra-iac/blob/main/deploy-local-and-prod.md).

## 연동 점검 — `GET /api/health`

이 서버가 두 DB에 실제로 닿는지 점검한다. API 서버가 이 결과를 받아 자기 것과 합쳐
프론트에 내려준다.

```json
{
  "service": "agent",
  "status": "ok",
  "checks": {
    "neo4j":    { "ok": true, "detail": { "uri": "bolt://..." },  "latency_ms": 10.1 },
    "postgres": { "ok": true, "detail": { "server": "PostgreSQL 17.10", "target": "host:5432/db" }, "latency_ms": 13.6 }
  }
}
```

**DB가 죽어도 HTTP 200을 준다.** 상태는 본문의 `status`로 구분한다 — 5xx를 내면 호출자가
"에이전트가 죽음"과 "에이전트는 살아있고 DB만 죽음"을 구분하지 못한다.

`detail.target`에는 호스트/DB만 남긴다. `DATABASE_URL`에 섞인 자격증명은 노출하지 않는다.

## 배포

`main`에 푸시하면 GitHub Actions가 소스를 묶어 S3에 올리고 SSM으로 EC2에서 설치·재기동한다.

의존성 설치는 러너가 아니라 **인스턴스에서** 한다 — 러너는 x86_64, 인스턴스는 arm64라
휠이 다르기 때문이다. `lorekeeper-poc`도 설치 직전에 클론한다.

원고(`data/`)와 리포트(`reports/`)는 아티팩트에 담지 않는다. 서버의 `/opt/agent/state/`를
심볼릭 링크로 연결해 재배포해도 유지된다.

## 남은 작업 (다음 단계)

- `lorekeeper.indexing()`을 실제로 호출하는 인덱싱 진입점 (원고 접수 페이지와 연결)
- `build_retrieval_tools()` 기반 LangGraph Q&A 에이전트 재구현 (archive의 `agent.py` 대체)
- lorekeeper 스키마(`Character/Location/Event/CharacterState/Organization/Item`) 위에서
  동작하는 설정 오류 탐지 파이프라인 재구현 (archive의 `contradiction_check.py` 대체,
  `entity_state_history` 리트리버 활용 검토)
- 프론트 4페이지를 mock에서 실제 API 연결로 전환
