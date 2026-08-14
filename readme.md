# search-agent-workflow — 결

소설 원고를 지식 그래프로 인덱싱하고, 새 회차가 기존 설정과 어긋나는지 검사하고,
작가의 질문에 그래프를 근거로 답하는 파이썬 워커.

CloudFront → Spring(8080) → **이 서버(127.0.0.1:8000)** 구조의 맨 안쪽이다. 외부에
노출되지 않고 Spring만 호출한다.

## 디렉토리 구조

```
search-agent-workflow/
├─ src/
│  ├─ app.py                  # FastAPI 조립. 진입점은 src.app:app
│  ├─ config/                 # .env 로드, 모델·경로 상수
│  ├─ common/                 # Tenant(소설 격리 키), OpenAI 호출 관문, 토큰 집계
│  ├─ controller/             # 라우트와 상태 코드만 안다 (health/index/detect/chat)
│  ├─ dto/                    # 와이어 포맷 (index·detect는 camelCase, chat만 snake_case)
│  ├─ service/
│  │  ├─ index/               # 인덱싱 — 작업 큐·워커·TPM + 추출 파이프라인·요약·병합
│  │  ├─ detect/              # 설정 오류 탐지 — 추출 → 검색 → 판정 3단계
│  │  ├─ chat/                # 작가 Q&A 에이전트
│  │  └─ kg_scope.py          # 요청을 KG 테넌트로 해소하는 유일한 지점
│  └─ repository/
│     ├─ neo4j/               # 드라이버·청크·사실·근거·검색 (그래프)
│     └─ postgres/            # 원고 조회, 탐지 결과 기록 (Spring과 공유하는 DB)
├─ scripts/eval_claims.py     # 탐지 파이프라인 평가 하네스. 프롬프트 문안의 원천이다
├─ tests/                     # LLM·DB를 실제로 부르지 않는다(전부 가짜로 대체)
├─ docs/
│  ├─ indexing-api-spec.md    # Indexing API 스펙 (Spring 팀용)
│  ├─ detecting-api-spec.md   # Detecting API 스펙 (Spring 팀용)
│  └─ claim-pipeline-eval-result.md  # 파이프라인 확정 근거(실측)
├─ data/                      # 원문(저작권상 미커밋)
├─ docker-compose.yml         # 로컬 Neo4j 2026.07 + PostgreSQL 17
├─ requirements.txt           # 운영 의존성
├─ requirements-dev.txt       # + 테스트
└─ archive/                   # 이전 세션의 자체 구현. 참고용 보존, 실행 대상 아님
```

## 지식 그래프 계층

원래 팀원 레포(`lorekeeper-poc`)를 editable 설치해 라이브러리로 쓰다가, 그쪽에 쌓인
로컬 수정이 어느 레포에도 저장되지 않는 문제 때문에 이 레포로 들여왔다. 지금은
`src/repository/neo4j/`(쿼리)와 `src/service/index/`(인덱싱 정책)로 나뉘어 있다.

```python
from src.common.tenant import Tenant
from src.service.index.indexing_service import indexing
from src.repository.neo4j.retrieval import build_retrievers, build_retrieval_tools

tenant = Tenant.of(user_id=42, work_id=7)   # 소설 한 편 = 테넌트
await indexing(tenant, chapter=6, text="…")
retrievers = build_retrievers(tenant)       # {"hybrid_cypher", "fact_search", "entity_search"}
tools = build_retrieval_tools(tenant)       # LLM 도구로 감싼 것
```

**모든 그래프 접근이 `Tenant`를 요구한다.** 그래프가 소설별로 격리돼 있고, 필터를
빠뜨린 경로가 타입으로 드러나게 하려는 설계다.

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
# 1. DB 기동 — Neo4j + PostgreSQL
docker compose up -d

# 2. .env 구성 (.env.example 참고)
cp .env.example .env    # OPENAI_API_KEY 와 DATABASE_URL 을 채운다

# 3. 의존성 설치
python3.12 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt   # 운영만 필요하면 requirements.txt

# 4. 서버 실행
.venv/bin/uvicorn src.app:app --host 127.0.0.1 --port 8000

# 테스트 (LLM·DB 를 실제로 부르지 않는다)
.venv/bin/pytest -q
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
휠이 다르기 때문이다.

원고(`data/`)와 리포트(`reports/`)는 아티팩트에 담지 않는다. 서버의 `/opt/agent/state/`를
심볼릭 링크로 연결해 재배포해도 유지된다.

## 남은 작업

- **Spring 계약 조율** — 탐지·채팅 요청에 `userId` 추가, 탐지는 camelCase 전환,
  조회 경로가 `/api/detect/jobs/{jobId}`로 옮겨갔고 status 어휘가 대문자가 됐다.
  `detection_findings` 스키마 마이그레이션도 배포 전에 적용돼야 한다
  (`docs/detecting-api-spec.md` 5절).
- **배포 스크립트** — 기동 진입점이 `src.webapp:app`에서 `src.app:app`으로 바뀌었다.
  원격 배포 스크립트는 이 레포 밖(mvp-infra-iac)에 있어 함께 고쳐야 한다.
- **하네스 재측정** — 검출 22/25는 Neo4j 5.26에서 잰 값이다. 2026.07은 검색 쿼리
  경로가 달라(라이브러리가 SEARCH 절로 전환) 근거가 달라질 수 있다. 한 번 다시 돌려
  수치를 확인하는 편이 좋다.
- **인덱싱의 이벤트 루프 블로킹** — `indexing()`이 async인데 내부는 동기 드라이버라,
  한 화 인덱싱(약 2분) 동안 서버 전체가 멈춘다. 예전부터 있던 문제다.
