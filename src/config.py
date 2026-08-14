import os
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
# lorekeeper 패키지(../lorekeeper-poc/poc/src/client.py 등)도 load_dotenv()를 호출하지만,
# 그쪽 탐색 경로는 lorekeeper-poc/ 기준이라 이 프로젝트 루트의 .env를 못 찾는다.
# 여기서 먼저 로드해 NEO4J_*/OPENAI_API_KEY를 프로세스 환경변수로 채워두면,
# lorekeeper 쪽의 load_dotenv()는 이미 있는 값을 덮어쓰지 않으므로(override=False 기본값)
# import 순서와 무관하게 항상 이 .env 값이 사용된다.
load_dotenv(ROOT_DIR / ".env")

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.6-terra")

# 추론 강도(reasoning_effort)를 추출용/검증용으로 분리해 둔다.
# 쓰이는 곳이 둘인데 서로 독립이기 때문이다 — 추출(pipeline._extract_claims_from_chunk)은
# 1단계 후보 선정 성능을, 검증(agent.verify_claim)은 2단계 judge 성능을 좌우한다.
# 지금 값(high)은 확정이 아니라 보수적 출발점이다. 추출은 P2에서 Recall@B를 기준으로,
# 검증은 P4에서 동결 후보 풀 위의 A/B로 각각 medium/high를 재확정한다.
EXTRACT_REASONING_EFFORT = os.environ.get("EXTRACT_REASONING_EFFORT", "high")
VERIFY_REASONING_EFFORT = os.environ.get("VERIFY_REASONING_EFFORT", "high")

DATA_DIR = ROOT_DIR / "data"

# ---------- KG 인덱싱·검색 모델 ----------
# 원래 인덱싱 파이프라인 안에 있던 상수인데, 검색 계층(repository/neo4j/retrieval.py)도
# 같은 임베딩 모델을 써야 한다. repository가 service를 import하면 레이어 방향이 뒤집히므로
# 두 레이어가 함께 올려다보는 config로 옮긴다.
#
# 추출용 모델은 GPT-5 계열이라 temperature/top_p 같은 샘플링 파라미터를 넘기지 않는다
# (비기본 temperature는 400으로 거부된다).
EXTRACTION_MODEL = os.environ.get("LOREKEEPER_MODEL") or "gpt-5.6-luna"
# 청크·사실 임베딩 모델. 인덱싱이 쓰는 것과 검색이 쓰는 것이 반드시 같아야 한다 —
# 다르면 같은 공간의 벡터가 아니라 검색 결과가 조용히 무의미해진다.
EMBEDDING_MODEL = "text-embedding-3-small"
