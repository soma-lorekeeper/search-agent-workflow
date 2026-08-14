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
