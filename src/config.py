import os
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
# .env는 여기서 딱 한 번 읽는다. 다른 모듈은 os.environ만 보므로, 이 모듈이 어떤 경로로든
# 가장 먼저 import되기만 하면 된다(src/app.py 첫 줄이 그 역할을 한다).
load_dotenv(ROOT_DIR / ".env")

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
# **채팅 전용** 모델. 예전에는 탐지(추출·판정)도 이 값을 썼는데, 탐지 성능 수치는
# 평가 하네스가 EXTRACTION_MODEL로 잰 것이라 서비스가 다른 모델로 돌면 그 수치를
# 물려받지 못한다. 지금은 탐지가 EXTRACTION_MODEL을 쓰고 여기는 채팅만 본다.
#
# env 키는 역할이 드러나는 CHAT_MODEL이 정본이다. OPENAI_MODEL은 프로덕션 배포
# (mvp-infra-iac가 SSM에서 agent.env를 만든다)가 아직 쓰는 구키라 과도기 fallback으로만
# 남겨둔다 — 인프라의 키 교체가 끝나면 제거한다.
CHAT_MODEL = os.environ.get("CHAT_MODEL") or os.environ.get("OPENAI_MODEL") or "gpt-5.6-terra"

DATA_DIR = ROOT_DIR / "data"

# ---------- KG 인덱싱·검색 모델 ----------
# 원래 인덱싱 파이프라인 안에 있던 상수인데, 검색 계층(repository/neo4j/retrieval.py)도
# 같은 임베딩 모델을 써야 한다. repository가 service를 import하면 레이어 방향이 뒤집히므로
# 두 레이어가 함께 올려다보는 config로 옮긴다.
#
# 추출용 모델은 GPT-5 계열이라 temperature/top_p 같은 샘플링 파라미터를 넘기지 않는다
# (비기본 temperature는 400으로 거부된다).
#
# **탐지(추출·판정)도 이 모델을 쓴다.** 이름은 인덱싱 추출에서 왔지만, 둘 다 "정해진
# 구조를 채우는 기계적 판정"이라는 같은 성격이고 무엇보다 평가 하네스가 이 모델로
# 검출 22/25를 쟀다(scripts/eval_claims.py의 EXTRACT_MODEL). 서비스가 다른 모델로 돌면
# 그 수치는 근거를 잃는다. 추론 강도는 용도별로 다르다 — 인덱싱은 high(하드코딩),
# 탐지는 미지정(하네스 기본값).
#
# env 키는 상수명과 같은 EXTRACTION_MODEL이 정본이고, LOREKEEPER_MODEL은 프로덕션
# 배포가 아직 쓰는 구키라 과도기 fallback이다(CHAT_MODEL 주석 참고).
EXTRACTION_MODEL = (
    os.environ.get("EXTRACTION_MODEL") or os.environ.get("LOREKEEPER_MODEL") or "gpt-5.6-luna"
)
# 청크·사실 임베딩 모델. 인덱싱이 쓰는 것과 검색이 쓰는 것이 반드시 같아야 한다 —
# 다르면 같은 공간의 벡터가 아니라 검색 결과가 조용히 무의미해진다.
EMBEDDING_MODEL = "text-embedding-3-small"
