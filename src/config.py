import os
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
# .env는 여기서 딱 한 번 읽는다. 다른 모듈은 os.environ만 보므로, 이 모듈이 어떤 경로로든
# 가장 먼저 import되기만 하면 된다(src/app.py 첫 줄이 그 역할을 한다).
load_dotenv(ROOT_DIR / ".env")

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.6-terra")

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
