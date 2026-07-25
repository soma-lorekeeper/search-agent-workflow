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
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.4")

DATA_DIR = ROOT_DIR / "data"
