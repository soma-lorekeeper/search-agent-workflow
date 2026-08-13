"""테스트 공통 준비.

src.config가 import 시점에 os.environ["OPENAI_API_KEY"]를 읽으므로, 테스트 모듈이
src.webapp을 import하기 전에 자리표시자를 넣어둔다. 테스트는 LLM을 실제로 부르지 않는다
(run_indexing을 스텁으로 갈아끼운다).
"""

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-placeholder")
