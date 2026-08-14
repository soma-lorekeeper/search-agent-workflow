"""테스트 공통 준비.

src.config가 import 시점에 os.environ["OPENAI_API_KEY"]를 읽으므로, 테스트 모듈이
src.app을 import하기 전에 자리표시자를 넣어둔다. 테스트는 LLM을 실제로 부르지 않는다
(run_indexing을 스텁으로 갈아끼운다).
"""

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-placeholder")


class RecordingDict(dict):
    """쓰기가 일어날 때마다 그 직후의 내용을 통째로 기록하는 dict.

    작업 상태(job state)를 담아두면 "조회가 볼 수 있었던 모든 중간 상태"가 snapshots에 남는다.
    상태 API는 작업과 다른 스레드에서 돌기 때문에, 상태와 결과를 두 줄로 나눠 쓰면 그 사이의
    한 순간(예: status="done"인데 findings는 아직 None)이 실제로 호출자에게 보인다 —
    호출자는 그걸 최종 결과로 믿고 폴링을 멈춘다. 그 한 순간이 존재했는지를 여기서 검사한다.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snapshots: list[dict] = []

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.snapshots.append(dict(self))

    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)
        self.snapshots.append(dict(self))
