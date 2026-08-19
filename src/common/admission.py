"""새 작업을 접수해도 되는지 판단하는 공통 부분.

인덱싱과 탐지는 **같은 모델 버킷을 나눠 쓴다**(둘 다 EXTRACTION_MODEL). 그래서 "OpenAI
한도가 거의 비었는가"는 두 서비스가 같은 답을 봐야 하는 질문이고, 여기 한 곳에 둔다.

반대로 "무엇이 얼마나 밀렸는가"는 공통이 아니다 — 인덱싱은 워커가 하나라 **큐가 자라고**,
탐지는 큐 없이 전부 동시에 도니 **한꺼번에 터진다.** 같은 지표로 잴 수 없어서 각 서비스가
자기 사정으로 판단한다. 이 모듈은 거기 얹는 마지막 안전망이다.

## 왜 "회차 하나가 쓸 토큰"으로 재지 않는가

정확히 알 수 없기 때문이다. 인덱싱 한 회차는 추출·요약·병합·임베딩을 합쳐 2분에 걸쳐
소비하는데, 그 총량은 원고 길이와 그래프 크기에 따라 크게 달라진다. 예전 게이트가 글자수
휴리스틱으로 추정하다가 실제와 어긋난 이유가 이것이다.

그래서 절대량 대신 **여유 비율**을 본다. "버킷이 거의 비었으면 새 작업을 시작하지 않는다"는
판단에는 정확한 소비량 추정이 필요 없다.

## 큐를 보지 않는다

대기 중인 작업이 앞으로 쓸 토큰을 여기에 더하면 안 된다. 잔량은 **지금 이 순간의 스톡**이고
큐의 소비는 **앞으로 수십 분에 퍼지는 플로우**라, 둘을 한 저울에 올리면 큐가 조금만 쌓여도
영구 거절이 된다(그동안 버킷은 계속 충전되는데 그걸 못 본다).

이 함수는 오직 "지금 새 작업을 하나 더 시작할 자리가 있는가"만 답한다.
"""

from __future__ import annotations

import math
import os
import time

from src.common import llm_limit

# 잔량이 한도의 이 비율 아래로 떨어지면 새 작업을 받지 않는다.
# 진행 중인 작업은 계속 돌고(게이트웨이 세마포어와 백오프가 받아낸다), 새로 시작하는 것만
# 막는다 — 이미 절반쯤 진행된 회차를 굶기는 것보다 낫다.
_MIN_HEADROOM_RATIO = float(os.environ.get("ADMISSION_MIN_HEADROOM_RATIO", "0.1"))

# reset 시각을 아직 모를 때 쓸 대기 시간. 버킷은 연속 충전이라 몇 초면 상당량이 차므로
# 길게 잡을 이유가 없다.
_DEFAULT_RETRY_AFTER = 10


def budget_retry_after(model: str) -> int | None:
    """이 모델 버킷에 새 작업을 시작할 여유가 있는지 본다.

    반환:
        None  — 여유가 있다(접수해도 된다)
        int   — 부족하다. 호출자가 429의 Retry-After로 쓸 초

    헤더를 아직 한 번도 못 본 모델은 통과시킨다. 관측이 없는 상태에서 거절하면 서버가
    기동 직후 아무 요청도 못 받는다 — 첫 호출이 나가야 헤더가 오고, 헤더가 와야 판단할 수
    있다는 순환에 빠진다.
    """
    axis = llm_limit.remaining(model)
    if axis.limit is None or axis.remaining is None:
        return None

    if axis.remaining >= axis.limit * _MIN_HEADROOM_RATIO:
        return None

    if axis.reset_at is None:
        return _DEFAULT_RETRY_AFTER
    # reset_at은 time.monotonic() 기준이다(llm_limit이 벽시계를 안 쓰는 이유는 그쪽 주석 참고).
    return max(1, math.ceil(axis.reset_at - time.monotonic()))
