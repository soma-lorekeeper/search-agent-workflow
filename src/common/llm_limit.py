"""모델별 LLM 호출 한도의 단일 상태 저장소 — 관측(미터)과 통제(세마포어).

이 서버의 OpenAI 호출은 모델마다 **별개의 한도 버킷**을 쓴다(OpenAI의 rate limit이
모델 단위다). 그래서 "얼마나 남았나"도 "몇 개까지 동시에 보낼까"도 전부 모델별이다.

두 가지를 한 모듈에 두는 이유는 키 공간이 같아서다 — 둘 다 모델명으로 찾고, 함께
움직인다(미터로 재서 세마포어 값을 정한다).

## 값의 출처: 헤더가 진실, usage는 보간

OpenAI는 응답 헤더로 남은 양을 알려준다(`x-ratelimit-remaining-tokens` 등). 이 값이
**절대값**이고 조직 전체를 세므로, 우리 프로세스 밖의 소비자(다른 서버, 팀원의 로컬,
평가 하네스)까지 반영돼 있다. 그래서 헤더가 오면 통째로 덮어쓴다.

문제는 헤더를 못 얻는 경로가 있다는 것이다(라이브러리 내부를 지나는 호출). 그쪽은
usage로 차감만 해서 다음 헤더가 올 때까지의 구간을 메운다. 보수적으로만 틀리고,
헤더가 오면 누적 오차가 사라진다.

## 저장 위치: 프로세스 메모리

Redis를 쓰지 않는다. 진짜 카운터는 OpenAI 쪽에 있고 우리가 들고 있는 건 **캐시**라서,
프로세스마다 각자 들고 있어도 다음 응답에서 진실로 수렴한다. 이 서버는 애초에 전체가
단일 프로세스 전제 위에 있다(src/app.py의 인덱싱 워커 주석 참고).
"""

from __future__ import annotations

import asyncio
import os
import re
import threading
import time
import weakref
from dataclasses import dataclass, field

from src.config import EMBEDDING_MODEL

# ---------- 설정값 ----------
#
# 아래 기본값은 **실측으로 정했다**(tests/test_llm_smoke.py, 2026-08-20 관측):
#
#   gpt-5.6-luna           TPM 4,000,000 RPM 5,000
#   text-embedding-3-small TPM 5,000,000 RPM 5,000
#   임베딩 호출 지연        평균 512ms (300~830ms) — 2026-08-16 관측, 한도와 무관
#
# ⚠️ 2026-08-16에는 luna 200,000/500, 임베딩 1,000,000/3,000이었다. **Tier 3로 올라가며
# 한도가 뛰었다.** 아래 상한은 옛 산정식을 그대로 두고 새 한도에 비례해 옮긴 값이라,
# 소비율 대비 여유 비율은 예전과 같다(추출 48% / 판정 162% / 임베딩 73%).
#
# 세마포어는 **거절이 아니라 대기**라 낮게 잡아도 요청이 실패하지 않는다. 그래서
# 안전한 쪽으로 잡고, 처리량이 문제가 되면 환경변수로 올린다.

# LLM은 **TPM이 병목**이다(RPM은 여유 — N=80에 지연 20초면 약 240 RPM, 한도 5,000).
# 기준은 임베딩과 같다: 지속 소비율 < 충전율(4,000,000/60 ≈ 66,667 토큰/초).
#
# 추출 호출 실측(2026-08-16, 실제 프롬프트 + 실제 원고 조각). 모델이 같아 한도가 올라도
# 호출당 소비는 그대로다:
#   입력 4,924 + 출력 2,530 = 8,052 토큰 / 호출,  지연 20.1초 → 슬롯당 약 400 토큰/초
#   N=80 지속 소비율 32,000 토큰/초 = 충전율의 48%
#
# 추출만 보면 더 올려도 되지만, **이 세마포어는 luna 전체가 공유한다.** 그리고 두 단계는
# 크기가 정반대로 자란다:
#
#   추출  조각 3,000자 고정 → 회차가 길면 **호출 수**가 는다 (가로)
#   판정  문서고 전체 1회   → 회차가 길면 **한 호출**이 커진다 (세로)
#
# 판정은 claim 전체를 한 번에 배치로 본다(judge_service의 설계 — 문서고를 한 번만 싣고
# claim 사이 관계까지 보려고). 그래서 쪼개서 흘려보낼 수가 없다. 약 27k 토큰이라는 값은
# openai_client.py의 기존 주석에 있던 관측치이고(우리가 이번에 잰 건 추출뿐이다), 문서고
# 크기가 회차마다 달라 실제로는 분포에 가깝다.
#
# 세마포어는 개수만 세고 무게는 모르므로 무거운 쪽이 몰렸을 때를 기준으로 잡는다:
#
#   슬롯 80개가 전부 추출 →  32,000/초 (48%)   여유
#   슬롯 80개가 전부 판정 → 108,000/초 (162%)  초과 (27k 기준. 긴 회차면 더 크다)
#
# 넘치면 429 백오프가 받아내지만 인덱싱 경로의 백오프가 20/40/80초라 로컬에서 줄 서는
# 편이 훨씬 싸다.
#
# 80인 근거(2026-08-20): Tier 3로 올라가며 TPM이 200,000 → 4,000,000이 됐다(20배).
# 충전율이 3,333/초 → 66,667/초라, 옛 4슬롯이 갖던 여유 비율(추출 48% / 판정 162%)을
# 그대로 유지하는 값이 80이다. 산정식은 바꾸지 않고 한도만 비례해 옮겼다.
# 호출당 소비(추출 슬롯당 400/초, 판정 1,350/초)는 모델이 같아 옛 실측을 그대로 쓴다.
# RPM은 안 걸린다 — 80슬롯 ÷ 20.1초 = 4요청/초로 충전율 83/초의 5%다.
_MAX_CONCURRENCY = int(os.environ.get("LLM_MAX_CONCURRENCY", "80"))

# 임베딩은 반대로 **RPM이 병목**이다. 호출당 토큰이 20~30개뿐이라 TPM 5M은 닿지 않고,
# 대신 탐지 1건이 claim×4채널까지 던져 요청 수가 크다.
#
# 기준은 "한도의 몇 %"가 아니라 **충전 속도**다. RPM 5,000은 60초 창이 아니라 초당 83개씩
# 차는 연속 충전 버킷이다(실측 reset이 ms 단위인 것이 그 증거 — "만수위 복귀까지"의 뜻이라
# 조금 쓰면 즉시 찬다). 그래서 지속 소비율이 충전율을 넘으면 버킷이 서서히 말라 429가 난다.
#
# 동시성별 실측 처리량(2026-08-16). 한도가 아니라 우리 쪽 특성이라 Tier가 올라도 유효하다:
#   N= 8 → 15.2/초   N=16 → 25.2/초   N=24 → 36.7/초   N=32 → 72.4/초
#   지연은 N=32까지도 안 늘었다(456→325ms) — 서버가 아니라 우리 동시성이 유일한 변수다.
#   대략 슬롯당 1.5/초다.
#
# 40인 근거(2026-08-20): Tier 3로 올라가며 RPM이 3,000 → 5,000이 됐다. 충전율이
# 50/초 → 83/초라, 40슬롯이면 약 61/초로 충전율의 73%다 — 옛 24슬롯이 50/초 대비
# 갖던 비율(37/50 = 73%)과 같다. 남는 22/초는 인덱싱의 임베딩(청크·사실) 몫이다 —
# 같은 5,000 버킷을 나눠 쓴다.
_EMBEDDING_CONCURRENCY = int(os.environ.get("EMBEDDING_MAX_CONCURRENCY", "40"))

# 콜드 스타트 가정값. 첫 응답 헤더가 오면 즉시 실제 값으로 교체된다 — 한도가 아니라
# "아직 아무것도 못 본 동안의 임시값"이다. 실측한 luna 값과 같게 두되, 임베딩 버킷은
# 이보다 크므로(5M/5000) 첫 응답 전까지만 과소평가한다(안전한 방향).
_ASSUMED_TPM = int(os.environ.get("LLM_ASSUMED_TPM", "4000000"))
_ASSUMED_RPM = int(os.environ.get("LLM_ASSUMED_RPM", "5000"))


def limit_for(model: str) -> int:
    """이 모델의 동시 호출 상한."""
    return _EMBEDDING_CONCURRENCY if model == EMBEDDING_MODEL else _MAX_CONCURRENCY


# ---------- 미터 ----------


@dataclass
class Axis:
    """한도의 한 축(토큰 또는 요청 수).

    reset_at은 time.monotonic() 기준이다. 벽시계를 쓰면 NTP 조정이나 서머타임에
    값이 흔들린다.
    """

    limit: int | None = None
    remaining: int | None = None
    reset_at: float | None = None


@dataclass
class Bucket:
    """모델 하나의 한도 상태. 토큰·요청 두 축을 함께 든다."""

    tokens: Axis = field(default_factory=Axis)
    requests: Axis = field(default_factory=Axis)
    # 마지막으로 이 버킷이 갱신된 시각(monotonic). None이면 이 모델로 아직 아무 호출도
    # 나가지 않았다는 뜻 — "그 경로가 게이트웨이를 우회했는가"를 판별하는 데 쓴다.
    updated_at: float | None = None


# 모델명 → Bucket. 프로세스 메모리에만 있다.
_buckets: dict[str, Bucket] = {}

# 미터·세마포어 dict를 함께 보호한다. 임베딩 경로가 여러 스레드에서 들어오므로
# (탐지 검색이 스레드풀에서 돈다) 이벤트 루프 단독 전제를 쓸 수 없다.
_lock = threading.Lock()

# "6m0s", "500ms", "1h2m3s" 같은 기간 문자열. OpenAI의 reset 헤더 형식이다.
# ms를 m보다 먼저 시도해야 "500ms"가 "500분 + s"로 잘못 읽히지 않는다.
_DURATION = re.compile(r"(\d+(?:\.\d+)?)(ms|h|m|s)")
_DURATION_UNITS = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 0.001}


def parse_duration(value: str | None) -> float | None:
    """reset 헤더를 초로 바꾼다. 못 읽으면 None(= 이 축은 갱신하지 않는다).

    형식이 바뀌었을 때 예외로 호출을 죽이는 대신 조용히 건너뛴다 — reset 값을 모른다고
    LLM 호출을 실패시킬 이유가 없다. 대신 remaining/limit은 별개로 갱신된다.
    """
    if not value:
        return None
    parts = _DURATION.findall(str(value))
    if not parts:
        return None
    return sum(float(amount) * _DURATION_UNITS[unit] for amount, unit in parts)


def _as_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _seed_axis(axis: Axis, assumed: int) -> None:
    """빈 칸만 가정값으로 메운다.

    **필드마다 따로 본다.** limit이 없다고 remaining까지 덮으면, 헤더가 remaining만
    주는 경우(응답에 일부 필드만 실리는 경우가 있다) 방금 관측한 값을 가정값으로
    지워버린다 — 미터가 조용히 항상 만수위를 가리키게 된다.
    """
    if axis.limit is None:
        axis.limit = assumed
    if axis.remaining is None:
        # 아무것도 못 봤으면 만수위로 가정한다. 시작점이 있어야 spend()가 뺄 수 있다.
        axis.remaining = axis.limit


def _seed(bucket: Bucket) -> None:
    """헤더로 못 채운 칸에 가정값을 넣는다. 첫 observe()가 대부분 덮어쓴다."""
    _seed_axis(bucket.tokens, _ASSUMED_TPM)
    _seed_axis(bucket.requests, _ASSUMED_RPM)


def _refresh(axis: Axis, now: float) -> None:
    """reset 시각이 지났으면 만수위로 되돌린다.

    별도 타이머를 두지 않고 읽을 때 정리한다 — 정확한 만료 시점이 필요한 게 아니라
    "물어보는 순간 기준으로 맞는 답"만 필요하기 때문이다.
    """
    if axis.reset_at is not None and now >= axis.reset_at:
        axis.remaining = axis.limit
        axis.reset_at = None


def _apply(axis: Axis, headers: dict, kind: str, now: float) -> None:
    """헤더 한 축을 축 상태에 반영한다. 없는 필드는 건드리지 않는다."""
    limit = _as_int(headers.get(f"x-ratelimit-limit-{kind}"))
    remaining = _as_int(headers.get(f"x-ratelimit-remaining-{kind}"))
    reset = parse_duration(headers.get(f"x-ratelimit-reset-{kind}"))
    if limit is not None:
        axis.limit = limit
    if remaining is not None:
        axis.remaining = remaining
    if reset is not None:
        axis.reset_at = now + reset


def observe(model: str, headers) -> None:
    """응답 헤더로 이 모델의 상태를 통째로 덮어쓴다.

    **429 응답에도 이 헤더가 붙는다.** 오히려 그때가 가장 정확하므로 실패 경로에서도
    반드시 부른다 — 성공했을 때만 부르면 한도에 걸린 순간의 값을 영영 못 본다.

    headers는 httpx.Headers도, 평범한 dict도 받는다(테스트가 dict를 넘긴다).
    """
    # httpx.Headers는 대소문자를 무시하지만 dict는 아니다. 한 번 낮춰서 둘 다 받는다.
    lowered = {str(k).lower(): v for k, v in dict(headers).items()}
    now = time.monotonic()
    with _lock:
        bucket = _buckets.setdefault(model, Bucket())
        _apply(bucket.tokens, lowered, "tokens", now)
        _apply(bucket.requests, lowered, "requests", now)
        _seed(bucket)  # 헤더에 없는 축이 있으면 가정값으로 메운다
        bucket.updated_at = now


def spend(model: str, tokens: int, requests: int = 1) -> None:
    """헤더를 못 얻는 경로가 쓴 양을 차감한다(보간).

    라이브러리 내부를 지나는 호출은 원본 HTTP 응답이 이미 버려진 뒤라 헤더에 닿을 수
    없다. 그쪽은 usage만 얻을 수 있으므로 remaining에서 빼기만 한다.

    이건 증분이라 오차가 쌓일 수 있지만, 다음 observe()가 절대값으로 갈아치우므로
    누적되지 않는다. 0 아래로는 내리지 않는다 — 음수 잔량은 호출자에게 의미가 없다.
    """
    now = time.monotonic()
    with _lock:
        bucket = _buckets.setdefault(model, Bucket())
        _seed(bucket)
        if bucket.tokens.remaining is not None:
            bucket.tokens.remaining = max(0, bucket.tokens.remaining - max(0, tokens))
        if bucket.requests.remaining is not None:
            bucket.requests.remaining = max(0, bucket.requests.remaining - max(0, requests))
        bucket.updated_at = now


def remaining(model: str) -> Axis:
    """이 모델의 남은 토큰. reset이 지났으면 복구한 뒤 돌려준다.

    반환이 Axis인 이유: 남은 양만으로는 "적은 게 문제인지 곧 풀리는지"를 알 수 없다.
    limit·reset_at을 함께 봐야 호출자가 기다릴지 거절할지 정할 수 있다.
    """
    now = time.monotonic()
    with _lock:
        bucket = _buckets.setdefault(model, Bucket())
        _seed(bucket)
        _refresh(bucket.tokens, now)
        return Axis(
            limit=bucket.tokens.limit,
            remaining=bucket.tokens.remaining,
            reset_at=bucket.tokens.reset_at,
        )


def remaining_requests(model: str) -> Axis:
    """이 모델의 남은 요청 수(RPM 축). 임베딩처럼 토큰은 작고 호출이 잦은 경로에서
    실제 병목이 되는 쪽이다."""
    now = time.monotonic()
    with _lock:
        bucket = _buckets.setdefault(model, Bucket())
        _seed(bucket)
        _refresh(bucket.requests, now)
        return Axis(
            limit=bucket.requests.limit,
            remaining=bucket.requests.remaining,
            reset_at=bucket.requests.reset_at,
        )


def snapshot() -> dict[str, Bucket]:
    """현재 전 모델의 상태 사본. 관측·테스트용이며 이 값을 고쳐도 원본은 안 바뀐다."""
    with _lock:
        return {
            model: Bucket(
                tokens=Axis(b.tokens.limit, b.tokens.remaining, b.tokens.reset_at),
                requests=Axis(b.requests.limit, b.requests.remaining, b.requests.reset_at),
                updated_at=b.updated_at,
            )
            for model, b in _buckets.items()
        }


# ---------- 세마포어 ----------
#
# asyncio.Semaphore는 **생성이 아니라 첫 대기자가 생길 때** 이벤트 루프를 기억한다
# (locks.py의 acquire: 자리가 있으면 숫자만 줄이고 끝, 없을 때만 _get_loop()로
# Future를 만든다). 그래서 모듈 전역 하나로 두면 경합이 없는 동안은 멀쩡히 돌다가,
# 한 번 포화된 뒤 다른 루프에서 쓰는 순간 "bound to a different event loop"로 죽는다.
# 즉시 실패가 아니라 **순서 의존 플래키**라 원인 추적이 어렵다.
#
# 그래서 루프를 키에 넣어 루프마다 자기 세마포어를 갖게 한다. 운영은 루프가 하나뿐이라
# 항목도 하나 — 동작과 비용이 지금과 같고, "루프가 하나여야 한다"는 숨은 전제만 사라진다.
#
# 키를 id(loop)로 잡으면 안 된다: 루프가 GC된 뒤 같은 주소를 다음 루프가 재사용해서
# 죽은 루프의 세마포어를 물려받는다 — 원래 버그가 더 찾기 어려운 형태로 부활한다.
# 강한 참조로 잡으면 딕셔너리가 루프의 GC를 막는다. 그래서 약한 참조다.
_async_slots: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()

# 임베딩용. threading.Semaphore는 루프에 묶이지 않으므로 프로세스 전역 하나면 된다.
_thread_slots: dict[str, threading.Semaphore] = {}


def async_slot(model: str) -> asyncio.Semaphore:
    """이 루프·이 모델의 동시 호출 세마포어.

    루프 밖에서 부르면 RuntimeError가 난다 — 그게 맞다. 루프 없이 asyncio 세마포어를
    쓸 방법이 없으므로 조용히 다른 걸 돌려주는 것보다 즉시 드러나는 편이 낫다.
    """
    loop = asyncio.get_running_loop()
    with _lock:
        per_model = _async_slots.setdefault(loop, {})
        semaphore = per_model.get(model)
        if semaphore is None:
            semaphore = asyncio.Semaphore(limit_for(model))
            per_model[model] = semaphore
        return semaphore


def thread_slot(model: str) -> threading.Semaphore:
    """이 모델의 동시 호출 세마포어(스레드용).

    임베딩은 동기 인터페이스라 스레드에서 호출된다(neo4j_graphrag의 Embedder가
    embed_query 하나만 요구하고, 탐지 검색은 그걸 스레드풀에서 부른다).
    """
    with _lock:
        semaphore = _thread_slots.get(model)
        if semaphore is None:
            semaphore = threading.Semaphore(limit_for(model))
            _thread_slots[model] = semaphore
        return semaphore
