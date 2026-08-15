"""실제 OpenAI를 부르는 유일한 테스트. 기본 실행에서는 건너뛴다.

## 왜 실호출이 필요한가

rate limit 헤더는 **실제 응답에만** 실린다. 가짜로는 다음 셋을 알 수 없다:
  1. 헤더 6종이 정말 오는가 (모델·엔드포인트마다 다를 수 있다)
  2. `reset` 값의 형식이 우리 파서와 맞는가 ("6m0s"? "1s"? "500ms"?)
  3. **이 조직의 실제 한도가 얼마인가** — 세마포어 상한을 정하는 근거

3번이 핵심이다. `LLM_ASSUMED_TPM` 같은 값은 전부 추측이라, 한 번은 실측해서 대체해야 한다.

## 비용

프롬프트를 최소로 하고 모델 버킷당 1회씩만 부른다. 한 번 재고 나면 관측값을 가짜 응답
fixture로 남겨, 이후에는 재호출 없이 재현한다.

## 실행

    OPENAI_API_KEY=$(grep -m1 '^OPENAI_API_KEY=' .env | cut -d= -f2-) \
    LLM_SMOKE=1 .venv/bin/pytest tests/test_llm_smoke.py -q -s

**키를 앞에 붙여야 한다.** conftest.py가 import 시점에
`os.environ.setdefault("OPENAI_API_KEY", "sk-test-placeholder")`로 자리표시자를 심고,
`load_dotenv()`는 기본적으로 기존 환경변수를 덮어쓰지 않는다. 그래서 pytest 안에서는
.env의 진짜 키가 무시되고 401이 난다.

이건 고칠 버그가 아니라 **안전장치**다 — 어떤 테스트도 실수로 크레딧을 쓸 수 없다.
실측할 때만 위처럼 명시적으로 키를 넘긴다(setdefault라 미리 넣어두면 덮이지 않는다).

CI는 이 변수들을 주지 않으므로 외부 호출 0회가 유지된다(이 레포에는 pytest.ini가 의도적으로
없어서 커스텀 마커 대신 환경변수 가드를 쓴다).
"""

from __future__ import annotations

import asyncio
import os

import pytest

from src.common import graphrag, llm_limit
from src.common.openai_client import create_completion
from src.config import EMBEDDING_MODEL, EXTRACTION_MODEL, OPENAI_MODEL

pytestmark = pytest.mark.skipif(
    not os.environ.get("LLM_SMOKE"),
    reason="실 API 호출 — LLM_SMOKE=1 로 명시적으로 켤 때만 돈다",
)

_RATE_LIMIT_HEADERS = (
    "x-ratelimit-limit-tokens",
    "x-ratelimit-remaining-tokens",
    "x-ratelimit-reset-tokens",
    "x-ratelimit-limit-requests",
    "x-ratelimit-remaining-requests",
    "x-ratelimit-reset-requests",
)


def _보고(제목: str, bucket) -> None:
    """상한을 정할 때 쓸 수치를 사람이 읽게 찍는다(-s 로 실행)."""
    print(f"\n[{제목}]")
    print(f"  TPM  limit={bucket.tokens.limit!r}  remaining={bucket.tokens.remaining!r}")
    print(f"  RPM  limit={bucket.requests.limit!r}  remaining={bucket.requests.remaining!r}")


def test_LLM_응답에_rate_limit_헤더가_실린다(capsys):
    """헤더가 안 오면 미터는 영원히 가정값만 가리킨다 — 조용한 실패라 실측이 유일한 확인법이다."""
    원본 = {}

    async def 한_번():
        # 가장 싼 호출: 짧은 프롬프트 + 한 단어 응답 요청.
        return await create_completion(
            model=EXTRACTION_MODEL,
            messages=[
                {"role": "system", "content": "한 글자로만 답해라."},
                {"role": "user", "content": "1+1?"},
            ],
        )

    # 관문이 헤더를 미터에 넣는다. 원본 헤더도 함께 보려고 observe를 감싼다.
    진짜_observe = llm_limit.observe

    def _기록하며_observe(model, headers):
        원본.update({str(k).lower(): v for k, v in dict(headers).items()})
        진짜_observe(model, headers)

    llm_limit.observe = _기록하며_observe
    try:
        응답 = asyncio.run(한_번())
    finally:
        llm_limit.observe = 진짜_observe

    assert 응답.choices[0].message.content is not None

    with capsys.disabled():
        print("\n=== 실제 응답 헤더(rate limit 관련) ===")
        for name in _RATE_LIMIT_HEADERS:
            print(f"  {name}: {원본.get(name)!r}")
        _보고(f"LLM 버킷 — {EXTRACTION_MODEL}", llm_limit.snapshot()[EXTRACTION_MODEL])

    빠진_헤더 = [h for h in _RATE_LIMIT_HEADERS if h not in 원본]
    assert not 빠진_헤더, f"기대한 헤더가 없다: {빠진_헤더}"


def test_reset_헤더_형식이_파서와_맞는다(capsys):
    """"6m0s" 같은 기간 문자열이다. 형식이 다르면 복구 시각이 통째로 어긋나는데,
    예외가 아니라 "한도가 영원히 안 풀리는 것처럼 보임"으로 나타난다."""
    bucket = llm_limit.snapshot().get(EXTRACTION_MODEL)
    assert bucket is not None, "앞 테스트가 먼저 돌아야 한다"
    # observe가 reset을 읽었으면 reset_at이 채워져 있다.
    assert bucket.tokens.reset_at is not None or bucket.requests.reset_at is not None


def test_임베딩_응답에도_헤더가_실린다(capsys):
    """임베딩 버킷에는 헤더를 주는 다른 경로가 없다 — 여기서 못 읽으면 계량이 불가능하다."""
    벡터 = graphrag.MeteredEmbedder(model=EMBEDDING_MODEL).embed_query("계량 확인")
    assert len(벡터) > 0

    bucket = llm_limit.snapshot()[EMBEDDING_MODEL]
    with capsys.disabled():
        _보고(f"임베딩 버킷 — {EMBEDDING_MODEL}", bucket)

    assert bucket.tokens.limit is not None
    assert bucket.requests.limit is not None


def test_채팅_모델이_별도_버킷이면_그것도_잰다(capsys):
    """OPENAI_MODEL과 EXTRACTION_MODEL이 다르면 버킷이 하나 더 있다.
    같으면 호출을 아끼려고 건너뛴다."""
    if OPENAI_MODEL == EXTRACTION_MODEL:
        pytest.skip(f"채팅이 같은 모델({OPENAI_MODEL})이라 이미 측정됐다")

    async def 한_번():
        return await create_completion(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": "1+1? 한 글자로."}],
        )

    asyncio.run(한_번())
    with capsys.disabled():
        _보고(f"채팅 버킷 — {OPENAI_MODEL}", llm_limit.snapshot()[OPENAI_MODEL])
