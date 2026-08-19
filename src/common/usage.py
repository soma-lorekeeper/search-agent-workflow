"""OpenAI 응답의 토큰 사용량을 모으고 합산한다.

reasoning_tokens를 따로 잡는 게 이 모듈의 값어치다. 총 토큰만 보면 "출력은 줄었는데
비용은 그대로"인 상황의 원인을 못 찾는다 — 실제로 추출 프롬프트를 바꿨을 때 출력이
줄어든 만큼 추론이 늘어 비용이 그대로였던 적이 있다.

cached_tokens는 prompt_tokens에 **포함된** 값이다. 따로 더하면 이중 계산이 된다 —
분리해 두는 건 캐시 적중분에 다른 단가를 매기기 위해서다.
"""

from __future__ import annotations

from typing import Any

# 합산 결과의 키. 전부 정수이고 단순 덧셈으로 합쳐진다.
_FIELDS = ("calls", "prompt_tokens", "cached_tokens", "completion_tokens", "reasoning_tokens")


def empty() -> dict[str, int]:
    return dict.fromkeys(_FIELDS, 0)


def from_response(response: Any) -> dict[str, int]:
    """chat.completions 응답 하나의 사용량.

    cached_tokens는 prompt_tokens에 **포함된** 값이다(따로 더하면 이중 계산). 비용을 낼 때
    캐시 적중분에 다른 단가를 매기려고 분리해 둔다 — prompts.py가 prompt_cache_key로
    캐시를 명시적으로 노리고 있어서 이 값이 실제로 크다.
    """
    u = getattr(response, "usage", None)
    if u is None:
        return empty()
    prompt_details = getattr(u, "prompt_tokens_details", None)
    completion_details = getattr(u, "completion_tokens_details", None)
    return {
        "calls": 1,
        "prompt_tokens": u.prompt_tokens or 0,
        "cached_tokens": getattr(prompt_details, "cached_tokens", 0) or 0,
        "completion_tokens": u.completion_tokens or 0,
        "reasoning_tokens": getattr(completion_details, "reasoning_tokens", 0) or 0,
    }


def merge(usages: list[dict[str, int]]) -> dict[str, int]:
    total = empty()
    for u in usages:
        for key in _FIELDS:
            total[key] += u.get(key, 0)
    return total
