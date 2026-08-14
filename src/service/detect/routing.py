"""claim 하나를 어떤 검색 질의로 바꿀지 정한다.

**모델이 도구를 고르지 않는다.** 예전 구조는 claim마다 LLM이 도구를 골라 부르게 했는데,
같은 입력에도 선택이 흔들려 프롬프트 개선의 효과와 선택의 요동을 구분할 수 없었다.
여기서는 코드가 규칙으로 정한다 — 같은 claim이면 항상 같은 질의가 나간다.

전부 평가 하네스(scripts/eval_claims.py)에서 확정한 구현 그대로다.
"""

from __future__ import annotations


def route_qav(claim: dict, with_quote: bool = False) -> list[tuple[str, dict]]:
    """qav 라우팅 — `axis`와 `axis: value` 두 계열을 hybrid·fact 두 채널에 보낸다.

    entity_search를 열지 않는다. 이름 조회는 인물의 사실 이력을 LIMIT 60까지 통째로 긁어와
    fact_search 결과와 대부분 겹쳤고, 겹치지 않는 꼬리는 청크 29개·사실 18개뿐이었다.
    그 대신 사실에 붙은 소속·소유·무대는 fact_search의 `[연관]` 줄이 직접 실어 온다.

    quote 계열을 기본에서 뺀 것은 실측 결과다 — quote×2+`axis:값`×2와 axis×2+`axis:값`×2가
    도달 23/24로 동률이었고 문서고 고유 노드도 274 vs 272로 같았다. 다만 그 측정은 axis를
    사람이 고른 것이라 "추출기가 axis를 빗맞혔을 때"의 위험이 0이었다. with_quote는 그
    안전망을 켜는 스위치다.

    두 계열이 상보적인 이유: axis는 오류 토큰이 없어 값이 틀린 claim에서도 대조축을 정확히
    가리키고, `axis: value`는 값 어휘 자체가 단서인 claim을 잡는다.
    """
    axis = (claim.get("axis") or "").strip()
    value = str(claim.get("value") or "").strip()
    quote = (claim.get("quote") or "").strip()

    calls: list[tuple[str, dict]] = []
    if with_quote and quote:
        calls.append(("hybrid_search", {"query_text": quote}))
        calls.append(("fact_search", {"query_text": quote}))
    if axis:
        calls.append(("hybrid_search", {"query_text": axis}))
        calls.append(("fact_search", {"query_text": axis}))
        if value:
            av = f"{axis}: {value}"
            calls.append(("hybrid_search", {"query_text": av}))
            calls.append(("fact_search", {"query_text": av}))
    elif quote and not with_quote:  # axis가 비면 quote라도 던져야 근거가 0이 되지 않는다
        calls.append(("hybrid_search", {"query_text": quote}))
        calls.append(("fact_search", {"query_text": quote}))
    return calls
