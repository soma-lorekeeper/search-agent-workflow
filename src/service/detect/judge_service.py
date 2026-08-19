"""3단계 — 문서고와 claim을 대조해 설정 오류를 판정한다.

claim마다 따로 부르지 않고 **한 번에 배치로** 판정한다. 문서고가 한 번만 실리고, 판정기가
claim들 사이의 관계까지 보면서 판단할 수 있다.

판정기는 원고를 보지 않는다 — quote와 axis/value, 그리고 문서고뿐이다. 원고를 함께 넣는
실험은 검출이 21에서 16으로 떨어졌다(근거인 문서고 대신 검증 대상인 원고를 근거로 삼기
시작했다).

전부 평가 하네스(scripts/eval_claims.py)에서 확정한 구현 그대로다.
"""

from __future__ import annotations

import json
import logging
import re

from src.common.openai_client import create_completion
from src.config import EXTRACTION_MODEL
from src.service.detect import prompts
from src.service.detect.docstore import build_docstore, render_claim_refs, render_docstore

logger = logging.getLogger("detect.judge")

# 오류로 확정하는 점수 문턱. 곡선이 2~8에서 평평해(검출 22, 오탐 0) 가운데를 잡았다.
ERROR_THRESHOLD = 7

# 반복 전달되는 프리픽스(기준+예시+문서고)의 프롬프트 캐시 키.
_CACHE_KEY = "detect-judge"

# 판정 응답의 claim 식별자. 추출이 붙인 P1~PN과 같은 모양이어야 한다.
_VERDICT_ID = re.compile(r"^P(\d+)$")


def build_system(store: dict) -> str:
    """판정용 system 프롬프트. 정적(기준+예시)을 앞, 문서고를 뒤에 둔다.

    이 문자열은 회차 안에서 안 바뀌므로 프롬프트 캐시의 안정 prefix가 된다.
    """
    return "\n\n".join(
        [prompts.JUDGE_CRITERIA, prompts.JUDGE_FEWSHOT, "[문서고]\n" + render_docstore(store)]
    )


def parse_verdicts(
    raw: str, wanted: list[int], claims: list[dict] | None = None
) -> tuple[dict[int, dict], list[str]]:
    """판정 JSON을 claim 인덱스 → 판정 dict로 푼다. 위반은 버리지 않고 flag로 남긴다.

    누락된 claim을 0점으로 채우지 않는 게 중요하다 — 0은 "근거가 없어 낮은 점수"라는 유효한
    판정값이라, 누락을 0으로 메우면 둘을 영영 구분할 수 없다.

    claimId(`P12`)를 우선 읽고, 없으면 옛 형식(`{"i": 12}`)의 1-base 번호로 폴백한다.
    lineIds는 그 claim이 제시한 후보 줄과 대조해 어긋나면 후보 전체로 되돌린다.
    """
    problems: list[str] = []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}, ["json_parse_failed"]

    out: dict[int, dict] = {}
    for v in data.get("verdicts") or []:
        flags = []
        cid = v.get("claimId")
        m = _VERDICT_ID.match(cid) if isinstance(cid, str) else None
        if m is not None:
            idx = int(m.group(1)) - 1  # P12 → 인덱스 11
        else:
            try:
                idx = int(v.get("i")) - 1  # 프롬프트는 1-base, 내부는 0-base
            except (TypeError, ValueError):
                problems.append("bad_index")
                continue
            if cid is not None:
                flags.append("claim_id_unknown")
        raw_score = v.get("score")
        try:
            score = int(raw_score)
        except (TypeError, ValueError):
            score, flags = 0, flags + ["score_not_int"]
        if score < 0 or score > 10:
            flags.append("score_out_of_range")
            score = max(0, min(10, score))
        cited = [str(c) for c in (v.get("cited") or [])]

        # lineIds는 그 claim이 제시한 후보 줄의 부분집합이어야 한다. 아니면 후보 전체로
        # 되돌린다 — 판정기는 원고를 못 보므로 없는 번호를 지어낼 여지가 있다.
        claim = claims[idx] if claims and 0 <= idx < len(claims) else None
        cand = list((claim or {}).get("lines") or [])
        picked = [n for n in (v.get("lineIds") or []) if isinstance(n, int)]
        if cand and (not picked or not set(picked) <= set(cand)):
            flags.append("line_fallback")
            picked = cand

        out[idx] = {"claim_index": idx, "score": score, "raw_score": raw_score,
                    "claim_id": (claim or {}).get("id"), "line_ids": picked,
                    "cited": cited, "reason": str(v.get("reason") or ""), "flags": flags}

    missing = [i for i in wanted if i not in out]
    if missing:
        problems.append(f"missing:{len(missing)}")
    extra = [i for i in out if i not in set(wanted)]
    if extra:
        problems.append(f"extra:{len(extra)}")
    return out, problems


def _resolve_cited(aliases: list[str], store: dict) -> list[dict]:
    """문서고 별칭(C001/F003)을 (회차, 조각 번호) 자연키 + 근거 원문으로 바꾼다.

    사실(F###)은 그 사실의 근거 청크로 펼친다 — 화면이 하이라이트할 수 있는 것은 원문
    조각이지 사실 노드가 아니다. 실측상 모든 사실이 근거 청크를 최소 하나 갖고 있어
    이 해소는 비어서 돌아오지 않는다.

    좌표만이 아니라 **원문(text)도 함께 싣는다.** chunkIndex는 이 서버의 KSS 청킹이 매긴
    번호라, 받는 쪽(Spring)은 그 분할을 재현할 수 없어 몇 번 조각이 어느 문장인지 알
    방법이 없다. 원문을 붙이면 그 조각을 그대로 보여줄 수 있고, 회차가 나중에 수정돼도
    판정 당시의 근거가 그대로 남는다.

    문서고에 없는 별칭은 버린다. 판정기가 지어낸 인용을 그대로 내보내면 화면이 없는
    자리를 가리키게 된다.
    """
    chunks = store.get("chunks") or {}
    facts = store.get("facts") or {}

    by_alias: dict[str, list[tuple]] = {}
    for c in chunks.values():
        by_alias[c["alias"]] = [(c.get("chapter"), c.get("index"), c.get("text"))]
    for f in facts.values():
        # 사실의 evidence는 **청크 eid 목록**이다 — build_docstore가 근거 원문을 청크
        # 사전으로 옮기고 키만 남기기 때문이다. 그 키로 청크를 되짚어야 좌표가 나온다.
        by_alias[f["alias"]] = [
            (chunks[k].get("chapter"), chunks[k].get("index"), chunks[k].get("text"))
            for k in (f.get("evidence") or [])
            if k in chunks
        ]

    out: list[dict] = []
    seen: set[tuple] = set()
    for alias in aliases:
        for chapter, index, text in by_alias.get(alias, []):
            if chapter is None or index is None:
                continue
            # 중복 판정은 좌표로만 한다. 같은 조각을 판정기가 C001로도, 그 조각을 근거로
            # 둔 F003으로도 인용하면 여기로 두 번 들어오는데, 화면에는 한 번만 보여야 한다.
            key = (chapter, index)
            if key in seen:
                continue
            seen.add(key)
            out.append({"episodeNo": chapter, "chunkIndex": index, "text": text or ""})
    return out


async def judge(claims: list[dict], evidence: dict, lines: list[str]) -> list[dict]:
    """claim들을 문서고와 대조해 **오류로 판정된 것만** 돌려준다.

    한 번의 호출로 전부 판정한다 — 문서고가 한 번만 실리고, 판정기가 claim들 사이의
    관계까지 보면서 판단할 수 있다.

    lines는 검사 중인 회차의 원고 줄 목록(extract가 돌려준 그것)이다. 판정이 고른 줄
    **번호**를 원문으로 되짚는 데만 쓴다 — 판정 자체에는 관여하지 않는다.
    """
    if not claims:
        return []

    store = build_docstore(evidence)
    system = build_system(store)
    user = render_claim_refs(store, claims)

    response = await create_completion(
        model=EXTRACTION_MODEL,
        response_format={"type": "json_object"},
        prompt_cache_key=_CACHE_KEY,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    raw = response.choices[0].message.content or "{}"
    verdicts, problems = parse_verdicts(raw, list(range(len(claims))), claims)
    if problems:
        # 폴백이 얼마나 도는지가 다음 개선의 신호다. 응답에는 싣지 않는다.
        logger.warning("판정 파싱 문제 | %s", problems)

    # 응답이 통째로 안 읽히면 "오류 0건"이 아니라 실패다.
    #
    # 판정 결과가 비면 아래 루프가 한 번도 안 돌아 findings=[]가 되고, 검사는 status=DONE
    # 오류 0건으로 끝난다. 작가는 검사가 성공했다고 믿고, LLM 비용은 전액 지불된 뒤다.
    # parse_verdicts의 docstring이 "누락을 0점으로 메우면 안 된다"고 짚은 구분을 여기서
    # 실제로 쓰는 자리다 — 0점은 "근거가 없다"는 판정이고, 빈 결과는 "판정을 못 읽었다"다.
    if not verdicts:
        raise RuntimeError(f"판정 응답을 읽지 못했다({', '.join(problems) or '결과 없음'}).")

    findings: list[dict] = []
    for idx in sorted(verdicts):
        v = verdicts[idx]
        if v["score"] < ERROR_THRESHOLD:
            continue
        if not 0 <= idx < len(claims):
            # 판정기가 없는 claim 번호를 지어낸 경우(P99 같은). 파서는 이를 extra로
            # 보고하지만 그대로 두면 여기서 IndexError가 나 판정 전체가 죽는다 —
            # 한 건의 환각이 나머지 정상 판정까지 버리게 할 이유가 없다.
            logger.warning("범위 밖 claim 번호를 무시한다 | idx=%d claims=%d", idx, len(claims))
            continue
        claim = claims[idx]
        if v["flags"]:
            logger.info("판정 폴백 | claim=%s flags=%s", v.get("claim_id"), v["flags"])
        findings.append(
            {
                "claimId": v.get("claim_id") or f"P{idx + 1}",
                "quote": claim.get("quote") or "",
                "axis": claim.get("axis") or "",
                "value": str(claim.get("value") or ""),
                # 줄 번호를 원문과 함께 싣는다. 번호만 보내면 받는 쪽이 이 서버의 줄
                # 분할(lines.split_lines)을 재현할 수 없어 91번 줄이 무엇인지 모른다.
                # 번호는 number_lines가 1부터 매기므로 목록 인덱스로는 -1 한다.
                # 범위 밖 번호는 버린다 — 문서고에 없는 별칭을 버리는 것과 같은 이유로,
                # 지어낸 좌표를 내보내면 화면이 없는 자리를 가리킨다.
                "lines": [
                    {"lineNo": n, "text": lines[n - 1]}
                    for n in v["line_ids"]
                    if 1 <= n <= len(lines)
                ],
                "isError": True,
                "score": v["score"],
                "reason": v["reason"],
                "cited": _resolve_cited(v["cited"], store),
            }
        )
    return findings
