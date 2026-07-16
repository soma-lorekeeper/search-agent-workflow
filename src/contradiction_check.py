"""신규 회차 원고를 기존 세계관 설정과 대조해 설정 오류를 탐지한다.

파이프라인: 신규 회차 원문 → (LLM) 검증할 claim 목록 추출 → claim마다 vector_search/
graph_query로 기존 설정 근거 검색 → (LLM) 모순 여부 판정 → 리포트 생성.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from openai import OpenAI

from src.config import OPENAI_API_KEY, OPENAI_MODEL, ROOT_DIR
from src.tools import graph_query, vector_search

_client = OpenAI(api_key=OPENAI_API_KEY)
logger = logging.getLogger("agent.contradiction")

MAX_CLAIMS = 20
REPORTS_DIR = ROOT_DIR / "reports"

CLAIM_EXTRACTION_PROMPT = """\
너는 웹소설 신규 원고에서, 기존 세계관 설정과 대조해 검증해야 할 "사실 주장(claim)"을
뽑아내는 어시스턴트다.

다음 신규 회차 원고를 읽고, 인물의 생사/소유물/능력/관계/시점·순서/장소 중 하나라도
전제하거나 서술하는 문장을 최대 {max_claims}개까지 뽑아라. 단순 묘사·감정 표현이나 완전히
새로운 사건 자체보다는, **이미 확립된 상태를 전제하거나 언급하는 서술**(예: "죽었던 A가
나타났다", "B가 여전히 그 물건을 갖고 있었다", "C의 배후성은 여전히 OO였다")을 우선하라.

각 claim마다 다음 필드를 채워라:
- quote: 원문에서 그대로 인용한 문장(또는 핵심 구절)
- entities: 이 claim과 관련된 핵심 고유명사(인물/아이템/장소 등) 목록
- category: 생사 | 소유물 | 능력 | 관계 | 시점/순서 | 장소 | 기타 중 하나

JSON 객체로만 답하라: {"claims": [{"quote": "...", "entities": ["..."], "category": "..."}, ...]}
"""

CLAIM_CHECK_PROMPT = """\
너는 신규 회차의 서술 한 문장이, 검색된 기존 설정 근거와 모순되는지 판단하는 어시스턴트다.

반드시 아래 제공된 검색 결과만 근거로 판단하고, 검색 결과에 없는 내용은 추측하지 마라.

핵심 원칙 (과거 실수를 통해 확정된 규칙이므로 반드시 지켜라):
- claim에는 핵심 사실(누가/무엇을/어떤 상태인지)과, 그 주변을 꾸미는 부가적 묘사(수식어,
  감정, 분위기, 세부 표현)가 섞여 있다. **핵심 사실만** 근거와 대조하라. 부가 묘사가
  근거에 토씨 하나 안 틀리고 등장하지 않는다고 해서 모순으로 판단하지 마라.
- "contradiction"은 근거와 claim이 **논리적으로 양립 불가능할 때만** 쓴다 (예: 근거는
  "A가 죽었다"고 하는데 claim은 "A가 살아있다"고 하는 경우, 근거는 "물건을 B가 가졌다"고
  하는데 claim은 "C가 가졌다"고 하는 경우). 단순히 근거에 claim의 특정 표현이 안 보인다는
  이유만으로 contradiction을 주지 마라 — 그런 경우는 "unknown"이다.
- 근거가 claim의 핵심 주장을 뒷받침하지도, 반박하지도 못한다면(즉 관련성이 약하거나
  다른 세부사항만 확인된다면) "unknown"으로 판단하라. "unknown"을 남발하는 것보다,
  확신 없이 "contradiction"을 주는 게 더 나쁘다 — 실제 저자에게 오탐(false positive)을
  리포트하면 신뢰를 잃는다.
- "consistent"는 근거가 claim의 핵심 사실을 지지하거나, 최소한 반박하지 않을 때 쓴다.

JSON 객체로만 답하라:
{"label": "contradiction|consistent|unknown",
 "established_fact": "판단 근거가 된 기존 설정 요약 한두 문장 (없으면 빈 문자열)",
 "source_episode": 근거가 된 화 번호(정수, 모르면 null),
 "explanation": "왜 그렇게 판단했는지 한두 문장"}
"""


def extract_claims(text: str, max_claims: int = MAX_CLAIMS) -> list[dict]:
    system_prompt = CLAIM_EXTRACTION_PROMPT.replace("{max_claims}", str(max_claims))
    response = _client.chat.completions.create(
        model=OPENAI_MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
    )
    data = json.loads(response.choices[0].message.content)
    claims = data.get("claims", [])
    return claims[:max_claims]


def check_claim(claim: dict) -> dict:
    quote = claim.get("quote", "")
    entities = claim.get("entities") or []
    entity_str = ", ".join(entities)

    search_query = f"{quote} (관련 대상: {entity_str})" if entities else quote
    vec_result = vector_search.invoke({"query": search_query, "top_k": 5})

    graph_question = (
        f'다음 서술과 관련해 확인된 기존 설정 사실을 모두 찾아줘: "{quote}"'
        + (f" (관련 대상: {entity_str})" if entities else "")
    )
    graph_result = graph_query.invoke({"question": graph_question})

    user_content = (
        f"[신규 회차 서술 (claim)]\n{quote}\n\n"
        f"[카테고리] {claim.get('category', '기타')}\n\n"
        f"[vector_search 결과]\n{vec_result}\n\n"
        f"[graph_query 결과]\n{graph_result}"
    )
    response = _client.chat.completions.create(
        model=OPENAI_MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": CLAIM_CHECK_PROMPT},
            {"role": "user", "content": user_content},
        ],
    )
    verdict = json.loads(response.choices[0].message.content)
    return {**claim, **verdict}


def check_new_episode(text: str) -> list[dict]:
    claims = extract_claims(text)
    logger.info("설정오류 검사 시작 | claim %d개 추출", len(claims))
    results = []
    for i, claim in enumerate(claims, 1):
        logger.info("[%d/%d] claim 검사 | %r", i, len(claims), claim.get("quote", "")[:50])
        result = check_claim(claim)
        logger.info("[%d/%d] 판정: %s", i, len(claims), result.get("label"))
        results.append(result)
    return results


def generate_report(results: list[dict], episode_label: str) -> str:
    contradictions = [r for r in results if r.get("label") == "contradiction"]
    unknowns = [r for r in results if r.get("label") == "unknown"]
    consistents = [r for r in results if r.get("label") == "consistent"]

    lines = [
        f"# 설정 오류 리포트 — {episode_label}",
        "",
        f"생성일시: {datetime.now().isoformat(timespec='seconds')}",
        f"검사한 서술 {len(results)}건 — 모순 {len(contradictions)}건 / "
        f"일치 {len(consistents)}건 / 확인불가 {len(unknowns)}건",
        "",
    ]

    if contradictions:
        lines.append(f"## ⚠️ 설정 오류 발견 ({len(contradictions)}건)")
        lines.append("")
        for i, r in enumerate(contradictions, 1):
            src = r.get("source_episode")
            src_str = f"{src}화" if src else "출처 불명"
            lines += [
                f"### {i}. [{r.get('category', '기타')}]",
                f"- **신규 회차 서술**: {r.get('quote', '')}",
                f"- **기존 설정 (출처: {src_str})**: {r.get('established_fact', '')}",
                f"- **설명**: {r.get('explanation', '')}",
                "",
            ]
    else:
        lines += ["## 설정 오류 없음", ""]

    if unknowns:
        lines.append(f"## 확인 불가 ({len(unknowns)}건, 참고용 — 근거 부족)")
        lines.append("")
        for r in unknowns:
            lines.append(f"- [{r.get('category', '기타')}] {r.get('quote', '')}")
        lines.append("")

    lines.append(f"## 문제 없음으로 확인됨 ({len(consistents)}건)")
    lines.append("")
    for r in consistents:
        src = r.get("source_episode")
        src_str = f"{src}화" if src else "-"
        lines.append(f"- [{r.get('category', '기타')}] {r.get('quote', '')} (근거: {src_str})")

    return "\n".join(lines)


def run_check(path: Path, episode_label: str | None = None) -> Path:
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    label = episode_label or path.stem

    results = check_new_episode(text)
    report_md = generate_report(results, label)

    REPORTS_DIR.mkdir(exist_ok=True)
    out_path = REPORTS_DIR / f"{label}_contradiction_report.md"
    out_path.write_text(report_md, encoding="utf-8")
    json_path = REPORTS_DIR / f"{label}_contradiction_report.json"
    json_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    return out_path


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("사용법: python -m src.contradiction_check <신규 회차 텍스트 파일> [라벨]")
        sys.exit(1)
    _path = Path(sys.argv[1])
    _label = sys.argv[2] if len(sys.argv) > 2 else None
    _out = run_check(_path, _label)
    print(f"리포트 생성 완료: {_out}")
