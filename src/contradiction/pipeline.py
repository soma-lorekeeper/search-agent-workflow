"""신규 회차 원고를 기존 세계관 설정과 대조해 설정 오류를 탐지하는 파이프라인.

고정 파이프라인(0/1/3/4단계) + claim별 에이전틱 fork(2단계)로 구성된다. 구조 다이어그램은
docs/architecture/contradiction-pipeline-architecture.html 참고.

  0. 배경 컨텍스트 준비 — 그래프 덤프 + 전역 요약(lorekeeper 재사용) + 전체 회차 요약(자체
     쿼리, 아래 참고). LLM 0회. 검사 대상 회차 번호를 알면 그 직전 회차까지로 상한을 건다
     (background_context 참고) — 대조 기준이 "기존 설정"이려면 대상 회차 자신과 그 뒤는 빠져야 한다.
  1. Claim 추출 — 원고를 청크로 나눠 청크마다 1회 LLM 호출(병렬), 도구 없음.
  2. claim별 검증 에이전트 — fork, 병렬, claim마다 최대 4 tool call (agent.verify_claim).
  3. 결과 집계 — 코드, LLM 불필요.
  4. 리포트 생성 — 마크다운, LLM 불필요.

0단계는 lorekeeper.context.load_summaries()를 쓰지 않는다 — 그건 인덱싱용으로 최근
_RECENT_WINDOW(=3)화 요약만 담는데(누적 프롬프트가 계속 커지지 않게 하려는 인덱싱 쪽
설계), 설정오류 검사는 오래된 화의 설정도 다 검증 대상이라 요구사항이 다르다. 대신
전역 요약은 그대로 재사용하고, 회차별 요약은 윈도우 없이 우리가 직접 쿼리한다
(lorekeeper-poc 코드 자체는 건드리지 않는다).
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable

from src.repository.neo4j.client import get_driver
from src.common.tenant import Tenant
from src.service.index.context_service import build_context, dump_graph_text, load_summaries
from src.service.index.indexing_service import DATABASE as LOREKEEPER_DATABASE
from src.service.index.splitters import KSSSentenceSplitter
from openai import AsyncOpenAI

from src.config import OPENAI_API_KEY, OPENAI_MODEL, ROOT_DIR
from src.contradiction.agent import verify_claim
from src.contradiction.prompts import CLAIM_EXTRACTION_PROMPT, EXTRACTION_CACHE_KEY
from src.contradiction.tools import build_openai_tools

logger = logging.getLogger("contradiction.pipeline")

EXTRACTION_CHUNK_SIZE = 2500  # claim 추출용 청크 목표 글자 수
TARGET_CLAIMS_PER_CHUNK = 4  # claim 밀도 목표(청크당 claim 개수) — 상한이 아니라 가이드라인.
# 실제 5화 분량(28,355자) 기준 11청크로 나뉘므로 4개/청크 ≈ 전체 44개(≈40개대) 노림.
MAX_CLAIMS_PER_CHUNK = 5  # 폭주 방지용 청크당 소프트 상한(목표+1로 좁혀 TPM 폭주를 억제)
VERIFY_CONCURRENCY = 5  # claim 검증 동시 실행 수 상한 — 조직 TPM(분당 토큰) 한도 대비 안전판
REPORTS_DIR = ROOT_DIR / "reports"

_client = AsyncOpenAI(api_key=OPENAI_API_KEY)


def _all_chapter_summaries(
    driver, database: str, up_to_chapter: int | None, tenant: Tenant
) -> str:
    """회차 요약을 회차 오름차순으로 이어붙인다(윈도우 없음).

    lorekeeper.context.load_summaries()의 "최근 회차" 부분과 같은 모양의 쿼리이되
    LIMIT이 없다 — 설정오류 검사는 몇 화 전이든 검증 대상이 될 수 있어서다.

    up_to_chapter가 있으면 그 앞 회차까지만 싣는다(c.number < up_to_chapter). 검사 대상 회차
    자신과 그 뒤 회차는 "기존 설정"이 아니다.
    """
    records, _, _ = driver.execute_query(
        """
        MATCH (c:Chapter)
        WHERE c.summary IS NOT NULL
          AND c.tenant_id = $tenant_id
          AND ($up_to_chapter IS NULL OR c.number < $up_to_chapter)
        RETURN c.number AS number, c.summary AS summary
        ORDER BY c.number ASC
        """,
        {"up_to_chapter": up_to_chapter, **tenant.params()},
        database_=database,
    )
    return "\n".join(f"[{r['number']}화] {r['summary']}" for r in records)


# ---------- 그래프 덤프의 회차 상한 ----------
# 검사 대상이 N화면 배경은 N화 **이전**의 설정이어야 한다. 그런데 lorekeeper의
# dump_graph_text()는 그래프 전체를 덤프하고 회차 인자가 없다(그리고 lorekeeper-poc는 우리가
# 고치지 않는다). 그래서 드라이버를 얇게 감싸, 덤프가 던지는 쿼리의 **결과에서** N화 이후의
# 노드·관계를 걸러낸 뒤 돌려준다. 렌더링 로직(200줄)을 복제하지 않고 상한을 거는 방법이다.
#
# 회차를 판정할 수 있는 노드만 걸러진다:
#   - Event: chapter 속성을 직접 갖는다(3.0, 3.1 … 처럼 float).
#   - CharacterState: 속성이 없어 ESTABLISHED_IN→Event.chapter, 없으면 EVIDENCED_BY→Chunk.chapter.
# Character/Item/Location/Organization은 어떤 회차 표시도 없다(회차마다 같은 노드로 MERGE되는
# 정준 엔티티라 "언제 생겼는지"가 그래프에 남지 않는다) — 그래서 N화 이후에 처음 등장한 인물·
# 사물·장소와 그들 사이의 관계는 걸러지지 않고 배경에 남는다. 이건 lorekeeper-poc를 고치지
# 않고는 좁힐 수 없는 한계다.
_FUTURE_NODES_CYPHER = """
MATCH (n)
WHERE NOT n:Chunk AND NOT n:Chapter AND NOT n:Story
  AND n.tenant_id = $tenant_id
OPTIONAL MATCH (n)-[:ESTABLISHED_IN]->(ev:Event)
OPTIONAL MATCH (n)-[:EVIDENCED_BY]->(ck:Chunk)
WITH n, coalesce(n.chapter, min(ev.chapter), min(ck.chapter)) AS chapter
WHERE chapter IS NOT NULL AND chapter >= $up_to_chapter
RETURN elementId(n) AS id
"""


class _ChapterBoundedDriver:
    """dump_graph_text()에 넘길 드라이버 대역. 쿼리는 진짜 드라이버로 그대로 보내고, 돌아온
    레코드에서 up_to_chapter 이후의 노드(와 그 노드가 걸린 관계)만 덜어낸다.

    어떤 쿼리인지는 결과 컬럼으로 알아본다(쿼리 문자열을 맞춰보면 lorekeeper 쪽 공백 하나에도
    깨진다). dump_graph_text가 던지는 세 쿼리의 컬럼은 각각 이렇게 다르다:
      노드 {id, labels, props} / 관계 {s, t, e, props} / 상태-회차 {id, chapter}
    """

    def __init__(self, driver, database: str, tenant: Tenant, up_to_chapter: int):
        self._driver = driver
        records, _, _ = driver.execute_query(
            _FUTURE_NODES_CYPHER,
            {"up_to_chapter": up_to_chapter, **tenant.params()},
            database_=database,
        )
        self._future_ids = {r["id"] for r in records}

    def execute_query(self, query, parameters_=None, **kwargs):
        records, summary, keys = self._driver.execute_query(query, parameters_, **kwargs)
        if not records:
            return records, summary, keys
        columns = set(records[0].keys())
        if {"id", "labels", "props"} <= columns:
            records = [r for r in records if r["id"] not in self._future_ids]
        elif {"s", "t", "e"} <= columns:
            # 양끝 중 하나라도 걸러낸 노드면 관계도 뺀다. 남겨두면 렌더러가 없는 노드를
            # 찾다가 KeyError로 죽는다.
            records = [
                r
                for r in records
                if r["s"] not in self._future_ids and r["e"] not in self._future_ids
            ]
        elif {"id", "chapter"} <= columns:
            records = [r for r in records if r["id"] not in self._future_ids]
        return records, summary, keys


def background_context(tenant: Tenant, up_to_chapter: int | None = None) -> str:
    """0단계: 그래프 덤프 + 전역 요약(lorekeeper 재사용) + 전체 회차 요약(자체 쿼리).

    up_to_chapter=N이면 "N화 직전까지의 세계관"을 만든다. 이 인자가 검사의 의미를 좌우한다 —
    없으면 N화를 N화 자신이 만든 사실과 대조해 "일치"라고 자평하고, 아직 나오지 않은 N+1화
    이후의 반전을 N화에 이미 심어둔 모순으로 읽는다.

    상한을 걸 때 전역 요약(Story.summary)은 **뺀다.** 그건 인덱싱된 전 회차를 하나의 문자열로
    압축해 둔 것이라 뒷 회차만 덜어낼 방법이 없다(회차별 출처가 남아 있지 않다). 대신 회차별
    요약을 창 없이 전부 싣고 있어서, 압축본이 빠져도 N화 이전 줄거리는 오히려 더 자세히 들어간다.
    """
    driver = get_driver()
    try:
        if up_to_chapter is None:
            graph_dump = dump_graph_text(driver, LOREKEEPER_DATABASE, tenant)
            global_summary, _recent_unused = load_summaries(driver, LOREKEEPER_DATABASE, tenant)
        else:
            bounded = _ChapterBoundedDriver(
                driver, LOREKEEPER_DATABASE, tenant, up_to_chapter
            )
            graph_dump = dump_graph_text(bounded, LOREKEEPER_DATABASE, tenant)
            global_summary = ""
        all_summaries = _all_chapter_summaries(
            driver, LOREKEEPER_DATABASE, up_to_chapter, tenant
        )
    finally:
        driver.close()
    return build_context(graph_dump, global_summary, all_summaries)


async def _extract_claims_from_chunk(
    chunk_text: str, full_draft: str, context: str
) -> list[dict]:
    """청크 하나에서 claim을 뽑는다(LLM 호출 1회, 여러 claim을 한꺼번에 반환)."""
    system_prompt = (
        CLAIM_EXTRACTION_PROMPT.replace("{background_context}", context)
        .replace("{full_draft}", full_draft)
        .replace("{target_claims_per_chunk}", str(TARGET_CLAIMS_PER_CHUNK))
        .replace("{max_claims_per_chunk}", str(MAX_CLAIMS_PER_CHUNK))
    )
    response = await _client.chat.completions.create(
        model=OPENAI_MODEL,
        response_format={"type": "json_object"},
        prompt_cache_key=EXTRACTION_CACHE_KEY,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": chunk_text},
        ],
    )
    data = json.loads(response.choices[0].message.content)
    claims = data.get("claims", [])
    return claims[:MAX_CLAIMS_PER_CHUNK]


async def extract_claims(text: str, background_context: str) -> list[dict]:
    """1단계: 원고를 청크로 나눠 청크마다 병렬로 claim을 뽑고 하나로 합친다.

    청크 분할은 lorekeeper.splitters.KSSSentenceSplitter를 그대로 재사용한다(문장 경계를
    안전하게 다루도록 이미 검증된 로직 — fix.md의 오버사이즈 문장 버그 수정이 반영돼 있다).
    각 청크 호출에는 원고 전체(full_draft)도 함께 실어, 청크 밖 맥락(대명사·앞부분에서만
    확립된 설정)을 참고할 수 있게 한다.
    """
    raw = await KSSSentenceSplitter(chunk_size=EXTRACTION_CHUNK_SIZE, chunk_overlap=0).run(text)
    chunk_results = await asyncio.gather(
        *[
            _extract_claims_from_chunk(chunk.text, text, background_context)
            for chunk in raw.chunks
        ]
    )
    return [claim for claims in chunk_results for claim in claims]


async def check_new_episode(
    text: str, tenant: Tenant, up_to_chapter: int | None = None
) -> list[dict]:
    """0~2단계를 이어서 실행한다: 컨텍스트 준비 → claim 추출 → claim별 병렬 검증(fork)."""
    return await check_new_episode_streaming(text, tenant, up_to_chapter)


async def check_new_episode_streaming(
    text: str,
    tenant: Tenant,
    up_to_chapter: int | None = None,
    on_claims_extracted: Callable[[list[dict]], None] | None = None,
    on_claim_done: Callable[[int, dict], None] | None = None,
) -> list[dict]:
    """check_new_episode와 동일한 파이프라인이되, 중간 진행 상황을 콜백으로 알려준다.

    up_to_chapter: 검사 대상 회차 번호. 이 회차 **직전**까지의 그래프·요약만 배경으로 삼는다
    (background_context 참고). None이면 그래프 전체를 배경으로 쓴다 — CLI로 회차 번호 없이
    원고 파일 하나만 검사할 때의 동작이다.
    on_claims_extracted(claims): 1단계 직후, claim 목록이 정해진 시점에 한 번 호출된다.
    on_claim_done(index, result): claim 하나(=claims[index])의 검증이 끝날 때마다 호출된다.
    claim들은 asyncio.gather로 병렬 실행되므로 완료 순서는 claims 순서와 다를 수 있다 —
    그래서 index로 어떤 claim이 끝났는지 알려준다. (웹앱의 실시간 진행 UI에 쓰인다.)
    """
    # Neo4j 왕복 세 번(그래프 덤프 포함)이라 그래프가 커질수록 길어진다. 이벤트 루프에서 직접
    # 부르면 그동안 인덱싱 워커·채팅·헬스체크가 전부 멈춘다.
    context = await asyncio.to_thread(background_context, tenant, up_to_chapter)
    claims = await extract_claims(text, context)
    logger.info("설정오류 검사 시작 | claim %d개 추출", len(claims))
    if on_claims_extracted:
        on_claims_extracted(claims)
    if not claims:
        return []

    tool_schemas, tools_by_name = build_openai_tools(tenant)
    semaphore = asyncio.Semaphore(VERIFY_CONCURRENCY)

    async def _verify(index: int, claim: dict) -> dict:
        async with semaphore:
            result = await verify_claim(claim, context, tool_schemas, tools_by_name)
        if on_claim_done:
            on_claim_done(index, result)
        return result

    results = await asyncio.gather(*[_verify(i, claim) for i, claim in enumerate(claims)])
    return list(results)


def generate_report(
    results: list[dict], episode_label: str, generated_at: str | None = None
) -> str:
    """3~4단계: 판정 결과를 그룹핑해 마크다운 리포트로 렌더한다."""
    generated_at = generated_at or datetime.now().isoformat(timespec="seconds")
    contradictions = [r for r in results if r.get("label") == "contradiction"]
    unknowns = [r for r in results if r.get("label") == "unknown"]
    consistents = [r for r in results if r.get("label") == "consistent"]

    lines = [
        f"# 설정 오류 리포트 — {episode_label}",
        "",
        f"생성일시: {generated_at}",
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


def save_report_files(results: list[dict], label: str, display_label: str | None = None) -> dict:
    """리포트를 reports/{label}_contradiction_report.{md,json}으로 저장한다.

    이 파일들은 디버깅용 흔적일 뿐 더 이상 진실의 원천이 아니다 — 웹앱에서 넘어오는 검사
    결과의 정답은 API 서버(Spring)가 PostgreSQL에 저장한 쪽이고, 여기 파일은 "그때 모델이
    실제로 뭐라고 판정했는지"를 서버에 들어가서 눈으로 확인할 때만 쓴다. 그래서 파일 키(label)는
    사람이 정한 이름이 아니라 작업 id(job_id)여야 한다: 그래야 Spring의 작업 기록과 파일이
    1:1로 대응돼 "이 검사 결과의 원본"을 찾아갈 수 있다.

    display_label은 md 리포트 제목에만 쓰는 사람용 이름이다. 파일명이 job_id면 제목까지
    job_id가 되어 열어봐도 몇 화 검사였는지 알 수 없어서 분리했다.
    """
    REPORTS_DIR.mkdir(exist_ok=True)
    generated_at = datetime.now().isoformat(timespec="seconds")
    report_md = generate_report(results, display_label or label, generated_at)
    (REPORTS_DIR / f"{label}_contradiction_report.md").write_text(report_md, encoding="utf-8")
    payload = {"label": label, "generated_at": generated_at, "results": results}
    (REPORTS_DIR / f"{label}_contradiction_report.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return payload


async def run_check(path: Path, episode_label: str | None = None) -> Path:
    """전체 파이프라인을 실행하고 리포트 파일(md+json)을 reports/에 저장한다."""
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    label = episode_label or path.stem

    results = await check_new_episode(text)
    save_report_files(results, label)
    return REPORTS_DIR / f"{label}_contradiction_report.md"


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    if len(sys.argv) < 2:
        print("사용법: python -m src.contradiction.pipeline <신규 회차 텍스트 파일> [라벨]")
        sys.exit(1)
    _path = Path(sys.argv[1])
    _label = sys.argv[2] if len(sys.argv) > 2 else None
    _out = asyncio.run(run_check(_path, _label))
    print(f"리포트 생성 완료: {_out}")
