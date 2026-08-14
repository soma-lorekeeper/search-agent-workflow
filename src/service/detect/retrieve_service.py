"""2단계 — claim마다 그래프에서 대조할 근거를 모은다.

라우팅은 코드가 한다(routing.route_qav). claim 하나가 hybrid·fact 두 채널에 axis 계열과
`axis: value` 계열을 던져 최대 네 번 검색한다.

**회차 상한이 검사의 의미를 좌우한다.** 5화를 검사하면서 5화 자신이 만든 사실을 근거로
쓰면 "일치"라고 자평하게 되고, 6화 이후의 반전을 5화가 심어둔 모순으로 읽는다. 그래서
검색 결과에서 검사 회차 이상의 것을 걸러낸다.

전부 평가 하네스(scripts/eval_claims.py)에서 확정한 구현 그대로다.
"""

from __future__ import annotations

import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor

from src.common.tenant import Tenant
from src.repository.neo4j.retrieval import build_retrieval_tools
from src.service.detect.routing import route_qav

logger = logging.getLogger("detect.retrieve")

# 채널당 가져올 결과 수. 하네스가 3으로 확정했다.
TOP_K = 3
# claim들을 동시에 조회할 스레드 수. retriever가 동기 API라 스레드로 병렬화한다.
_WORKERS = 8

_WS = re.compile(r"\s+")


def _norm(text: str) -> str:
    """dedupe 키. 공백 차이만으로 다른 결과로 세지 않게 한다."""
    return _WS.sub("", text or "")


def _chapter_of(md: dict) -> int | None:
    """검색 결과 항목이 어느 회차의 것인지. 청크는 chapter, 사실은 성립 회차다."""
    ch = md.get("chapter")
    return ch if isinstance(ch, int) else None


def _is_future(md: dict, up_to_chapter: int | None) -> bool:
    """검사 회차 이상의 근거인가.

    회차를 모르는 항목(chapter가 없는 노드)은 **버리지 않는다.** 정준 엔티티처럼 회차
    표시가 없는 것들이 있어서, 모른다고 버리면 배경이 통째로 비어버린다.
    """
    if up_to_chapter is None:
        return False
    ch = _chapter_of(md)
    return ch is not None and ch >= up_to_chapter


def _retrieve_one(claim: dict, tools: dict, up_to_chapter: int | None) -> dict:
    """claim 하나의 전 채널. 스레드에서 돌므로 바깥 상태를 건드리지 않는다."""
    channels: list[dict] = []
    # 채널들이 같은 청크를 반복해 물어오므로 claim 단위로 dedupe한다.
    seen: set[str] = set()

    for tool_name, args in route_qav(claim):
        args = {**args, "top_k": TOP_K}
        items: list[dict] = []
        try:
            result = tools[tool_name].execute(**args)
            fresh = [
                item for item in result.items if not _is_future(item.metadata or {}, up_to_chapter)
            ]
            # items는 dedupe **이전**의 전량을 담는다 — dedupe는 "이 claim의 앞 채널과
            # 겹친다"는 이유로 텍스트를 빼는 것이고, claim이 그 노드를 참조했다는 사실
            # 자체는 남아야 문서고가 claim↔노드 연결을 복원할 수 있다. 문서고가 eid로
            # 다시 병합하므로 중복은 무해하다.
            items = [{"metadata": item.metadata or {}} for item in fresh]

            kept: list[str] = []
            for item in fresh:
                key = _norm(item.content)
                if key in seen:
                    continue
                seen.add(key)
                # 텍스트는 무가공 그대로 싣거나 통째로 스킵만 한다 — 가공하면 판정기가
                # 원문과 대조하는 근거가 달라진다.
                kept.append(item.content)
            content = "\n\n".join(f"[결과 {i}]\n{c}" for i, c in enumerate(kept, 1)) or (
                "(모든 결과가 이 claim의 앞 채널과 중복이라 생략)"
            )
        except Exception as exc:  # noqa: BLE001 — 실패도 "근거 없음"으로 기록하고 계속한다
            logger.warning("검색 실패 | tool=%s | %s", tool_name, exc)
            content = f"(도구 실행 오류: {exc})"
        channels.append({"tool": tool_name, "args": args, "content": content, "items": items})

    return {"claim": claim, "channels": channels}


async def retrieve(
    claims: list[dict], tenant: Tenant, up_to_chapter: int | None = None
) -> dict:
    """claim 목록의 근거를 모아 문서고 입력(evidence)으로 돌려준다."""
    if not claims:
        return {"records": []}

    # 도구 이름(hybrid_search·fact_search)이 route_qav가 부르는 이름과 같다.
    tools = {t.get_name(): t for t in build_retrieval_tools(tenant)}

    def run() -> list[dict]:
        # map은 입력 순서대로 결과를 돌려준다 — 문서고의 claim_index가 이 순서에 묶여
        # 있어 as_completed로 바꾸면 claim↔노드 대응이 조용히 어긋난다.
        with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
            return list(pool.map(lambda c: _retrieve_one(c, tools, up_to_chapter), claims))

    # retriever가 동기 API(Neo4j 드라이버)라 이벤트 루프를 막지 않도록 스레드로 뺀다.
    records = await asyncio.to_thread(run)
    return {"records": records}
