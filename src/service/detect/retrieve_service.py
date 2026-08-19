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
#
# 임베딩 세마포어(_EMBEDDING_CONCURRENCY)와 **같은 값이어야 한다.** 이 스레드가 하는 일이
# 임베딩 + Neo4j 조회인데 임베딩이 그 세마포어를 지나므로, 스레드만 늘리면 세마포어 앞에
# 줄만 길어지고 처리량은 그대로다(둘이 직렬로 걸려 있다).
#
# 24인 근거(실측, 2026-08-16): 임베딩 처리량이 36.7/초로 RPM 충전율 50/초 아래라 지속
# 가능하다. 자세한 산정은 src/common/llm_limit.py의 _EMBEDDING_CONCURRENCY 주석 참고.
# 다른 축도 여유롭다 — Neo4j 드라이버 풀 기본 100 중 24개(24%)를 쓴다.
_WORKERS = 24

# 풀은 **프로세스 전역**이다. 예전에는 검사마다 `with ThreadPoolExecutor(...)`로 새로
# 만들었는데, 그러면 `_WORKERS`가 "검사 하나 안에서 8개"라는 뜻이 되어 동시 검사 10건이면
# 스레드가 80개가 된다. 게이트웨이 세마포어는 OpenAI 호출만 세므로 이 축을 못 막는다
# (여기 스레드는 임베딩만이 아니라 Neo4j 쿼리도 돌린다 — 드라이버 커넥션도 함께 쓴다).
#
# ⚠️ `with`로 감싸지 말 것. ThreadPoolExecutor.__exit__이 shutdown(wait=True)을 부르고,
# 한 번 shutdown된 executor는 되살릴 수 없어 두 번째 검사부터 RuntimeError가 난다.
# 같은 이유로 shutdown()도 부르지 않는다 — 프로세스가 끝날 때 atexit이 정리한다.
_POOL = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="detect-retrieve")

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
        failed = False
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
            failed = False
        except Exception as exc:  # noqa: BLE001 — 한 채널의 실패로 검사 전체를 멈추지 않는다
            logger.warning("검색 실패 | tool=%s | %s", tool_name, exc)
            content = f"(도구 실행 오류: {exc})"
            failed = True
        channels.append(
            {"tool": tool_name, "args": args, "content": content, "items": items, "failed": failed}
        )

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
        # 있어 as_completed로 바꾸면 claim↔노드 대응이 조용히 어긋난다. 공유 풀이라
        # 다른 검사의 작업과 섞여 실행되지만, map은 **이 호출이 제출한** future들을
        # 순서대로 거둬들이므로 순서 계약은 그대로다.
        return list(_POOL.map(lambda c: _retrieve_one(c, tools, up_to_chapter), claims))

    # retriever가 동기 API(Neo4j 드라이버)라 이벤트 루프를 막지 않도록 스레드로 뺀다.
    records = await asyncio.to_thread(run)

    # 채널 하나가 실패하는 건 흔하다(그 질의로 걸리는 게 없거나 일시적 오류) — 근거가
    # 조금 얇아질 뿐이라 계속 간다. 그런데 **전부** 실패했다면 그건 근거가 없는 게 아니라
    # 그래프에 닿지 못한 것이다. 그대로 두면 판정기가 빈 문서고를 보고 "대조할 설정이
    # 없다"며 전부 낮은 점수를 매기고, 검사는 "오류 0건"으로 완료된다 — 작가는 검사가
    # 돌지도 않았다는 걸 모른 채 초록불을 본다. 조용한 성공보다 시끄러운 실패가 낫다.
    #
    # 실패 표시는 content가 아니라 flag로 센다. 실패 사유를 content에 적어두긴 하지만
    # **문서고는 content를 읽지 않는다**(items의 metadata만 본다) — 그쪽에 적힌 문구로
    # 판정하려 들면 아무 효과가 없다.
    total = sum(len(r["channels"]) for r in records)
    failed = sum(1 for r in records for c in r["channels"] if c["failed"])
    if total and failed == total:
        raise RuntimeError(
            f"검색이 전부 실패했다({total}건) — 그래프에 닿지 못한 것으로 보고 검사를 중단한다."
        )
    if failed:
        logger.warning("검색 채널 일부 실패 | %d/%d", failed, total)

    return {"records": records}
