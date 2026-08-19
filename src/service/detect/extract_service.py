"""1단계 — 원고에서 검증 대상 주장(claim)을 뽑는다.

claim 하나는 네 필드다: quote(원문 서술), axis(무엇에 대한 주장인가), value(그 값),
lines(근거 줄 번호). 옳고 그름은 여기서 판단하지 않는다 — 원고가 주장하는 바를 구조화해
옮기기만 하고, 대조는 판정 단계가 한다.

전부 평가 하네스(scripts/eval_claims.py)에서 확정한 구성 그대로다:
line-3000 + qav2, 즉 원고 전역에 줄 번호를 매긴 뒤 3000자 조각으로 나눠 병렬 추출한다.
"""

from __future__ import annotations

import asyncio
import json

from src.common import usage
from src.common.openai_client import create_completion
from src.common.tenant import Tenant
from src.config import EXTRACTION_MODEL
from src.service.detect import entity_nodes, prompts
from src.service.detect.lines import number_lines, split_lines

# 원고를 나누는 조각 크기(글자). 1800·2500·3000을 n=5로 재서 고른 값이다 — 커버리지는
# 2500·3000이 동률로 가장 높았고 그중 3000이 호출 수가 적어 27% 쌌다.
CHUNK_SIZE = 3000

# 반복 전달되는 프리픽스(기준+few-shot)의 프롬프트 캐시 라우팅 안정화용 키.
_CACHE_KEY = "detect-extract"


def _cap_for(chunk_size: int) -> int:
    """조각 하나에서 뽑을 수 있는 claim의 상한.

    폭주 방지용이지 목표가 아니다. 상한은 **실제 조각 길이가 아니라 설정된 조각 크기**로
    계산한다 — 조각마다 값이 달라지면 system 프롬프트가 조각마다 달라져 프롬프트 캐시가
    통째로 깨진다(입력이 매번 새 프리픽스가 된다).
    """
    return max(20, chunk_size // 30)


def _chunk(text: str) -> list[str]:
    """원고를 줄 번호가 붙은 조각들로 나눈다.

    번호는 **원고 전역으로 한 번** 매긴 뒤 그 줄들을 조각으로 묶는다. 조각마다 1부터 다시
    매기면 L번호가 원고 안에서 유일하지 않게 되어, 합친 뒤에는 claim이 가리키는 줄을 찾을
    수 없다. 경계도 줄에서만 끊어 문장이 잘리지 않게 한다.
    """
    units: list[str] = []
    cur: list[str] = []
    size = 0
    for line in number_lines(split_lines(text)).split("\n"):
        # 첫 줄은 무조건 담는다 — 한 줄이 조각 크기보다 길어도 빈 조각을 만들지 않는다.
        if cur and size + len(line) > CHUNK_SIZE:
            units.append("\n".join(cur))
            cur, size = [], 0
        cur.append(line)
        size += len(line)
    if cur:
        units.append("\n".join(cur))
    return units


def _build_system(tenant: Tenant, up_to_chapter: int | None) -> str:
    """추출 system 프롬프트. 기준 + few-shot + 범위 + 등장인물 노드 순으로 잇는다."""
    scope = prompts.EXTRACT_SCOPE.replace("{max_claims}", str(_cap_for(CHUNK_SIZE)))
    nodes = entity_nodes.render(tenant, up_to_chapter)
    parts = [prompts.EXTRACT_CRITERIA, prompts.EXTRACT_FEWSHOT, scope]
    if nodes:
        parts.append(prompts.ENTITY_NODE_HEADER + "\n" + nodes)
    return "\n\n".join(parts)


async def _extract_one(system: str, unit: str) -> tuple[list[dict], dict]:
    """조각 하나에서 claim을 뽑는다."""
    response = await create_completion(
        model=EXTRACTION_MODEL,
        response_format={"type": "json_object"},
        prompt_cache_key=_CACHE_KEY,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": unit},
        ],
    )
    data = json.loads(response.choices[0].message.content or "{}")
    return data.get("claims", []) or [], usage.from_response(response)


def assign_claim_ids(claims: list[dict]) -> None:
    """claim에 `P1`~`PN` 식별자를 코드가 붙인다(제자리 수정). P는 Proposal.

    추출기에게 맡기지 않는 이유는 id가 모델이 만들어야 할 정보가 아니라 시스템이 부여하는
    식별자이기 때문이다. 조각을 병렬로 불러도 `asyncio.gather`가 순서를 보존하고 claims를
    조각 순서대로 이어 붙이므로, 번호는 **원고 등장 순서**와 같고 실행마다 결정적이다.

    접두어가 `C`가 아니라 `P`인 것은 문서고 별칭과 겹치지 않게 하기 위해서다. 판정
    프롬프트에는 claim 식별자(`P12`)와 문서고 별칭(`C001`/`F003`)이 같이 실리므로,
    접두어가 겹치면 판정기가 claim 번호를 근거로 인용해 버릴 수 있다.
    """
    for i, c in enumerate(claims, 1):
        c["id"] = f"P{i}"


async def extract(
    text: str, tenant: Tenant, up_to_chapter: int | None = None
) -> tuple[list[dict], list[str], dict]:
    """원고에서 claim을 뽑는다.

    반환: (claims, lines, 토큰 사용량). lines는 번호를 매긴 원본 줄 목록이라, 호출자가
    claim의 lines 번호로 원문을 되짚을 수 있다.
    """
    lines = split_lines(text)
    units = _chunk(text)
    # 등장인물 노드 조회는 Neo4j 왕복이고 드라이버가 동기 API다. 여기서 그냥 부르면
    # 그동안 이벤트 루프가 통째로 멈춰 인덱싱 워커·채팅·헬스체크가 다 같이 선다.
    system = await asyncio.to_thread(_build_system, tenant, up_to_chapter)

    # gather는 순서를 보존한다 — claim 번호가 원고 등장 순서와 같아지는 근거다.
    results = await asyncio.gather(*[_extract_one(system, u) for u in units])

    claims = [c for cs, _ in results for c in cs]
    # 조각을 다 합친 **뒤에** 번호를 매긴다 — 조각별로 매기면 경계에서 충돌한다.
    assign_claim_ids(claims)
    return claims, lines, usage.merge([u for _, u in results])
