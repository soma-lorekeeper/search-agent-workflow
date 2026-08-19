"""검색 결과를 판정기가 읽을 하나의 문서고로 병합한다.

claim마다 따로 근거를 붙이면 같은 청크·사실이 수십 번 중복돼 판정 입력이 폭증한다.
대신 모든 claim의 검색 결과를 노드 단위로 합쳐 한 번만 싣고, claim은 별칭(C001/F003/E01)
으로 그 노드를 가리킨다.

별칭은 **자연키 정렬**로 부여한다 — elementId 순으로 매기면 같은 그래프·같은 질의라도
실행마다 번호가 달라져 판정 결과를 비교할 수 없다.

전부 평가 하네스(scripts/eval_claims.py)에서 확정한 구현 그대로다.
"""

from __future__ import annotations

# 그래프 덤프에서 뺄 메타/lexical 라벨. 표시용 도메인 라벨 하나를 고를 때 걸러낸다.
_META_LABELS = {'Chapter', 'Chunk', 'Fact', 'Story', '__Entity__', '__KGBuilder__'}

def _domain_label(labels: list | None) -> str:
    for lab in labels or []:
        if lab not in _META_LABELS:
            return lab
    return "Node"

def build_docstore(evidence: dict) -> dict:
    """evidence의 검색 결과를 노드 단위로 병합한 문서고를 만든다.

    같은 청크가 hybrid의 앵커(`[원문 발췌]`)로도, fact의 근거(`[근거 원문]`)로도 실려 오고,
    같은 사실이 fact_search와 entity_search에서 각각 렌더된다. 채널 텍스트를 그대로 이어
    붙이면 이 중복이 claim 수만큼 곱해진다 — 노드 하나를 한 번만 싣고 나머지는 별칭으로
    가리키면 그 곱셈이 사라진다.

    별칭(C014/F003/E02)은 eid가 아니라 **자연키 정렬**로 부여한다. eid는 KG를 다시 빌드하면
    값이 바뀌어서, eid 순서로 번호를 매기면 재빌드마다 문서고 전체가 흔들려 diff가 무의미해진다.
    """
    chunks: dict[str, dict] = {}
    facts: dict[str, dict] = {}
    entities: dict[str, dict] = {}
    refs: list[dict] = []

    def put_chunk(eid, chapter, index, text):
        if not eid or not text:
            return None
        node = chunks.setdefault(
            eid, {"eid": eid, "chapter": chapter, "index": index, "text": text}
        )
        return node["eid"]

    for ci, rec in enumerate(evidence["records"]):
        ref = {"claim_index": ci, "quote": rec["claim"].get("quote", ""),
               "chunks": [], "facts": [], "entities": []}
        for ch in rec["channels"]:
            for item in ch.get("items", []):
                md = item.get("metadata") or {}
                eid, kind = md.get("eid"), md.get("kind")
                if not eid:
                    continue
                if kind == "chunk":
                    put_chunk(eid, md.get("chapter"), md.get("chunk_index"), md.get("text"))
                    if eid not in ref["chunks"]:
                        ref["chunks"].append(eid)
                elif kind == "fact":
                    # 근거 원문은 사실 안에 재수록하지 않고 청크 사전으로 보낸 뒤 참조만 남긴다.
                    ev_keys = []
                    for e in md.get("evidence") or []:
                        k = put_chunk(e.get("eid"), e.get("chapter"), e.get("index"), e.get("text"))
                        if k and k not in ev_keys:
                            ev_keys.append(k)
                    node = facts.get(eid)
                    if node is None:
                        # fact_search와 entity_search가 같은 사실을 다른 키 이름으로 준다
                        # (name vs fact_name). 조립기가 흡수하고 사전에는 한 벌만 남긴다.
                        facts[eid] = {
                            "eid": eid,
                            "label": _domain_label(md.get("fact_labels")),
                            "name": md.get("name") or md.get("fact_name") or "",
                            "description": md.get("description") or "",
                            "chapter": md.get("chapter"),
                            "story_order": md.get("story_order"),
                            "participants": list(md.get("participants") or []),
                            # 사실이 묶인 조직·물건·장소(이름만). entity_search를 안 여는 대신
                            # 소속·소유·무대 정보가 이 경로로 들어온다.
                            "related": list(md.get("related") or []),
                            "evidence": ev_keys,
                        }
                    else:
                        # 두 경로가 채워 온 필드를 합친다(빈 값은 채우고, 근거는 합집합).
                        node["description"] = node["description"] or (md.get("description") or "")
                        if node["story_order"] is None:
                            node["story_order"] = md.get("story_order")
                        for k in ev_keys:
                            if k not in node["evidence"]:
                                node["evidence"].append(k)
                        # entity_search는 related를 주지 않으므로(fact_search 전용 컬럼)
                        # 먼저 온 쪽이 비어 있을 수 있다. 이름 기준 합집합으로 채운다.
                        seen_rel = {r.get("name") for r in node.setdefault("related", [])}
                        for r in md.get("related") or []:
                            if r.get("name") not in seen_rel:
                                node["related"].append(r)
                                seen_rel.add(r.get("name"))
                    if eid not in ref["facts"]:
                        ref["facts"].append(eid)
                elif kind == "profile":
                    entities.setdefault(eid, {
                        "eid": eid,
                        "label": _domain_label(md.get("entity_labels")),
                        "name": md.get("name") or "",
                        "aliases": md.get("aliases") or "",
                        "description": md.get("description") or "",
                        "related": list(md.get("related_characters") or []),
                        "parents": list(md.get("parents") or []),
                    })
                    if eid not in ref["entities"]:
                        ref["entities"].append(eid)
        refs.append(ref)

    def assign(nodes: dict, sort_key, prefix: str, width: int):
        for n, eid in enumerate(sorted(nodes, key=lambda e: sort_key(nodes[e])), 1):
            nodes[eid]["alias"] = f"{prefix}{n:0{width}d}"

    assign(chunks, lambda c: (c["chapter"] or 0, c["index"] or 0), "C", 3)
    assign(facts, lambda f: (f["chapter"] or 0, f["story_order"] or 0, f["name"]), "F", 3)
    assign(entities, lambda e: e["name"], "E", 2)

    # run/draft 같은 평가 아티팩트 식별자는 싣지 않는다 — 하네스가 파일 이름을 짓는 데
    # 쓰던 것이고, 렌더러도 쓰지 않는다.
    return {"chunks": chunks, "facts": facts, "entities": entities, "refs": refs}

def render_docstore(store: dict) -> str:
    """문서고를 judge 프롬프트에 실을 텍스트로 렌더한다. 원문은 무가공 그대로 싣는다."""
    lines: list[str] = ["[문서고 · 원문 청크]"]
    for c in sorted(store["chunks"].values(), key=lambda x: x["alias"]):
        lines.append(f"{c['alias']} ({c['chapter']}화#{c['index']}) {c['text']}")

    lines += ["", "[문서고 · 사실]"]
    for f in sorted(store["facts"].values(), key=lambda x: x["alias"]):
        order = f" · 순서 {f['story_order']}" if f["story_order"] is not None else ""
        head = f"{f['alias']} ({f['label']} · {f['chapter']}화{order}) {f['name']}"
        if f["description"]:
            head += f" — {f['description']}"
        lines.append(head)
        if f["participants"]:
            lines.append(f"     참가자: {', '.join(f['participants'])}")
        # 이 사실이 묶인 조직·물건·장소. 소속·소유·무대를 다투는 판정이 이 줄을 대조한다.
        rel = ", ".join(
            f"{r.get('name')}({r.get('label')})" for r in f.get("related") or [] if r.get("name")
        )
        if rel:
            lines.append(f"     연관: {rel}")
        if f["evidence"]:
            aliases = [store["chunks"][k]["alias"] for k in f["evidence"] if k in store["chunks"]]
            if aliases:
                lines.append(f"     근거: {' '.join(sorted(aliases))}")

    # entity_search를 열지 않는 구성(qav)에서는 개체가 하나도 안 들어온다 — 빈 제목만 남으면
    # judge가 "개체 정보가 있는데 비었다"로 읽을 수 있으므로 섹션째 생략한다.
    if not store["entities"]:
        return "\n".join(lines)

    lines += ["", "[문서고 · 개체]"]
    for e in sorted(store["entities"].values(), key=lambda x: x["alias"]):
        head = f"{e['alias']} ({e['label']}) {e['name']}"
        if e["aliases"]:
            head += f" [별칭: {e['aliases']}]"
        if e["description"]:
            head += f" — {e['description']}"
        lines.append(head)
        rel = ", ".join(
            f"{r.get('name')}({r.get('type')})" if r.get("type") else str(r.get("name"))
            for r in e["related"] if r.get("name")
        )
        if rel:
            lines.append(f"     관련인물: {rel}")
        if e["parents"]:
            lines.append(f"     상위: {', '.join(str(p) for p in e['parents'])}")
    return "\n".join(lines)

def render_claim_refs(
    store: dict,
    claims: list[dict],
    indices: list[int] | None = None,
    include_context: bool = False,
) -> str:
    """claim별로 "어느 노드를 보라"는 참조 목록. 문서고와 claim을 잇는 유일한 통로다.

    indices를 주면 그 claim만 렌더한다(judge 배치용). include_context는 기본 False로 둔다 —
    켜면 토큰이 늘어 report_context의 기존 수치와 어긋난다. 판정 때만 켠다.
    """
    lines = ["[claim별 참조]"]
    wanted = None if indices is None else set(indices)
    for ref in store["refs"]:
        if wanted is not None and ref["claim_index"] not in wanted:
            continue
        claim = claims[ref["claim_index"]]
        # 스키마마다 이름이 다르다: sav=subject/attribute/value, stm=statement, qav=axis/value.
        desc = " / ".join(
            str(claim.get(k))
            for k in ("subject", "attribute", "axis", "value", "statement")
            if claim.get(k)
        )
        def al(kind, keys):
            return " ".join(sorted(store[kind][k]["alias"] for k in keys if k in store[kind])) or "-"
        cols = f"{al('chunks', ref['chunks'])} | {al('facts', ref['facts'])}"
        if store["entities"]:  # qav는 entity_search를 안 열어 이 열이 통째로 비어 있다
            cols += f" | {al('entities', ref['entities'])}"
        # 식별자는 코드가 붙인 P번호를 쓴다. 없는 스키마(sav 등)는 예전 `#N` 표기로 떨어진다.
        head = claim.get("id") or f"#{ref['claim_index'] + 1}"
        # 후보 줄. 판정이 이 중에서 근거가 된 줄을 골라 lineIds로 돌려준다.
        span = f" [줄 {', '.join(str(n) for n in claim['lines'])}]" if claim.get("lines") else ""
        lines.append(f"{head} \"{ref['quote']}\" ({desc}){span}  → {cols}")
        # sav 전용 필드다. 원고 내부에만 근거가 있는 모순(v5-GT3 유형)을 전달하는 통로인데,
        # qav는 이 필드를 없애고 원고 전문을 judge에 직접 넘긴다.
        inner = (claim.get("context") or "").strip()
        if include_context and inner:
            lines.append(f"     원고 내부 관련 서술: {inner}")
    return "\n".join(lines)
