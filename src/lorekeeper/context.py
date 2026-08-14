"""
배경 컨텍스트(novel_context) 조립 및 요약 계층.

indexing.py가 각 회차 추출 프롬프트에 주입할 novel_context를 만든다. 세 소스를 결합한다.
  (a) 전역 줄거리 요약 — Story.summary. 매 회차 LLM으로 갱신하되 크기는 일정(상한 지시)하게 유지.
  (b) 최근 회차 요약   — 최근 _RECENT_WINDOW화의 Chapter.summary 원문(직전 연속성 보존).
  (c) 그래프 덤프      — 현재 DB의 도메인 노드/관계를 엔티티 중심 중첩 텍스트로 직렬화.
      관계는 별도 섹션 없이 노드 줄에 인라인하고, Event/CharacterState는 이름·구조 정보만
      싣고 description은 제외해 컨텍스트의 선형 증가를 억제한다.

이 회차의 요약을 만드는 summarize_episode, 전역 요약을 갱신하는 update_global_summary도 여기 둔다.
Chapter.summary는 회차별로 계속 저장한다 — 전역 요약이 drift하면 재구축할 수 있는 원천이다.
"""

from __future__ import annotations

from collections import defaultdict

from .pipeline import build_llm

# 그래프 덤프에서 제외할 라벨. 메타 라벨(writer 부여)과 lexical/provenance/요약 레이어
# (Chunk/Chapter/Story), 검색용 보조 라벨(Fact — facts.ensure_fact_layer가 Event/
# CharacterState에 붙인다)은 배경 컨텍스트로 주지 않는다 — 추출 대상은 도메인 노드다.
_EXCLUDED_LABELS = {"__Entity__", "__KGBuilder__", "Chunk", "Chapter", "Story", "Fact"}

# '최근' 창 크기(회차). 최근 K화의 Chapter.summary 원문을 컨텍스트에 그대로 싣는다.
_RECENT_WINDOW = 3


def _domain_label(labels: list[str]) -> str:
    """메타/lexical을 뺀 도메인 라벨(보통 1개)을 고른다."""
    domain = [lab for lab in labels if lab not in _EXCLUDED_LABELS]
    return domain[0] if domain else "Node"


def _name_of(props: dict) -> str:
    """대표 이름. 모든 도메인 노드가 name을 가지므로 name 하나로 고른다(없으면 '?'로 방어)."""
    return str(props["name"]) if props.get("name") else "?"


def _extras_str(props: dict) -> str:
    """
    name 외 나머지 속성을 'k=v' 나열로 편다. Character/Item/Location/Organization 줄에만 쓴다
    (Event/CharacterState는 이름·구조 정보만 렌더하고 description을 싣지 않는다). 제외 대상:
     - `__` 접두 속성: neo4j-graphrag 내부 식별자(UUID 노이즈).
     - evidence_chunk: EVIDENCED_BY 배선용 내부 번호 — LLM 배경으로는 의미 없다.
     - chapter/story_order: 덤프 렌더러가 괄호 구조 정보로 직접 싣는다(중복 방지).
     - embedding: 검색용 1536차원 벡터(facts.ensure_fact_layer가 :Fact에 붙인다).
       현재 렌더 규칙상 이 줄에 오지 않지만, 새면 배경 컨텍스트가 통째로 망가지므로 막아둔다.
    """
    extras = {
        k: v
        for k, v in props.items()
        if k not in ("name", "chapter", "story_order", "evidence_chunk", "embedding")
        and not k.startswith("__")
        and v not in (None, "")
    }
    return ", ".join(f"{k}={v}" for k, v in extras.items())


def dump_graph_text(driver, database: str) -> str:
    """
    현재 DB의 도메인 노드/관계를 엔티티 중심 중첩 텍스트로 직렬화한다.

    - 관계는 전부 노드 줄에 인라인한다(별도 '## 관계' 섹션 없음):
        Character 하위에 상태(HAS_STATE, 그 안에 ESTABLISHED_IN 회차·ABOUT 대상)와
        인물 관계(RELATED_TO)를 중첩, Event 줄에 장소(HOSTS)·참여(APPEARS_IN) 인라인,
        Location/Organization 줄에 상위(LOCATED_IN/PART_OF) 인라인.
        어떤 규칙에도 안 걸리는 관계 타입은 출발 노드 하위에 일반형으로 인라인.
    - Event/CharacterState는 이름·구조 정보만 싣고 description은 제외한다(덤프 크기의
        대부분을 차지하는 서술을 빼 선형 증가를 억제). CharacterState의 성립 회차는
        chapter 속성이 없어 ESTABLISHED_IN 대상 Event(폴백: EVIDENCED_BY Chunk)의
        chapter로 판정한다.
    - 메타/lexical/요약 라벨(__Entity__/__KGBuilder__/Chunk/Chapter/Story)은 제외.

    DB가 비어 있으면(첫 회차) 빈 문자열을 반환한다.
    """
    node_records, _, _ = driver.execute_query(
        """
        MATCH (n)
        WHERE NOT n:Chunk AND NOT n:Chapter AND NOT n:Story
        RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props
        """,
        database_=database,
    )
    if not node_records:
        return ""

    # 도메인 노드 사이의 관계만 조회(양끝이 Chunk/Chapter/Story가 아닌 것).
    rel_records, _, _ = driver.execute_query(
        """
        MATCH (a)-[rel]->(b)
        WHERE NOT a:Chunk AND NOT b:Chunk AND NOT a:Chapter AND NOT b:Chapter
          AND NOT a:Story AND NOT b:Story
        RETURN elementId(a) AS s, type(rel) AS t, elementId(b) AS e,
               properties(rel) AS props
        """,
        database_=database,
    )

    # CharacterState의 성립 회차 맵. 상태 노드엔 chapter 속성이 없으므로 ESTABLISHED_IN 대상
    # Event.chapter를 쓰고, (고아 상태 대비) 없으면 EVIDENCED_BY Chunk.chapter로 폴백한다.
    state_ch_records, _, _ = driver.execute_query(
        """
        MATCH (s:CharacterState)
        OPTIONAL MATCH (s)-[:ESTABLISHED_IN]->(ev:Event)
        OPTIONAL MATCH (s)-[:EVIDENCED_BY]->(ck:Chunk)
        RETURN elementId(s) AS id, coalesce(min(ev.chapter), min(ck.chapter)) AS chapter
        """,
        database_=database,
    )
    state_chapter = {r["id"]: r["chapter"] for r in state_ch_records}

    # 조회 결과를 파이썬 자료구조로 편다.
    label_of: dict[str, str] = {}
    props_of: dict[str, dict] = {}
    for r in node_records:
        label_of[r["id"]] = _domain_label(r["labels"])
        props_of[r["id"]] = r["props"]

    outs: dict[str, list] = defaultdict(list)  # 출발 노드 id → [(타입, 도착 id, 관계 props)]
    ins: dict[str, list] = defaultdict(list)  # 도착 노드 id → [(타입, 출발 id, 관계 props)]
    for r in rel_records:
        outs[r["s"]].append((r["t"], r["e"], r["props"] or {}))
        ins[r["e"]].append((r["t"], r["s"], r["props"] or {}))

    # 인라인 규칙이 소화하는 관계 타입. 이 밖의 타입은 출발 노드 하위에 일반형으로 인라인.
    _HANDLED = {
        "HAS_STATE", "ESTABLISHED_IN", "ABOUT", "APPEARS_IN",
        "HOSTS", "LOCATED_IN", "PART_OF", "RELATED_TO",
    }

    def _generic_rel_lines(nid: str, indent: str = "  ") -> list[str]:
        """인라인 규칙 밖의 관계를 '· 관계: TYPE → 대상' 일반형으로 렌더(현 스키마에선 빈 목록)."""
        lines = []
        for t, target, rprops in outs[nid]:
            if t in _HANDLED:
                continue
            prop_str = ", ".join(f"{k}={v}" for k, v in rprops.items() if v)
            suffix = f" ({prop_str})" if prop_str else ""
            lines.append(
                f"{indent}· 관계: {t} → {label_of[target]}:{_name_of(props_of[target])}{suffix}"
            )
        return lines

    def _state_line(sid: str, prefix: str = "  · 상태: ") -> str:
        """상태 한 줄. 성립 회차(ESTABLISHED_IN)·대상(ABOUT)을 괄호에 흡수한다(description 미포함)."""
        sprops = props_of[sid]
        ch = state_chapter.get(sid)
        parts = []
        if ch is not None:
            parts.append(f"{ch}화 성립")
        # ABOUT 대상(소유 아이템·소속 조직 등).
        targets = [
            _name_of(props_of[t]) for typ, t, _ in outs[sid] if typ == "ABOUT"
        ]
        if targets:
            parts.append("대상: " + ", ".join(sorted(targets)))
        head = f"{prefix}{_name_of(sprops)}"
        if parts:
            head += f" ({', '.join(parts)})"
        return head

    lines: list[str] = []

    # --- 인물: Character 줄 + 하위에 상태·인물 관계 중첩 ---
    char_ids = sorted(
        (nid for nid, lab in label_of.items() if lab == "Character"),
        key=lambda nid: _name_of(props_of[nid]),
    )
    if char_ids:
        lines.append("## 인물")
        for cid in char_ids:
            extra = _extras_str(props_of[cid])
            lines.append(f"- (Character) {_name_of(props_of[cid])}" + (f" — {extra}" if extra else ""))
            # 상태 중첩(성립 회차 오름차순 → 이력 순서대로 읽힌다).
            state_ids = sorted(
                (t for typ, t, _ in outs[cid] if typ == "HAS_STATE"),
                key=lambda sid: (state_chapter.get(sid) or 0, _name_of(props_of[sid])),
            )
            lines.extend(_state_line(sid) for sid in state_ids)
            # 인물 관계(RELATED_TO).
            for typ, t, rprops in outs[cid]:
                if typ != "RELATED_TO":
                    continue
                prop_str = ", ".join(f"{k}={v}" for k, v in rprops.items() if v)
                suffix = f" — {prop_str}" if prop_str else ""
                lines.append(f"  · 관계: {_name_of(props_of[t])}와 RELATED_TO{suffix}")
            lines.extend(_generic_rel_lines(cid))

        # 소유자(HAS_STATE) 없는 고아 상태도 한 번은 등장해야 한다(무손실).
        orphan_states = sorted(
            (
                nid
                for nid, lab in label_of.items()
                if lab == "CharacterState"
                and not any(typ == "HAS_STATE" for typ, _, _ in ins[nid])
            ),
            key=lambda nid: _name_of(props_of[nid]),
        )
        for sid in orphan_states:
            lines.append(_state_line(sid, prefix="- (CharacterState, 소유자 미상) "))

    # --- 사건: Event 줄에 장소(HOSTS)·참여(APPEARS_IN) 인라인, 회차·순서 순 정렬 ---
    event_ids = sorted(
        (nid for nid, lab in label_of.items() if lab == "Event"),
        key=lambda nid: (
            props_of[nid].get("chapter") or 0,
            props_of[nid].get("story_order") or 0,
        ),
    )
    if event_ids:
        lines.append("## 사건")
        for eid in event_ids:
            eprops = props_of[eid]
            ch = eprops.get("chapter")
            order = eprops.get("story_order")
            pos = f"{ch}화" + (f" #{order}" if order is not None else "") if ch is not None else ""
            parts = [pos] if pos else []
            hosts = sorted(
                _name_of(props_of[s]) for typ, s, _ in ins[eid] if typ == "HOSTS"
            )
            if hosts:
                parts.append("장소: " + ", ".join(hosts))
            actors = sorted(
                _name_of(props_of[s]) for typ, s, _ in ins[eid] if typ == "APPEARS_IN"
            )
            if actors:
                parts.append("참여: " + ", ".join(actors))
            head = f"- (Event) {_name_of(eprops)}"
            if parts:
                head += f" ({', '.join(parts)})"
            # description은 싣지 않는다(이름·구조 정보만 — 덤프 크기 억제).
            lines.append(head)
            lines.extend(_generic_rel_lines(eid))

    # --- 사물·장소·조직: 항상 전체 렌더(정준 식별자라 배경의 핵심이고 총량이 작다) ---
    other_ids = sorted(
        (nid for nid, lab in label_of.items() if lab in ("Item", "Location", "Organization")),
        key=lambda nid: (label_of[nid], _name_of(props_of[nid])),
    )
    if other_ids:
        lines.append("## 사물·장소·조직")
        for oid in other_ids:
            # 상위 계층(LOCATED_IN/PART_OF)을 괄호로 인라인.
            parents = sorted(
                _name_of(props_of[t])
                for typ, t, _ in outs[oid]
                if typ in ("LOCATED_IN", "PART_OF")
            )
            head = f"- ({label_of[oid]}) {_name_of(props_of[oid])}"
            if parents:
                head += f" (상위: {', '.join(parents)})"
            extra = _extras_str(props_of[oid])
            lines.append(head + (f" — {extra}" if extra else ""))
            lines.extend(_generic_rel_lines(oid))

    return "\n".join(lines)


def load_summaries(driver, database: str) -> tuple[str, str]:
    """
    (전역 줄거리 요약, 최근 회차 요약 텍스트)를 로드한다.

    전역: Story.summary(매 회차 update_global_summary가 일정 크기로 갱신).
    최근: 최근 _RECENT_WINDOW화의 Chapter.summary를 회차 오름차순으로 이어붙인 텍스트.
    """
    global_records, _, _ = driver.execute_query(
        "MATCH (s:Story {id:'main'}) WHERE s.summary IS NOT NULL RETURN s.summary AS summary",
        database_=database,
    )
    global_summary = global_records[0]["summary"] if global_records else ""

    recent_records, _, _ = driver.execute_query(
        """
        MATCH (c:Chapter)
        WHERE c.summary IS NOT NULL
        RETURN c.number AS number, c.summary AS summary
        ORDER BY c.number DESC
        LIMIT $window
        """,
        {"window": _RECENT_WINDOW},
        database_=database,
    )
    recent = "\n".join(
        f"[{r['number']}화] {r['summary']}" for r in reversed(recent_records)
    )
    return global_summary, recent


def build_context(graph_dump: str, global_summary: str, recent_summaries: str) -> str:
    """전역 줄거리·최근 회차 요약·그래프 덤프를 섹션 구분해 novel_context로 결합한다."""
    parts: list[str] = []
    if global_summary:
        parts.append("# 지금까지의 전체 줄거리(압축)\n" + global_summary)
    if recent_summaries:
        parts.append("# 최근 회차 요약\n" + recent_summaries)
    if graph_dump:
        parts.append("# 지금까지의 그래프\n" + graph_dump)
    return "\n\n".join(parts)


async def summarize_episode(text: str) -> str:
    """이 회차 원고를 3~5문장으로 요약한다. build_llm('high')로 추론 강도를 높여 인과·복선을 반영."""
    llm = build_llm("high")
    # 이 요약은 Chapter.summary에 저장돼 '다음 회차 추출의 배경 컨텍스트'로 주입된다.
    # 따라서 요약의 오류는 이후 회차 추출로 그대로 전파된다 — 정확성이 간결함보다 우선이다.
    # 실제로 1화(없는 관계 창작)·2화(다른 작품과 혼동) 환각이 오염원이 된 사례가 있어
    # 아래 지시를 명시적으로 넣는다.
    system = (
        "당신은 웹소설 편집자다. 회차 원고를 읽고 이후 회차와 대조할 때 도움이 되도록 "
        "핵심 서사를 간결히 요약한다. 이 요약은 다음 회차를 읽을 때 배경 지식으로 쓰이므로, "
        "틀린 내용이 들어가면 이후 회차 해석까지 오염된다. 간결함보다 정확성이 우선이다."
    )
    user = (
        "다음 회차 원고를 3~5문장의 한국어로 요약하라. 등장인물, 주요 사건, 인물의 상태 변화"
        "(부상·생사·소속·능력·소지품)와 새로 드러난 관계를 중심으로 쓴다.\n\n"
        "지켜야 할 것:\n"
        "- 원문에 서술된 사실만 쓴다. 해석·추정·평가를 덧붙이지 않는다.\n"
        "- 원문에 나오지 않은 인물 사이의 관계·감정을 만들어 내지 않는다"
        "(원문이 부정적으로 그린 관계를 호의적으로 바꿔 쓰는 것도 안 된다).\n"
        "- 고유명(인물·작품·조직·장소)은 원문 표기 그대로 쓴다. 여러 작품·인물이 언급되면 "
        "각각을 구분하고, 어느 것인지 확실하지 않으면 아예 언급하지 않는다.\n\n"
        f"{text}"
    )
    resp = await llm.ainvoke(user, system_instruction=system)
    return resp.content.strip()


async def update_global_summary(
    driver, database: str, chapter: int, chapter_summary: str
) -> str:
    """
    전역 줄거리 요약(Story.summary)을 이번 회차 요약으로 갱신한다.

    첫 회차도 특례 없이 같은 LLM 경로를 태운다(기존 요약이 없으면 빈 입력) — 형식·문체를
    회차와 무관하게 일관되게 만들기 위함이다. 결과는 회차가 누적돼도 일정 크기를 유지해야
    하므로 상한(15문장/1,200자)을 지시하고, 넘치면 오래된 사건을 더 굵게 압축하게 한다.
    갱신과 함께 Story를 모든 Chapter의 부모로 잇는다(Chapter-[:IN_STORY]->Story,
    Chunk-[:IN_CHAPTER]->Chapter와 같은 자식→부모 방향 관례).
    """
    records, _, _ = driver.execute_query(
        "MATCH (s:Story {id:'main'}) WHERE s.summary IS NOT NULL RETURN s.summary AS summary",
        database_=database,
    )
    previous = records[0]["summary"] if records else ""

    llm = build_llm("high")
    # summarize_episode와 같은 이유로 정확성 제약을 명시한다 — 전역 요약의 오류는
    # '이후 모든 회차'의 배경으로 전파되므로 오염 파급이 회차 요약보다 크다.
    system = (
        "당신은 웹소설 편집자다. 지금까지의 전체 줄거리 요약에 이번 회차 요약을 반영해 "
        "전체 줄거리 요약을 갱신한다. 이 요약은 이후 모든 회차 해석의 배경 지식으로 쓰이므로, "
        "틀린 내용이 들어가면 오염이 누적된다. 간결함보다 정확성이 우선이다."
    )
    user = (
        "아래 '기존 전체 줄거리 요약'에 '이번 회차 요약'을 반영한 새 전체 줄거리 요약을 "
        "한국어로 작성하라.\n\n"
        "지켜야 할 것:\n"
        "- 두 입력에 있는 내용만 쓴다. 새 사실·인과·해석을 만들어 내지 않는다.\n"
        "- 압축할 때 행위·예상의 방향(주다/받다, 약속/요구, 생존/죽음, 성공/실패)을 입력 그대로 "
        "유지한다 — 짧게 줄이다 방향이 뒤집히면 사실 왜곡이다.\n"
        "- 고유명(인물·작품·조직·장소)은 입력 표기 그대로 쓴다.\n"
        "- 결과는 회차가 누적돼도 일정 크기를 유지해야 한다: 15문장, 1,200자 이내로 쓰고, "
        "넘칠 것 같으면 오래된 사건을 더 굵게 압축해 최근 전개에 비중을 둔다.\n"
        "- 요약문만 출력한다(제목·머리말 없이).\n\n"
        f"[기존 전체 줄거리 요약]\n{previous if previous else '(없음 — 첫 회차)'}\n\n"
        f"[이번 회차 요약 ({chapter}화)]\n{chapter_summary}"
    )
    resp = await llm.ainvoke(user, system_instruction=system)
    merged = resp.content.strip()

    driver.execute_query(
        """
        MERGE (s:Story {id:'main'})
        SET s.summary = $summary
        WITH s
        MATCH (c:Chapter {number: $chapter})
        MERGE (c)-[:IN_STORY]->(s)
        """,
        {"summary": merged, "chapter": chapter},
        database_=database,
    )
    return merged
