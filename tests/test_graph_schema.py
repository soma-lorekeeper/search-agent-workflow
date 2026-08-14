"""KG 스키마의 계약을 고정한다.

스키마는 LLM에게 "무엇을 어떤 모양으로 뽑으라"고 지시하는 문서다. 여기서 한 줄이 바뀌면
추출 결과의 모양이 바뀌고, 그걸 읽는 검색 쿼리가 조용히 빈손으로 돌아온다 — 예외가 아니라
"결과 없음"으로 나타나므로 눈에 띄지 않는다.

특히 인물 관계는 직접 간선(RELATED_TO)에서 상태를 거치는 형태로 옮겨왔다. 그 전환이
스키마·프롬프트·검색 쿼리 세 곳에서 어긋나지 않게 붙들어 둔다.
"""

from __future__ import annotations

from src.service.index import extraction_examples, extractor, graph_schema


def test_인물_관계는_상태를_거쳐_잇는다():
    """관계를 간선이 아니라 CharacterState + ABOUT→Character로 표현한다.

    간선에 담으면 성립 시점도 근거도 붙일 수 없고, 관계가 바뀌었을 때 이력이 남지 않는다
    (간선을 고치면 이전 값이 사라진다). 상태로 두면 소유·소속과 같은 취급을 받는다.
    """
    assert ("CharacterState", "ABOUT", "Character") in graph_schema.PATTERNS


def test_직접_관계_간선은_스키마에_없다():
    """RELATED_TO가 남아 있으면 추출기가 두 표현 사이에서 흔들린다."""
    labels = {r.label for r in graph_schema.RELATIONSHIP_TYPES}
    assert "RELATED_TO" not in labels
    assert all(rel != "RELATED_TO" for _, rel, _ in graph_schema.PATTERNS)


def test_ABOUT_대상_세_종류가_모두_열려_있다():
    """소유(Item)·소속(Organization)·관계(Character). 하나라도 빠지면 그 부류가 통째로 사라진다."""
    targets = {b for a, rel, b in graph_schema.PATTERNS if a == "CharacterState" and rel == "ABOUT"}
    assert targets == {"Item", "Organization", "Character"}


def test_프롬프트가_양방향_생성을_지시한다():
    """관계는 두 사람 각자의 상태다 — 한쪽만 만들면 다른 인물을 조회했을 때 안 보인다.

    검색에 안전망(상대 쪽 상태로도 찾기)이 있지만 그건 어디까지나 보정이고, 그래프가
    비대칭이면 그 인물의 상태 목록에서 관계가 빠진 채로 남는다.
    """
    assert "양쪽을 모두" in graph_schema.CHARACTER_STATE.description

    # 프롬프트는 줄바꿈이 자주 바뀌므로 공백을 눌러 비교한다.
    guide = " ".join(extractor.KoreanWebNovelERTemplate.DEFAULT_TEMPLATE.split())
    assert "양쪽을 모두 만든다" in guide
    assert "ABOUT→Character" in guide


def test_few_shot이_새_형태를_보여준다():
    """예시가 실제 출력 형태를 가르치는 자리라, 여기가 옛 형태면 지시문이 무력해진다."""
    fewshot = extraction_examples.EXTRACTION_FEW_SHOT
    assert "RELATED_TO" not in fewshot
    # 사제 예시가 양쪽 상태를 모두 만든다.
    assert "진자강의 제자" in fewshot and "청운의 사부" in fewshot


def test_프로필_조회가_자기_상태를_먼저_쓴다():
    """관계를 양쪽에 저장하더라도, 한 사람을 조회할 때는 **그 사람 입장**만 보여준다.

    두 방향을 그냥 합치면 청운을 조회했는데 '진자강의 제자'(청운 입장)와 '청운의 사부'
    (진자강 입장)가 같이 나온다 — 두 번째는 진자강의 프로필에 있어야 할 정보다.
    저장의 대칭성이 화면의 중복으로 새지 않게, 자기 상태를 먼저 모으고 상대 쪽은
    아직 안 나온 인물만 보탠다.
    """
    from src.repository.neo4j import retrieval

    q = retrieval._ENTITY_PROFILE_QUERY
    # 자기 상태(정방향)
    assert "(e)-[:HAS_STATE]->(rs:CharacterState)-[:ABOUT]->(oc:Character)" in q
    # 상대 쪽 상태(역방향) — 한쪽만 만들어진 그래프의 안전망
    assert "(e)<-[:ABOUT]-(rs2:CharacterState)<-[:HAS_STATE]-(oc2:Character)" in q
    # 중복 제거: 정방향에서 이미 나온 인물은 역방향에서 뺀다
    assert "NOT r.name IN [x IN own_rel | x.name]" in q


def test_사실_검색이_관계_상대를_연관으로_싣는다():
    """관계 상태의 ABOUT 대상은 Character다 — 대상 필터가 Item/Organization만 열려 있으면
    관계 상태만 대상 없이 렌더돼 '누구와의 관계인지'가 통째로 빠진다."""
    from src.repository.neo4j import retrieval

    assert "tgt:Item OR tgt:Organization OR tgt:Character" in retrieval._FACT_RELATED
    assert "'Item', 'Organization', 'Character'" in retrieval._FACT_RELATED
