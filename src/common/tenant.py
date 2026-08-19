"""KG의 테넌트(= 소설 한 편) 키를 다루는 유일한 지점.

Neo4j Community 에디션은 표준 데이터베이스를 하나만 가질 수 있어서, 소설별로 DB를
나누는 방식은 쓸 수 없다. 대신 모든 노드에 테넌트 property를 심고 모든 읽기·쓰기를
그 값으로 좁힌다.

테넌트의 단위는 api-spec이 정한 대로 **userId × workId 조합**이다. workId 단독으로
유니크할 개연성이 높지만(그쪽 DB에서 works.id가 PK다) 그건 Spring 구현의 현재 사실이지
계약이 아니다. workId를 나중에 사용자 스코프로 바꾸면 조용한 교차 조회가 된다.

두 가지 표기를 만든다. 값이 두 벌이라 헷갈릴 수 있지만, 쓰이는 자리가 완전히 달라서
하나로 합칠 수 없다:

  - `id`     : "42:7"  — Cypher 파라미터·벡터 인덱스 필터용. 사람이 읽기 쉽다.
  - `ft`     : "u42w7" — 풀텍스트(Lucene) 필드 한정 검색용.

풀텍스트 쪽이 왜 다른가: 풀텍스트 인덱스는 값을 analyzer로 토큰화해 저장하는데, 우리
인덱스의 analyzer는 한국어용 cjk다. 그 안의 StandardTokenizer가 "42:7"을 콜론에서 잘라
["42", "7"] 두 토큰으로 만든다 — 그러면 12:34와 34:12가 구분되지 않는다. 영숫자가 이어진
"u42w7"은 어떤 analyzer로도 한 토큰으로 남는다.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Tenant:
    """소설 한 편을 가리키는 테넌트 키."""

    user_id: int
    work_id: int

    @staticmethod
    def of(user_id: int, work_id: int) -> "Tenant":
        """정수 두 개로 테넌트를 만든다.

        int로 강제하는 것이 안전장치다 — resolver의 병합 필터는 파라미터 바인딩이 없는
        raw WHERE 문자열이라 값이 그대로 Cypher에 박힌다. 여기서 int가 아닌 값을 걸러내면
        그 경로에 주입이 성립할 수 없다.
        """
        return Tenant(user_id=int(user_id), work_id=int(work_id))

    @property
    def id(self) -> str:
        """노드 property `tenant_id`에 저장하는 값. Cypher 파라미터로 그대로 쓴다."""
        return f"{self.user_id}:{self.work_id}"

    @property
    def ft(self) -> str:
        """노드 property `tenant_ft`에 저장하는 값. Lucene 필드 한정 검색어에 쓴다.

        analyzer가 쪼개지 못하도록 구분자 없이 영숫자만 쓴다(위 모듈 docstring 참고).
        """
        return f"u{self.user_id}w{self.work_id}"

    def params(self) -> dict:
        """Cypher 파라미터 dict. 쿼리에서 $tenant_id / $tenant_ft로 받는다."""
        return {"tenant_id": self.id, "tenant_ft": self.ft}

    def filter_literal(self) -> str:
        """resolver에 넘길 WHERE 절.

        neo4j-graphrag의 EntityResolver는 filter_query를 파라미터 없이 문자열로만 받는다
        (`MATCH (entity:__Entity__) {filter_query}` 형태로 이어 붙인다). 그래서 값을
        직접 박아야 하는데, of()가 int를 강제하므로 이 문자열에는 숫자와 콜론만 들어간다.
        """
        return f"WHERE entity.tenant_id = '{self.id}'"
