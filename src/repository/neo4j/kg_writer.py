"""추출된 도메인 노드에 테넌트를 새겨 넣는 writer.

추출 파이프라인의 노드 생성은 라이브러리(neo4j_graphrag의 Neo4jWriter)가 한다. 우리가
그 사이에 끼어들 자리가 없으면 노드는 테넌트 표시 없이 그래프에 들어가고, 나중에
`MATCH (n) WHERE n.tenant_id IS NULL SET ...` 같은 후처리로 메워야 한다.

그 후처리 방식을 쓰지 않는 이유: 쓰기와 SET 사이에 프로세스가 죽으면 테넌트 없는 노드가
남는데, 다음에 인덱싱하는 **다른 테넌트**의 후처리가 "tenant_id가 비어 있는 노드"라는
술어로 그 고아들을 자기 것으로 흡수한다. 소설 A의 인물이 소설 B의 그래프에 편입되고,
되돌릴 방법이 없다.

그래서 노드 생성과 테넌트 기록이 **같은 upsert 안에서** 일어나게 한다. 라이브러리가
쓰기 직전에 들고 있는 그래프 객체에 property를 채워 넣으면 된다 — 고아가 생길 창 자체가
없어진다.
"""

from __future__ import annotations

from neo4j_graphrag.experimental.components.kg_writer import KGWriterModel, Neo4jWriter
from neo4j_graphrag.experimental.components.types import LexicalGraphConfig, Neo4jGraph
from pydantic import validate_call

from src.common.tenant import Tenant


class TenantTaggingWriter(Neo4jWriter):
    """모든 노드에 tenant_id/tenant_ft를 붙인 뒤 원래 writer에게 넘긴다.

    run의 인자·반환 타입 주석을 원본 그대로 다시 적는 이유: 라이브러리의 ComponentMeta가
    파이프라인 컴포넌트에 타입 주석을 요구하고(주석이 없으면 클래스 정의 시점에 예외),
    그 주석으로 DAG 배선을 검증한다. *args/**kwargs로 넘기면 그 검증이 통과하지 못한다.
    """

    def __init__(self, *args, tenant: Tenant, **kwargs) -> None:
        # tenant는 키워드 전용이다 — 위치 인자로 받으면 라이브러리가 시그니처를 바꿀 때
        # 조용히 다른 인자에 섞여 들어간다.
        super().__init__(*args, **kwargs)
        self._tenant = tenant

    # validate_call이 없으면 안 된다. 오케스트레이터는 앞 컴포넌트의 결과를 model_dump()한
    # **dict**로 넘기고(orchestrator.py), 라이브러리의 Neo4jWriter.run도 이 데코레이터로
    # dict를 Neo4jGraph로 되살린다. 오버라이드가 이를 빠뜨리면 graph.nodes에서 죽는다
    # (2026-08-19 E2E에서 실제로 발생).
    @validate_call
    async def run(
        self,
        graph: Neo4jGraph,
        lexical_graph_config: LexicalGraphConfig = LexicalGraphConfig(),
    ) -> KGWriterModel:
        params = self._tenant.params()
        for node in graph.nodes:
            # properties는 라이브러리가 그대로 노드 property로 쓴다. 쓰기 직전에 채우므로
            # 노드 생성과 테넌트 기록이 같은 upsert 안에서 일어난다.
            node.properties.update(params)
        return await super().run(graph, lexical_graph_config)
