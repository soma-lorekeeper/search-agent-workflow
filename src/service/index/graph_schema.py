"""
KG 스키마 정의 모듈.

웹소설 회차를 누적 인덱싱하기 위한 도메인 스키마를 정의한다. 범위는 다음과 같다.
  - 엔티티: Character / Location / Event / Organization / Item
  - 상태 변화: CharacterState(인물이 특정 시점부터 갖는 상태) + HAS_STATE / ESTABLISHED_IN
  - 대상 지시: 소유·소속·역할처럼 대상이 있는 상태는 ABOUT으로 그 노드를 직접 가리킨다
               (문자열이 아닌 그래프 노드로 식별해 '이 사물의 현재 소유자' 같은 조회를 가능케 함)
  - 시간·공간 구조: Event.chapter / story_order, Location의 LOCATED_IN 계층
  - 근거(provenance): evidence_chunk → EVIDENCED_BY(아래 별도 주석 참고)

조회는 vector RAG로 찾은 노드에서 n-hop 확장해 컨텍스트를 모으고 판단은 LLM이 하는 구조를
전제한다. 따라서 각 노드는 '그 노드만 읽어도 무슨 사실인지 알 수 있게' 자기서술적이어야 한다.

모든 도메인 노드는 name(무엇인지 식별하는 자기서술적 구)과 description(name이 압축하며 버린
정황을, 근거 청크 원문에 기대어 복원한 서술)을 공통으로 가진다 — 타입별 특수 서술 속성
(title/state/evidence)을 두지 않는다. 원문 근거는 evidence_chunk→EVIDENCED_BY→Chunk 링크가
전담하므로 노드에 원문을 축자 인용해 중복 보관하지 않는다.
"""

from neo4j_graphrag.experimental.components.schema import (
    SchemaBuilder,
    NodeType,
    RelationshipType,
    PropertyType,
    GraphSchema,
)

# --- 노드 타입 ---
# additional_properties=False: 스키마 미정의 속성을 GraphPruning이 제거(pruning_stats에 기록).
# RelationshipType은 properties가 비면 라이브러리가 True로 강제하므로 노드에만 실효.

CHARACTER = NodeType(
    label="Character",
    additional_properties=False,
    description=(
        "소설에 등장하는 인물. 이 노드는 '누구인가'를 식별하는 자리이고, 그 인물에 관한 사실·상태는 "
        "변하든 변하지 않든 전부 CharacterState로 둔다(나이·신분·소속·능력·부상·생사·소유·역할 등). "
        "한 번 만든 뒤 값을 갱신하지 않는 정적 식별자 노드다. "
        "서사적으로 의미 있는 인물만 만든다(지나가는 행인·단역 같은 이름 없는 엑스트라는 제외)."
    ),
    properties=[
        PropertyType(
            name="name",
            type="STRING",
            description=(
                "인물의 정식 이름 또는 대표 호칭 하나. 별명·직함·존칭 등으로 다르게 불려도 항상 같은 "
                "대표 이름으로 통일한다(달라지면 같은 인물이 여러 노드로 분열). 변형 표기는 aliases에, "
                "외형/성격 등은 description에."
            ),
        ),
        PropertyType(
            name="aliases",
            type="STRING",
            description=(
                "이 인물이 원문에서 달리 불리는 호칭을 쉼표로 나열한다(예: '독자 씨, 김 대리'). 원문에 실제로 "
                "등장한 호칭만 쓰고 서술·설명은 넣지 않는다('~라 불리는 검객' 같은 문장은 호칭이 아니다). "
                "name은 대표 이름 하나로 유지하고 변형 표기는 전부 여기 모은다 — 다음 회차에서 같은 인물을 "
                "같은 노드로 잇는 단서가 된다. 달리 불리는 호칭이 원문에 없으면 비워 둔다."
            ),
        ),
        PropertyType(
            name="description",
            type="STRING",
            description=(
                "이 인물이 어떤 인물인지 설명하는 자연어 서술(외형/성격/이력 등). 참고·RAG용이며 "
                "CharacterState 등 노드/관계와 내용이 겹쳐도 무방하다. 원문에 근거해 쓰고 추론·평가를 "
                "덧붙이지 않는다. "
                "구조로 표현 가능한 사실은 여기에만 두지 말고 반드시 해당 노드/관계로도 만든다. "
                "이력 추적 대상이 아니며 덮어쓰기 가능."
            ),
        ),
    ],
)

LOCATION = NodeType(
    label="Location",
    additional_properties=False,
    description=(
        "소설 내 장소 하나. 인물이 물리적으로 존재하는 실제 공간만 만든다 — 댓글창·게시판·앱 화면 같은 "
        "온라인·가상 공간은 제외. 상위 장소는 LOCATED_IN으로 한 단계씩만 잇는다(요새→도시→왕국, 건너뛰기 "
        "금지). 상위 장소가 원문에 없으면 노드만 만들어도 된다."
    ),
    properties=[
        PropertyType(
            name="name",
            type="STRING",
            description="장소의 정식 명칭. 여러 표현이 나와도 같은 대표 이름으로 통일한다(달라지면 노드 분열·계층 단절).",
        ),
        PropertyType(
            name="description",
            type="STRING",
            description=(
                "장소의 사건과 무관한 일반 특징(지형/분위기/역할). 특정 사건 전개는 여기 적지 말고 Event로 분리한다. "
                "원문에 근거해 쓰고 추론·평가를 덧붙이지 않는다. "
                "구조로 표현 가능한 사실은 여기에만 두지 말고 반드시 해당 노드/관계로도 만든다."
            ),
        ),
    ],
)

EVENT = NodeType(
    label="Event",
    additional_properties=False,
    description=(
        "소설 내 하나의 사건. 이후 참조·대조될 만한 단위로 추출하고, 문장 단위로 쪼개거나 배경 묘사·사소한 "
        "행동까지 사건화하지 않는다. chapter=이 사건이 실린 연재 회차, story_order=작중 시간순(속성 설명 참고)."
    ),
    properties=[
        PropertyType(
            name="name",
            type="STRING",
            description=(
                "사건을 한 구절로 식별하는 자기서술적 제목. 이 줄만 읽어도 무엇이 일어났는지 알 수 있게 쓰되"
                "('사고' 같은 막연한 제목 금지), 정황·경위·정도는 description이 담당하므로 서술을 제목에 "
                "몰아넣지 않는다. evidence_chunk가 가리키는 원문 범위 안에서만 쓴다 — 원문이 명시하지 않은 "
                "결과(사망·성공·실패)나 인과(누가 누구를 죽였는지)를 단정하지 않고, 원문이 진행 중이면 진행 "
                "중으로 쓴다. 같은 사건이 여러 청크에 걸쳐 언급되면 하나의 Event로 낸다."
            ),
        ),
        PropertyType(
            name="description",
            type="STRING",
            description=(
                "name이 압축하며 버린 정황을 복원한다 — 누가 관여했는지, 어떤 계기로, 어느 정도로, 어떤 "
                "순서로, 원문이 결과를 어디까지 서술했는지. 모든 Event에 채운다. evidence_chunk가 가리키는 "
                "청크의 원문에 근거해 쓰고, 그 청크에 없는 인과·동기·감정은 지어내지 않는다. 고유명·수치·호칭은 "
                "원문 표기 그대로 쓴다. name을 어미만 바꿔 되풀이하면 이 속성은 쓰지 않은 것과 같다 — 덧붙일 "
                "정황이 원문에 없으면 근거 문장을 풀어 쓰는 데서 그친다(짧은 서술이 지어낸 서술보다 낫다)."
            ),
        ),
        PropertyType(
            name="chapter",
            type="INTEGER",
            description="이 사건이 실린 연재 회차 번호. 원문에 회차 표시가 있으면 그 값을 그대로 쓴다.",
        ),
        PropertyType(
            name="story_order",
            type="FLOAT",  # 두 값 사이(예: 3.0/3.1 사이 3.05)에 삽입 가능하도록 FLOAT.
            description=(
                "Event들을 작중 시간순으로 정렬하기 위한 순서값(실제 연도/날짜를 그대로 적지 않고 이 스케일로 "
                "변환). 모든 Event에 채운다. 같은 chapter에 여러 사건이 있으면 발생 순서대로 chapter.0, "
                "chapter.1, chapter.2 …로 0.1씩 증가(예: 3화 세 사건이면 3.0, 3.1, 3.2); 하나면 chapter.0. "
                "회상·과거 사건은 더 작은 값(3화의 '지난달' 사건이면 2.8), 1화보다 이전(프리퀄)이면 0이나 음수"
                "(예: 0.5, -1.0). 두 값 사이 시점이면 그 사이 실수(3.0과 3.1 사이 3.05)."
            ),
        ),
        PropertyType(
            name="evidence_chunk",
            type="STRING",
            description=(
                "이 사건의 근거가 되는 원문이 있는 청크 번호(예: 'C3', 여럿이면 'C3,C4'). 실제 그 원문이 있는 "
                "청크만 쓰고 추측하지 않는다. description은 이 청크의 원문에만 근거해야 하므로 실제 근거 청크를 "
                "빠짐없이 적는다. 후처리에서 EVIDENCED_BY 관계로 바뀌고 노드에서 제거된다."
            ),
        ),
    ],
)

CHARACTER_STATE = NodeType(
    label="CharacterState",
    additional_properties=False,
    description=(
        "인물에 관한 사실·상태 하나. 그 인물에 대해 원문이 알려 주는 것은 **변하든 변하지 않든 여기에 담는다** "
        "— 나이·신분·직급·학년·소속·능력·부상·생사·소지품 소유·작품에 대한 역할 등. "
        "인물에 관한 사실은 Character.description에만 남기지 말고 반드시 이 노드로도 만든다. "
        "대상이 있는 상태는 그 대상을 ABOUT으로 직접 가리킨다: 소지품 소유는 ABOUT→Item, 조직 소속은 "
        "ABOUT→Organization, 작품의 저자·독자·제작자 같은 역할도 ABOUT→Item(대상은 문자열이 아니라 노드로 식별). "
        "소지품이 인물 간 이동하면 넘긴 인물과 받은 인물의 상태를 각각 만든다. "
        "'회사원'·'계약직' 같은 신분·고용형태는 소속과 별개의 상태로 분리한다. "
        "제외 기준은 '변하는가'가 아니라 **지속되는가·서사적으로 의미가 있는가**다 — 일시적 통증·피로처럼 "
        "그 회차에서 소모되는 상태나, 배경 묘사로만 스치고 이후 아무 역할이 없는 사실은 만들지 않는다. "
        "상태가 원문에 명시적으로 제시되면(서술이든 목록·표·공지 형태든) 빠짐없이 만든다 — 특히 한 인물의 "
        "여러 상태가 한자리에 열거되면 일부만 고르지 않는다. "
        "한 인물에 여러 상태가 쌓이며, 각 상태가 언제 성립했는지는 ESTABLISHED_IN이 가리키는 Event로 안다. "
        "상태가 바뀌면 기존 노드를 고치지 말고 항상 새 노드를 만든다 — 변화 이력이 노드의 나열로만 남는다."
    ),
    properties=[
        PropertyType(
            name="name",
            type="STRING",
            description=(
                "이 인물에 관한 사실·상태를 그 자체로 읽히게 서술한다(예: '어깨를 칼날에 깊게 베임', "
                "'대한물산 인사팀에 계약직으로 소속', '코인 6200 보유', '탑의 문의 유일한 독자', "
                "'스물여덟 살', '청일고교 2학년'). "
                "원문 문장을 그대로 옮기지 말고 상태로 압축하되, 무엇에 관한 상태인지 알 수 있을 만큼 "
                "구체적으로 쓴다 — 이 노드만 따로 읽혔을 때도 뜻이 통해야 한다(성립 정황은 description이 담당). "
                "대상이 있는 상태는 그 대상 노드를 ABOUT으로 함께 잇는다."
            ),
        ),
        PropertyType(
            name="description",
            type="STRING",
            description=(
                "name이 압축하며 버린 성립 정황을 복원한다 — 어떤 계기로, 누구를 통해, 어느 정도로 그 상태가 "
                "성립했는지. 모든 CharacterState에 채운다. evidence_chunk가 가리키는 청크의 원문에 근거해 쓰고, "
                "그 청크에 없는 인과·동기는 지어내지 않는다. 고유명·수치·호칭은 원문 표기 그대로 쓴다. name을 "
                "어미만 바꿔 되풀이하면 이 속성은 쓰지 않은 것과 같다 — 덧붙일 정황이 원문에 없으면 근거 문장을 "
                "풀어 쓰는 데서 그친다(짧은 서술이 지어낸 서술보다 낫다). 한 근거 문장에서 여러 상태를 뽑았다면 "
                "각 description은 그 상태에 해당하는 부분에만 초점을 맞춘다(같은 서술을 여러 노드에 복사하지 않는다)."
            ),
        ),
        PropertyType(
            name="evidence_chunk",
            type="STRING",
            description=(
                "이 상태의 근거가 되는 원문이 있는 청크 번호(예: 'C3', 여럿이면 'C3,C4'). 실제 그 원문이 있는 "
                "청크만 쓰고 추측하지 않는다. description은 이 청크의 원문에만 근거해야 하므로 실제 근거 청크를 "
                "빠짐없이 적는다. 후처리에서 EVIDENCED_BY 관계로 바뀌고 노드에서 제거된다."
            ),
        ),
    ],
)

ORGANIZATION = NodeType(
    label="Organization",
    additional_properties=False,
    description=(
        "조직·세력·단체(문파·길드·가문·회사·부서 등). 여러 인물이 공유하는 엔티티라 문자열이 아닌 독립 노드로 "
        "둔다('이 조직의 구성원은 누구인가' 조회를 위해). 인물 소속은 CharacterState를 만들어 ABOUT으로 이 조직에 "
        "잇고, '현재 소속'은 가장 나중에 성립한 상태에서 파생한다. 부서·지부처럼 더 큰 조직의 일부인 조직은 "
        "PART_OF로 상위 조직을 한 단계씩 잇는다(부서→회사, 건너뛰기 금지). 상위 조직이 원문에 없으면 노드만 만들어도 된다."
    ),
    properties=[
        PropertyType(
            name="name",
            type="STRING",
            description="조직의 정식 명칭. 여러 표현이 나와도 같은 대표 이름으로 통일한다(달라지면 노드 분열).",
        ),
        PropertyType(
            name="description",
            type="STRING",
            description=(
                "조직의 사건과 무관한 일반 특징(성격/목적/규모 등) 요약. 참고·RAG용. 원문에 근거해 쓰고 "
                "추론·평가를 덧붙이지 않는다. "
                "구조로 표현 가능한 사실은 여기에만 두지 말고 반드시 해당 노드/관계로도 만든다."
            ),
        ),
    ],
)

ITEM = NodeType(
    label="Item",
    additional_properties=False,
    description=(
        "소유·이동·저작되거나 인물이 주고받는 '사물'(소지품·작중 창작물·무기·성물·선물·첨부물 등). 인물도 장소도 "
        "조직도 아닌, 사람이 다루는 물건/작품이면 여기다. 고유명이 없어도 서사적으로 중요하면 만들되 내용을 "
        "요약한 지시적 이름을 붙인다(예: '작가가 보낸 선물'). 단, 농담·소품으로 스치듯 언급되고 서사에 영향이 "
        "없는 사물(예: 제목만 흘려본 소설)은 만들지 않는다 — 실제로 소유·저작·사용되거나 이후 비중 있게 다뤄지는 "
        "것만. 이 노드는 사물의 정체성(무엇인지·종류·유래)만 담고, '지금 누가 가졌는가'와 '누가 저술·제작·열독했는가'는 "
        "CharacterState+ABOUT→Item으로 별도 표현한다(정체성과 시점별 소유·역할이 공존)."
    ),
    properties=[
        PropertyType(
            name="name",
            type="STRING",
            description=(
                "사물의 정식 명칭. 여러 표현이 나와도 같은 대표 이름으로 통일한다. 고유명이 없으면 내용을 요약한 "
                "지시적 이름을 만든다(예: '작가가 보낸 선물', '노인의 상자')."
            ),
        ),
        PropertyType(
            name="description",
            type="STRING",
            description=(
                "사물의 비시간적 특징(종류/유래/용도/저작 배경 등) 요약. 참고·RAG용. 원문에 근거해 쓰고 "
                "추론·평가를 덧붙이지 않는다. "
                "구조로 표현 가능한 사실은 여기에만 두지 말고 반드시 해당 노드/관계로도 만든다"
                "(소유자·저자 등은 CharacterState가 담당)."
            ),
        ),
    ],
)

# --- 관계 타입 ---

APPEARS_IN = RelationshipType(
    label="APPEARS_IN",
    description=(
        "인물이 사건에 실제로 참여하거나 현장에 있었음. 다른 인물의 대화·회상 속에서 이름만 언급된 경우는 제외."
    ),
)
HOSTS = RelationshipType(
    label="HOSTS",
    description=(
        "장소가 사건의 무대가 됨. 가장 구체적인 장소 하나에만 연결한다(상위 장소는 LOCATED_IN을 따라가면 알 수 있음)."
    ),
)
HAS_STATE = RelationshipType(
    label="HAS_STATE",
    description="인물이 CharacterState를 가짐. 한 인물에 여러 상태가 쌓이면 각각을 모두 이 관계로 연결한다.",
)
ESTABLISHED_IN = RelationshipType(
    label="ESTABLISHED_IN",
    description="CharacterState가 성립된 Event를 가리킨다. 그 Event.chapter가 이 상태의 시간 순서 기준이 된다.",
)
LOCATED_IN = RelationshipType(
    label="LOCATED_IN",
    description="장소가 한 단계 위 장소를 가리킴(요새→도시→왕국). 단계를 건너뛰지 않는다(계층을 순회로 복원하기 위해).",
)
PART_OF = RelationshipType(
    label="PART_OF",
    description=(
        "조직이 한 단계 위 상위 조직을 가리킴(부서→회사, 지부→본부, 계열사→그룹). LOCATED_IN의 조직판이며 "
        "단계를 건너뛰지 않는다(계층을 순회로 복원하기 위해). 상위 조직이 원문에 없으면 노드만 만들어도 된다."
    ),
)

RELATED_TO = RelationshipType(
    label="RELATED_TO",
    additional_properties=False,
    description=(
        "인물↔인물의 서사적 관계(동맹·적대·사제·혈연·연인 등). 종류는 type 속성에. 방향은 주체→대상(상호적 관계는 "
        "한 방향만). 현재 확립된 관계만 담는다(시점 추적이 필요하면 CharacterState로). "
        "작품의 저작·소비처럼 사물을 매개로 한 역할은 사람-사람 관계가 아니므로 이 관계로 묶지 말고, 각자를 그 "
        "사물에 대한 CharacterState로 만든다(작가와 독자를 이 관계로 평탄화하지 않는다)."
    ),
    properties=[
        PropertyType(
            name="type",
            type="STRING",
            description="관계 종류를 짧게(예: 동맹, 적대, 사제, 혈연, 연인). 같은 종류는 항상 같은 표현으로 통일한다.",
        ),
        PropertyType(
            name="description",
            type="STRING",
            description=(
                "이 관계에 대한 짧은 부연(예: '어린 시절 같은 스승 밑에서 수학'). 없으면 생략 가능. "
                "구조로 표현 가능한 사실은 여기에만 두지 말고 반드시 해당 노드/관계로도 만든다 — "
                "부연에 등장하는 인물·조직·사물도 노드로 존재해야 한다."
            ),
        ),
    ],
)

ABOUT = RelationshipType(
    label="ABOUT",
    description=(
        "CharacterState가 어떤 외부 대상에 관한 것인지 그 노드로 직접 가리킨다 — 소유·역할 상태는 Item, "
        "소속 상태는 Organization. 덕분에 대상을 문자열이 아닌 그래프 노드로 식별한다('이 사물의 현재 소유자', "
        "'이 작품의 저자', '이 조직의 구성원' 조회). 부상·생사·능력처럼 외부 대상이 없는 상태에는 만들지 않는다."
    ),
)

# --- 근거(provenance) 레이어 ---
# 아래 노드/관계는 LLM이 아니라 indexing.py가 직접 만든다. 그래서 NODE_TYPES/RELATIONSHIP_TYPES/PATTERNS에는
# 넣지 않는다(넣으면 LLM이 생성하려 든다): Chunk{chapter,index,text,embedding}, Chapter{number,summary},
# (Event|CharacterState)-[:EVIDENCED_BY]->Chunk, (Chunk)-[:IN_CHAPTER]->Chapter, (Chunk)-[:NEXT_CHUNK]->Chunk.

# --- 허용 관계 패턴 ---

PATTERNS = [
    ("Character", "APPEARS_IN", "Event"),
    ("Location", "HOSTS", "Event"),
    ("Character", "HAS_STATE", "CharacterState"),
    ("CharacterState", "ESTABLISHED_IN", "Event"),
    ("Location", "LOCATED_IN", "Location"),
    ("Organization", "PART_OF", "Organization"),
    ("Character", "RELATED_TO", "Character"),
    ("CharacterState", "ABOUT", "Item"),             # 소유·역할(저자/독자/제작자) 대상
    ("CharacterState", "ABOUT", "Organization"),     # 소속 대상
]

# --- 스키마 조립 ---
# SchemaBuilder.run()은 node_types/relationship_types/patterns를 인자로 받으므로,
# 커스텀 파이프라인에서 재사용할 수 있게 이 목록들을 모듈 변수로 노출한다.

NODE_TYPES = [CHARACTER, LOCATION, EVENT, CHARACTER_STATE, ORGANIZATION, ITEM]
RELATIONSHIP_TYPES = [
    APPEARS_IN, HOSTS, HAS_STATE, ESTABLISHED_IN, LOCATED_IN, PART_OF,
    RELATED_TO, ABOUT,
]

SCHEMA: GraphSchema = SchemaBuilder.create_schema_model(
    node_types=NODE_TYPES,
    relationship_types=RELATIONSHIP_TYPES,
    patterns=PATTERNS,
)


if __name__ == "__main__":
    print("스키마 로드 성공")
    print(f"노드 타입: {[n.label for n in SCHEMA.node_types]}")
    print(f"관계 타입: {[r.label for r in SCHEMA.relationship_types]}")
    print(f"패턴: {SCHEMA.patterns}")
