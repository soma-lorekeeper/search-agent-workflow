"""
그래프 추출용 few-shot 예시.

`EXTRACTION_FEW_SHOT`은 LLMEntityRelationExtractor의 프롬프트 템플릿 `{examples}` 자리에
주입된다(ERExtractionTemplate). 무협에 한정하지 않고 웹소설 전반(현대 회귀 판타지, 로맨스
판타지 등)을 커버해 장르 편향 없이 추출 규칙을 가르친다.

각 예시가 시연하는 규칙은 예시 헤더에 표기했다. 규칙의 원문(권위)은 extractor.py의 도메인 추출
규칙과 schema.py의 노드/관계 description에 있으므로, 여기서는 중복 나열하지 않는다.

주의: Event·CharacterState의 description은 name이 버린 정황을 evidence_chunk가 가리키는 청크
원문에 기대어 복원한 서술이다. 각 예시의 description은 그 예시 입력 텍스트에서 확인 가능한
정황만 담아야 하고(원문에 없는 인과·동기·감정을 지어내면 안 됨), name을 어미만 바꿔 되풀이하지
않는다. 고유명·수치·호칭은 원문 표기 그대로 쓴다.
"""

EXTRACTION_FEW_SHOT = r"""
예시 1 (로맨스 판타지 — 소속을 Organization + CharacterState + ABOUT로, 장소 계층)

입력 텍스트:
[chapter:8]
[C0] 의족을 맞춘 강도현은 다시 걷게 되었다. [C1] 그는 왕성의 대연회장에서 열린 연회에 참석했고, 그날 황실을 배신하고 반란군에 가담했다.

출력:
{"nodes": [
  {"id": "0", "label": "Character", "properties": {"name": "강도현", "description": "각성자"}},
  {"id": "1", "label": "Location", "properties": {"name": "대연회장", "description": "왕성 내부의 연회 공간"}},
  {"id": "2", "label": "Location", "properties": {"name": "왕성", "description": "왕국의 중심 성"}},
  {"id": "3", "label": "Organization", "properties": {"name": "반란군", "description": "황실에 맞서는 세력"}},
  {"id": "4", "label": "Event", "properties": {"name": "대연회장 연회 참석과 반란군 가담", "description": "강도현이 왕성의 대연회장에서 열린 연회에 참석한 그날, 황실을 배신하고 반란군에 가담했다.", "chapter": 8, "story_order": 8.0, "evidence_chunk": "C1"}},
  {"id": "5", "label": "CharacterState", "properties": {"name": "왼다리에 의족을 착용해 다시 걷게 됨", "description": "의족을 맞춘 뒤 다시 걷게 되었다. 다리를 잃은 경위나 시점은 원문에 서술되지 않는다.", "evidence_chunk": "C0"}},
  {"id": "6", "label": "CharacterState", "properties": {"name": "반란군에 가담", "description": "왕성 대연회장 연회에 참석한 그날 황실을 배신하며 가담했다.", "evidence_chunk": "C1"}}
],
"relationships": [
  {"type": "APPEARS_IN", "start_node_id": "0", "end_node_id": "4", "properties": {}},
  {"type": "HOSTS", "start_node_id": "1", "end_node_id": "4", "properties": {}},
  {"type": "LOCATED_IN", "start_node_id": "1", "end_node_id": "2", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "5", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "5", "end_node_id": "4", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "6", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "6", "end_node_id": "4", "properties": {}},
  {"type": "ABOUT", "start_node_id": "6", "end_node_id": "3", "properties": {}}
]}

예시 2 (무협 — 같은 회차 두 사건을 story_order 12.0/12.1로 순서 부여, 별칭은 aliases로, 소속(Organization+ABOUT), 사제 관계, 무공)

입력 텍스트:
[chapter:12]
[C0] 청운은 화산파에 정식 입문했다. [C1] 이후 그의 사부 검선 진자강이 매화검법을 전수했고, 청운은 마침내 매화검법을 대성했다.

출력:
{"nodes": [
  {"id": "0", "label": "Character", "properties": {"name": "청운", "description": "화산파 후기지수"}},
  {"id": "1", "label": "Character", "properties": {"name": "진자강", "aliases": "검선", "description": "화산파의 검객"}},
  {"id": "2", "label": "Organization", "properties": {"name": "화산파", "description": "정파 검문"}},
  {"id": "3", "label": "Event", "properties": {"name": "청운의 화산파 입문", "description": "청운이 화산파에 정식으로 입문했다. 입문 경위나 조건은 원문에 서술되지 않는다.", "chapter": 12, "story_order": 12.0, "evidence_chunk": "C0"}},
  {"id": "4", "label": "Event", "properties": {"name": "진자강의 매화검법 전수와 청운의 대성", "description": "청운의 사부인 검선 진자강이 매화검법을 전수했고, 청운이 마침내 그것을 대성했다.", "chapter": 12, "story_order": 12.1, "evidence_chunk": "C1"}},
  {"id": "5", "label": "CharacterState", "properties": {"name": "화산파에 정식 입문", "description": "청운이 화산파의 정식 문인이 되었다. 입문 경위는 원문에 서술되지 않는다.", "evidence_chunk": "C0"}},
  {"id": "6", "label": "CharacterState", "properties": {"name": "매화검법 대성", "description": "사부 검선 진자강에게 매화검법을 전수받아 마침내 대성했다.", "evidence_chunk": "C1"}}
],
"relationships": [
  {"type": "APPEARS_IN", "start_node_id": "0", "end_node_id": "3", "properties": {}},
  {"type": "APPEARS_IN", "start_node_id": "0", "end_node_id": "4", "properties": {}},
  {"type": "APPEARS_IN", "start_node_id": "1", "end_node_id": "4", "properties": {}},
  {"type": "RELATED_TO", "start_node_id": "0", "end_node_id": "1", "properties": {"type": "사제", "description": "청운이 진자강의 제자로 매화검법을 전수받음"}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "5", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "5", "end_node_id": "3", "properties": {}},
  {"type": "ABOUT", "start_node_id": "5", "end_node_id": "2", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "6", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "6", "end_node_id": "4", "properties": {}}
]}
(주의 1: '검선'은 진자강을 부르는 다른 호칭이므로 aliases에 넣는다 — description에 '검선이라 불리는'처럼 서술로 묻어 두지 않는다.
주의 2: id3 '청운의 화산파 입문'의 근거 원문은 "청운은 화산파에 정식 입문했다" 한 문장뿐이다. description은 이 문장이 주는 것만 풀어 쓰고, 입문 경위·조건처럼 원문에 없는 정황은 '원문에 서술되지 않는다'로 한계를 명시할 뿐 지어내지 않는다 — name을 어미만 바꿔 되풀이하거나 없는 정황을 채우느니 짧게 끝내는 편이 옳다. 단, 원문이 단서·암시를 주는 경우에는 '~로 암시된다'처럼 암시 수준으로 쓰고, '원문에 서술되지 않는다'는 아무 단서도 없을 때만 쓴다.)

예시 3 (로맨스 판타지 — Item(정체성) + 소유 이동을 두 CharacterState + ABOUT로: 넘긴 인물과 받은 인물의 상태를 각각)

입력 텍스트:
[chapter:15]
[C0] 황녀 레아나는 자신의 성물인 빛의 성배를 기사 카일에게 건넸다. [C1] 성배를 받은 카일은 그것을 지키기로 맹세했다.

출력:
{"nodes": [
  {"id": "0", "label": "Character", "properties": {"name": "레아나", "description": "제국의 황녀"}},
  {"id": "1", "label": "Character", "properties": {"name": "카일", "description": "황녀를 호위하는 기사"}},
  {"id": "2", "label": "Item", "properties": {"name": "빛의 성배", "description": "제국 황가에 전해지는 성물"}},
  {"id": "3", "label": "Event", "properties": {"name": "레아나가 카일에게 빛의 성배를 넘김", "description": "황녀 레아나가 자신의 성물인 빛의 성배를 기사 카일에게 건넸고, 성배를 받은 카일은 그것을 지키기로 맹세했다.", "chapter": 15, "story_order": 15.0, "evidence_chunk": "C0,C1"}},
  {"id": "4", "label": "CharacterState", "properties": {"name": "빛의 성배를 카일에게 넘겨 상실", "description": "자신의 성물이던 빛의 성배를 기사 카일에게 직접 건네 넘겼다. 넘긴 이유는 원문에 서술되지 않는다.", "evidence_chunk": "C0"}},
  {"id": "5", "label": "CharacterState", "properties": {"name": "빛의 성배를 넘겨받아 보유", "description": "레아나에게서 빛의 성배를 넘겨받은 뒤 그것을 지키기로 맹세했다.", "evidence_chunk": "C1"}}
],
"relationships": [
  {"type": "APPEARS_IN", "start_node_id": "0", "end_node_id": "3", "properties": {}},
  {"type": "APPEARS_IN", "start_node_id": "1", "end_node_id": "3", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "4", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "4", "end_node_id": "3", "properties": {}},
  {"type": "ABOUT", "start_node_id": "4", "end_node_id": "2", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "1", "end_node_id": "5", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "5", "end_node_id": "3", "properties": {}},
  {"type": "ABOUT", "start_node_id": "5", "end_node_id": "2", "properties": {}}
]}

예시 4 (현대 드라마 — 반례 종합: 변하지 않는 사실도 CharacterState로, description은 사실 저장소가 아님, 엑스트라·일시적 상태 배제)

입력 텍스트:
[chapter:7]
[C0] 대한물산 인사팀에서 계약직으로 일하던 스물여덟 살 유나는 지훈과 함께 퇴근길 지하철을 탔다. [C1] 지하철이 급정거하자 유나가 지훈의 팔을 세게 붙잡았고, 옆자리 할머니와 아이가 놀라 웅성거렸다. [C2] 그 사고로 지훈은 갈비뼈가 부러졌고, 유나는 그달 정직원으로 전환되었다.

출력:
{"nodes": [
  {"id": "0", "label": "Character", "properties": {"name": "유나", "description": "대한물산 인사팀 사무직"}},
  {"id": "1", "label": "Character", "properties": {"name": "지훈", "description": "유나와 같은 회사에 다니는 동료"}},
  {"id": "2", "label": "Location", "properties": {"name": "지하철", "description": "퇴근길 지하철"}},
  {"id": "3", "label": "Organization", "properties": {"name": "대한물산", "description": "유나가 다니는 회사"}},
  {"id": "4", "label": "Event", "properties": {"name": "퇴근길 지하철 급정거 사고", "description": "퇴근길 지하철이 급정거하자 유나가 지훈의 팔을 세게 붙잡았고, 그 사고로 지훈은 갈비뼈가 부러졌다.", "chapter": 7, "story_order": 7.0, "evidence_chunk": "C1,C2"}},
  {"id": "5", "label": "CharacterState", "properties": {"name": "계약직 신분", "description": "대한물산 인사팀에서 계약직으로 일하고 있었다.", "evidence_chunk": "C0"}},
  {"id": "6", "label": "CharacterState", "properties": {"name": "대한물산 인사팀 소속", "description": "대한물산의 인사팀에 소속되어 있었다.", "evidence_chunk": "C0"}},
  {"id": "7", "label": "CharacterState", "properties": {"name": "정직원으로 전환", "description": "지하철 사고가 있던 그달에 계약직에서 정직원으로 전환되었다.", "evidence_chunk": "C2"}},
  {"id": "8", "label": "CharacterState", "properties": {"name": "갈비뼈 골절", "description": "퇴근길 지하철 급정거 사고로 갈비뼈가 부러졌다.", "evidence_chunk": "C2"}},
  {"id": "9", "label": "CharacterState", "properties": {"name": "스물여덟 살", "description": "스물여덟 살이다.", "evidence_chunk": "C0"}},
  {"id": "10", "label": "Organization", "properties": {"name": "인사팀", "description": "대한물산의 부서."}}
],
"relationships": [
  {"type": "APPEARS_IN", "start_node_id": "0", "end_node_id": "4", "properties": {}},
  {"type": "APPEARS_IN", "start_node_id": "1", "end_node_id": "4", "properties": {}},
  {"type": "HOSTS", "start_node_id": "2", "end_node_id": "4", "properties": {}},
  {"type": "PART_OF", "start_node_id": "10", "end_node_id": "3", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "5", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "5", "end_node_id": "4", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "6", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "6", "end_node_id": "4", "properties": {}},
  {"type": "ABOUT", "start_node_id": "6", "end_node_id": "10", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "7", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "7", "end_node_id": "4", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "1", "end_node_id": "8", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "8", "end_node_id": "4", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "9", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "9", "end_node_id": "4", "properties": {}}
]}
(주의 1: '스물여덟 살'처럼 변하지 않는 사실도 CharacterState로 만든다 — 기준은 '변하는가'가 아니라 '지속되고 서사적으로 의미가 있는가'다.
주의 2: 유나의 소속·신분은 description('대한물산 인사팀 사무직')에도 드러나지만, 그것과 무관하게 CharacterState로도 만든다. description과 상태가 겹치는 것은 무방하나, 사실이 description에만 있어서는 안 된다.
주의 3: 옆자리 할머니·아이는 지나가는 엑스트라이므로 Character로 만들지 않는다. 유나가 지훈의 팔을 붙잡은 것은 일시적 접촉이므로 CharacterState로 만들지 않는다. 지속되는 부상인 지훈의 '갈비뼈 골절'만 만든다. 소속('대한물산 인사팀 소속')과 신분('계약직 신분')은 별개의 상태로 나눈다.
주의 4: id5(계약직 신분)·id6(대한물산 인사팀 소속)·id9(스물여덟 살)는 모두 같은 C0 한 문장("대한물산 인사팀에서 계약직으로 일하던 스물여덟 살 유나")에서 뽑혔다. 그래도 각 description은 자기 name이 다루는 부분(고용형태 / 소속 / 나이)에만 초점을 맞추고, 같은 문장을 세 노드에 통째로 복사하지 않는다. 원문이 그 이상을 주지 않으므로 셋 다 짧은 것이 정상이다 — 늘리면 곧 환각이다.
주의 5: 인사팀(id10)은 대한물산(id3)의 부서이므로 PART_OF로 상위 조직에 잇는다(부서→회사, 한 단계씩). 소속 상태(id6)의 ABOUT은 가장 구체적인 조직인 인사팀을 가리키고, 회사(대한물산)는 인사팀의 PART_OF를 따라가면 알 수 있다 — Location의 HOSTS(가장 구체적 장소)+LOCATED_IN(상위 계층)과 같은 구조다.)

예시 5 (작중 창작물 — Item + 역할을 CharacterState + ABOUT로, 작가-독자를 사람-사람 관계로 평탄화하지 않음)

입력 텍스트:
[chapter:2]
[C0] 무명 작가 해무가 쓴 웹소설 <탑의 문>은 10년째 연재 중이었다. [C1] 준호는 그 소설의 유일한 독자로, 매 회차를 빠짐없이 읽었다. [C2] 준호는 가끔 지하철에서 심심풀이로 유행하는 아무 소설이나 제목만 흘려보기도 했다.

출력:
{"nodes": [
  {"id": "0", "label": "Character", "properties": {"name": "해무", "description": "무명 웹소설 작가"}},
  {"id": "1", "label": "Character", "properties": {"name": "준호", "description": "탑의 문의 애독자"}},
  {"id": "2", "label": "Item", "properties": {"name": "탑의 문", "description": "10년째 연재 중인 웹소설"}},
  {"id": "3", "label": "Event", "properties": {"name": "탑의 문 10년 연재와 준호의 완독", "description": "무명 작가 해무가 쓴 웹소설 <탑의 문>이 10년째 연재 중이었고, 준호는 그 유일한 독자로 매 회차를 빠짐없이 읽었다.", "chapter": 2, "story_order": 2.0, "evidence_chunk": "C0,C1"}},
  {"id": "4", "label": "CharacterState", "properties": {"name": "탑의 문의 저자", "description": "무명 작가로서 웹소설 <탑의 문>을 썼고, 그 작품은 10년째 연재 중이었다.", "evidence_chunk": "C0"}},
  {"id": "5", "label": "CharacterState", "properties": {"name": "탑의 문의 유일한 독자", "description": "<탑의 문>의 유일한 독자로 매 회차를 빠짐없이 읽었다.", "evidence_chunk": "C1"}}
],
"relationships": [
  {"type": "APPEARS_IN", "start_node_id": "0", "end_node_id": "3", "properties": {}},
  {"type": "APPEARS_IN", "start_node_id": "1", "end_node_id": "3", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "0", "end_node_id": "4", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "4", "end_node_id": "3", "properties": {}},
  {"type": "ABOUT", "start_node_id": "4", "end_node_id": "2", "properties": {}},
  {"type": "HAS_STATE", "start_node_id": "1", "end_node_id": "5", "properties": {}},
  {"type": "ESTABLISHED_IN", "start_node_id": "5", "end_node_id": "3", "properties": {}},
  {"type": "ABOUT", "start_node_id": "5", "end_node_id": "2", "properties": {}}
]}
(주의 1: 해무와 준호는 '작가-독자'이지만 작품(탑의 문)에 대한 각자의 역할이므로 두 인물 사이에 RELATED_TO를 만들지 않는다. 각자를 그 작품에 대한 CharacterState로 만들고 ABOUT으로 작품에 잇는다.
주의 2: C2의 '유행하는 아무 소설'은 심심풀이로 제목만 흘려본 소품·농담성 언급이라 Item으로 만들지 않는다 — 준호가 실제로 몰입해 읽는 <탑의 문>만 만든다.)
"""
