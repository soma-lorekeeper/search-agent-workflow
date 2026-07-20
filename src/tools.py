import json
import logging

from langchain_core.tools import tool
from openai import OpenAI

from src import graphdb
from src.config import OPENAI_API_KEY, OPENAI_MODEL

_client = OpenAI(api_key=OPENAI_API_KEY)
logger = logging.getLogger("agent.tools")

TEXT2CYPHER_SYSTEM_PROMPT = """\
너는 소설 세계관 그래프 데이터베이스(Neo4j)에 대한 자연어 질문을 읽기 전용 Cypher 쿼리로
변환하는 어시스턴트다.

스키마:
- 엔티티 노드 라벨: Character, Item, Location, Faction, Skill (엔티티마다 이 중 하나만 가짐)
  - 공통 속성: name, episode_introduced, description
- Fact 노드: 사실(사건) 하나를 나타낸다.
  - 속성: predicate(자유 서술어, 예: owns, defeated, gave 등), episode, valid_from_episode,
    valid_until_episode(NULL이면 아직 유효), note, object_text(목적어가 엔티티가 아니라 자유
    서술일 때만 채워짐)
- 관계:
  - (주어 엔티티)-[:HAS_FACT]->(Fact)  — 그 엔티티가 주어인 사실
  - (Fact)-[:ABOUT]->(목적어 엔티티)  — Fact의 목적어가 엔티티일 때만 존재 (없으면 Fact.object_text 참고)

핵심 규칙 (과거 실수를 통해 확정된 규칙이므로 반드시 지켜라):
1. 이름으로 필터링할 때는 그 이름의 엔티티가 주어일 수도, 목적어일 수도 있다. 반드시 두
   방향을 모두 찾아라:
     MATCH (n)-[:HAS_FACT]->(f:Fact)
     WHERE n.name CONTAINS '이름'
     OPTIONAL MATCH (f)-[:ABOUT]->(o)
     RETURN n.name AS subject_name, f.predicate AS predicate, f.episode AS episode,
            coalesce(o.name, f.object_text) AS object_name, f.note AS note
     UNION
     MATCH (n)-[:HAS_FACT]->(f:Fact)-[:ABOUT]->(m)
     WHERE m.name CONTAINS '이름'
     RETURN n.name AS subject_name, f.predicate AS predicate, f.episode AS episode,
            m.name AS object_name, f.note AS note
   위와 같이 두 패턴을 UNION으로 합쳐서, 그 엔티티가 관련된 모든 사실(당한 사건 포함)을
   놓치지 않게 하라.
2. 이름으로 이미 필터링했다면 predicate로 추가 필터링하지 마라. 같은 사실이 동의어
   predicate(gave/owned_by/transferred_to 등)로 기록되어 있을 수 있으니, 이름 필터만으로
   넓게 가져와서 호출자가 predicate를 직접 읽고 판단하게 하라.
3. 이름 매칭은 정확히 일치하지 않을 수 있으니 CONTAINS를 사용하라.
4. 아이템/사건이 여러 사람을 거쳐 이동한 것(예: A가 B에게 준 물건을 B가 다시 C에게 줌) 같은
   질문은, 그래프 순회를 활용해 한 번의 쿼리로 체인을 따라가라. 예:
     MATCH (a)-[:HAS_FACT]->(f1:Fact)-[:ABOUT]->(item)
     MATCH (item)-[:HAS_FACT]->(f2:Fact)-[:ABOUT]->(c)
     WHERE item.name CONTAINS '아이템이름'
     RETURN a.name, f1.predicate, f1.episode, item.name, f2.predicate, f2.episode, c.name
     ORDER BY f1.episode, f2.episode
5. RETURN하는 컬럼에는 절대 방향을 해석한 의미론적 별칭(예: giver_name, receiver_name)을
   붙이지 마라. subject_name/object_name처럼 중립적인 이름과 predicate를 함께 반환해서
   호출자가 방향을 직접 판단하게 하라.
6. 오직 읽기 전용 Cypher(MATCH/OPTIONAL MATCH/WHERE/RETURN/WITH/UNION/ORDER BY/LIMIT)만
   사용하라. CREATE/MERGE/SET/DELETE/DETACH/REMOVE/DROP/CALL 등은 절대 사용하지 마라.
7. 결과가 너무 많을 것 같으면 LIMIT을 붙여라 (기본 50 정도).

오직 Cypher 쿼리 하나만 출력하라. 설명이나 마크다운 코드블록 없이 텍스트만.
"""


def _question_to_cypher(question: str) -> str:
    response = _client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": TEXT2CYPHER_SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
    )
    cypher = response.choices[0].message.content.strip()
    if cypher.startswith("```"):
        cypher = cypher.strip("`")
        if cypher.lower().startswith("cypher"):
            cypher = cypher[6:]
    return cypher.strip()


@tool
def graph_query(question: str) -> str:
    """소설 세계관의 구조화된 사실(엔티티 간 관계, 소유 이력, 사건, 능력치 등)을 그래프 DB에서
    조회한다. '누가 무엇을 소유했나', '어떤 아이템이 누구를 거쳐 이동했나'처럼 여러 단계를
    거치는 연쇄/경로 질문, '두 인물 사이에 어떤 사건이 있었나', '~한 적이 있는가(있음/없음
    확인)' 같은 질문에 적합하다.
    """
    logger.info("graph_query 호출 | question=%r", question)
    try:
        cypher = _question_to_cypher(question)
        logger.info("graph_query 생성된 Cypher | %s", cypher.replace("\n", " "))
        rows = graphdb.run_cypher(cypher)
        logger.info("graph_query 결과 | %d행", len(rows))
    except graphdb.UnsafeCypherError as exc:
        logger.warning("graph_query 거부됨 | %s", exc)
        return f"조회 실패: {exc}"
    except Exception as exc:  # noqa: BLE001 — LLM이 생성한 Cypher가 잘못됐을 수 있음
        logger.warning("graph_query Cypher 실행 오류 | %s", exc)
        return f"Cypher 실행 오류 (생성된 쿼리가 스키마와 맞지 않을 수 있음): {exc}"
    if not rows:
        return "해당 조건에 맞는 사실을 그래프에서 찾지 못했습니다 (기록이 없을 수 있습니다)."
    return json.dumps(rows, ensure_ascii=False, default=str)
