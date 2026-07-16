import json
import logging

from langchain_core.tools import tool
from openai import OpenAI

from src import graphdb, mysqldb, vectordb
from src.config import OPENAI_API_KEY, OPENAI_MODEL

_client = OpenAI(api_key=OPENAI_API_KEY)
logger = logging.getLogger("agent.tools")

TEXT2SQL_SYSTEM_PROMPT = """\
너는 소설 세계관 데이터베이스에 대한 자연어 질문을 SQL SELECT 문으로 변환하는 어시스턴트다.

스키마:
- entities(id, name, type, episode_introduced, description)
  - type은 character|item|location|faction|skill 중 하나
- facts(id, subject_id, predicate, object_id, object_text, episode,
        valid_from_episode, valid_until_episode, note)
  - subject_id/object_id는 entities.id를 참조
  - object_id 또는 object_text 중 하나만 채워져 있음
  - predicate는 자유 서술어 문자열 (예: owns, defeated, joined, has_stat 등) — 정확히 일치하지
    않을 수 있으니 LIKE로 유연하게 매칭하는 것을 고려하라
  - valid_until_episode가 NULL이면 아직도 유효한 사실
- episode_summaries(episode, summary)

이름으로 조회할 때는 정확히 일치하지 않을 수 있으니 LIKE '%name%'을 사용하라.
질문에 등장하는 인물/아이템 이름으로 필터링할 때는, 그 엔티티가 subject일 수도 object일
수도 있으므로 반드시 두 join을 모두 만들어 (subject_id로 join한 이름) OR (object_id로
join한 이름)로 필터링하라. subject 쪽만 필터링하면 그 엔티티가 당한 사건(예: 죽임을
당함, 도둑맞음)을 놓친다.

predicate 값은 owns, defeated, killed, joined, stole_from, evolved_into, has_stat,
gave_coins_to 처럼 짧은 영어 snake_case 동사다. 한국어 키워드로 predicate를 필터링하지
마라(절대 일치하지 않는다). 사건의 의미로 좁히고 싶으면 note나 object_text(한국어 원문
인용/서술)에 대해 LIKE를 걸거나, 아예 predicate로는 필터링하지 말고 이름 필터만으로 넓게
가져와라.

아이템 소유 이력처럼 대상이 여러 facts row에 걸쳐 있을 수 있는 질문은, 특정 predicate로
좁히지 말고 관련된 엔티티(이름)를 필터로 삼아 폭넓게 SELECT한 뒤 여러 행을 반환해서
호출자가 직접 시간순으로 해석할 수 있게 하라.

이름으로 이미 필터링했다면 predicate에 추가로 WHERE 조건을 걸지 마라. 질문의 동사(예:
"줬다")에 대응하는 predicate만 필터링하면, 같은 사실이 다른 동의어 predicate(예: gave 대신
owned_by, has_item, transferred_to 등)로 기록되어 있을 때 결과를 놓친다. 이름 필터만으로
가져온 관련 행이 여러 개라도 전부 반환해서 호출자가 predicate들을 직접 읽고 판단하게 하라.

컬럼에는 절대 의미를 해석한 별칭(예: killer_name, victim_name, owner_name)을 붙이지 마라.
subject_id로 join한 엔티티 이름은 반드시 subject_name, object_id로 join한 엔티티 이름은
반드시 object_name으로만 alias하라. predicate 컬럼(주어가 목적어에 대해 무엇을 했는지)을
함께 SELECT해서, 호출자가 "누가 누구에게 무엇을 했는지"를 predicate로 직접 판단하게 하라.
방향을 재해석한 별칭을 붙이면 subject/object가 뒤바뀐 것처럼 오인될 수 있다.

오직 SQL SELECT 문 하나만 출력하라. 설명이나 마크다운 코드블록 없이 SQL 텍스트만.
"""


def _question_to_sql(question: str) -> str:
    response = _client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": TEXT2SQL_SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
    )
    sql = response.choices[0].message.content.strip()
    if sql.startswith("```"):
        sql = sql.strip("`")
        if sql.lower().startswith("sql"):
            sql = sql[3:]
    return sql.strip()


@tool
def vector_search(query: str, top_k: int = 5) -> str:
    """소설 원문 텍스트에서 의미적으로 관련된 구절을 찾는다.
    줄거리 회상, 서술 묘사, '어떤 방식으로 ~했는지' 같은 자연어 질문에 적합하다.
    구조화된 관계(소유, 사건 시점, 능력치 등)를 정확히 물을 때는 graph_query를 대신 사용하라.
    """
    logger.info("vector_search 호출 | query=%r top_k=%d", query, top_k)
    hits = vectordb.search(query, top_k=top_k)
    logger.info(
        "vector_search 결과 | %d개 | %s",
        len(hits),
        [(h["episode"], round(h["score"], 3)) for h in hits],
    )
    if not hits:
        return "관련 문서를 찾지 못했습니다."
    lines = [f"[{h['episode']}화, score={h['score']:.3f}] {h['text']}" for h in hits]
    return "\n---\n".join(lines)


@tool
def structured_query(question: str) -> str:
    """소설 세계관의 구조화된 사실(엔티티 간 관계, 소유 이력, 사건, 능력치 등)을 DB에서 조회한다.
    '누가 무엇을 소유했나', '어떤 아이템이 어떻게 변했나', '두 인물 사이에 어떤 사건이 있었나',
    '~한 적이 있는가(있음/없음 확인)' 같은 질문에 적합하다.
    """
    logger.info("structured_query 호출 | question=%r", question)
    try:
        sql = _question_to_sql(question)
        logger.info("structured_query 생성된 SQL | %s", sql.replace("\n", " "))
        rows = mysqldb.run_select(sql)
        logger.info("structured_query 결과 | %d행", len(rows))
    except mysqldb.UnsafeSQLError as exc:
        logger.warning("structured_query 거부됨 | %s", exc)
        return f"조회 실패: {exc}"
    except Exception as exc:  # noqa: BLE001 — LLM이 생성한 SQL이 잘못됐을 수 있음
        logger.warning("structured_query SQL 실행 오류 | %s", exc)
        return f"SQL 실행 오류 (생성된 SQL이 스키마와 맞지 않을 수 있음): {exc}"
    if not rows:
        return "해당 조건에 맞는 사실을 DB에서 찾지 못했습니다 (기록이 없을 수 있습니다)."
    return json.dumps(rows, ensure_ascii=False, default=str)


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
    확인)' 같은 질문에 적합하다. 특히 물건이나 사건이 여러 인물을 거치는 다단계 추적 질문에서
    vector_search보다 정확하다.
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
