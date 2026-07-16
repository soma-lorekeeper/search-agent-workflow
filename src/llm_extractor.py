import json

from openai import OpenAI

from src.config import OPENAI_API_KEY, OPENAI_EXTRACTION_MODEL

_client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """\
너는 소설 원문에서 세계관 정보를 구조화해 추출하는 어시스턴트다.
주어진 화(episode) 원문을 읽고 아래 JSON 스키마로만 응답하라. 다른 텍스트는 절대 출력하지 마라.

{
  "summary": "이번 화 내용을 3~6문장으로 요약",
  "entities": [
    {"name": "고유 명칭", "type": "character|item|location|faction|skill", "description": "짧은 설명"}
  ],
  "facts": [
    {
      "subject": "entities에 있는 이름",
      "predicate": "관계/사건을 나타내는 짧은 서술어 (예: owns, defeated, joined, stole_from, evolved_into, has_stat)",
      "object": "entities에 있는 이름 (엔티티 간 관계일 때)",
      "object_text": "object가 엔티티가 아니라 값/설명일 때 사용 (예: 능력치 수치, 자유 서술)",
      "valid_from_episode": 이 사실이 성립하기 시작한 화 번호(모르면 현재 화),
      "valid_until_episode": 이 사실이 더 이상 유효하지 않게 된 화 번호(모르면 null),
      "note": "판단 근거가 된 원문 표현을 짧게 인용"
    }
  ]
}

규칙:
- object 또는 object_text 중 하나만 채운다 (관계 대상이 등장인물/아이템이면 object, 단순 서술/수치면 object_text).
- 이미 알려진 엔티티 목록에 있는 이름은 표기를 그대로 재사용해 동일 인물/아이템이 분리되지 않게 하라.
- 이번 화에서 실제로 서술된 사실만 추출한다. 추측하거나 지어내지 마라.
- 소유/관계가 이번 화에서 끝나거나 바뀌면 valid_until_episode를 채워라.
"""


def extract_episode(episode_num: int, text: str, known_entities: list[str]) -> dict:
    known = ", ".join(known_entities) if known_entities else "(없음)"
    user_prompt = (
        f"화 번호: {episode_num}\n"
        f"이미 알려진 엔티티 목록: {known}\n\n"
        f"원문:\n{text}"
    )
    response = _client.chat.completions.create(
        model=OPENAI_EXTRACTION_MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )
    content = response.choices[0].message.content
    return json.loads(content)
