"""요청·응답 모델이 공유하는 기반 클래스.

와이어 포맷(JSON)과 파이썬 필드 이름의 규칙이 서로 다르다. JSON은 camelCase,
파이썬은 snake_case다. 필드 이름 자체를 camelCase로 쓰면 코드 안에 이름 규칙이
두 개 생기므로, 이름은 snake_case로 두고 alias로만 바꾼다.
"""

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class CamelModel(BaseModel):
    """JSON은 camelCase, 파이썬 필드는 snake_case.

    populate_by_name=True라서 파이썬 쪽에서 모델을 만들 때는 snake_case 이름을
    그대로 쓸 수 있다(alias로만 만들 수 있게 하면 생성 코드가 전부 camelCase가 된다).
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
