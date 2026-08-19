"""neo4j-graphrag 라이브러리에 끼워 넣는 계량 어댑터.

라이브러리는 LLM과 embedder를 **주입받도록** 설계돼 있다(각각 `LLMBase`, `Embedder`
인터페이스). 그래서 우리 구현으로 갈아끼우면 라이브러리 내부를 지나는 호출도 관문과
같은 계량·통제를 받는다.

## 왜 감싸지 않고 대체하는가

라이브러리의 `OpenAIEmbeddings.embed_query`는 응답에서 벡터만 꺼내고 원본 HTTP 응답을
그 자리에서 버린다. 그 바깥에서 감싸면 우리가 받는 건 이미 `list[float]`뿐이라
**헤더에도 usage에도 닿을 수 없다.**

embedding 버킷에는 헤더를 주는 다른 경로가 하나도 없어서(임베딩 호출이 전부 라이브러리를
지난다), 대체하지 않으면 그 버킷은 영원히 콜드 스타트 가정값만 가리킨다. 반면 대체 비용은
`embed_query` 한 메서드다 — `Embedder` 인터페이스의 추상 메서드가 그것뿐이다.

## 라이브러리 업그레이드 시

`Embedder`의 추상 메서드가 늘거나 시그니처가 바뀌면 여기도 함께 고쳐야 한다.
`src/service/index/extractor.py`가 같은 계약을 지고 있다(라이브러리 본문을 복제한 곳).
"""

from __future__ import annotations

import logging
from typing import Any

from neo4j_graphrag.embeddings.base import Embedder
from neo4j_graphrag.llm.base import LLMBase
from neo4j_graphrag.llm.types import LLMResponse, LLMUsage
from openai import OpenAI
from pydantic import BaseModel

from src.common import llm_limit
from src.common.openai_client import create_completion
from src.config import EMBEDDING_MODEL, OPENAI_API_KEY

logger = logging.getLogger("graphrag.metered")

# 임베딩은 동기 인터페이스라(`Embedder.embed_query`) 동기 클라이언트를 쓴다.
# 프로세스 하나에 하나만 두어 커넥션 풀을 재사용한다 — 매번 만들면 TLS 핸드셰이크를
# 호출마다 새로 한다.
_client = OpenAI(api_key=OPENAI_API_KEY)


class MeteredEmbedder(Embedder):
    """임베딩 호출을 계량·통제하는 embedder.

    라이브러리가 요구하는 것은 `embed_query(text) -> list[float]` 하나뿐이다
    (`async_embed_query`는 기본 구현이 이걸 그대로 부른다).

    **스레드에서 불린다.** 탐지 검색이 retriever를 스레드풀에서 돌리므로
    (`detect/retrieve_service.py`가 동기 Neo4j 드라이버 때문에 그렇게 한다),
    통제도 `asyncio.Semaphore`가 아니라 `threading.Semaphore`를 써야 한다.
    """

    def __init__(self, model: str = EMBEDDING_MODEL) -> None:
        super().__init__()
        self.model = model

    def embed_query(self, text: str) -> list[float]:
        """텍스트 하나를 임베딩한다.

        세마포어는 **호출 구간만** 감싼다. 응답 파싱은 네트워크가 아니라 CPU라 슬롯을
        붙들고 있을 이유가 없다.

        재시도를 여기서 하지 않는 것은 의도다 — 라이브러리 원본은 `@rate_limit_handler`
        데코레이터로 자체 재시도를 걸지만, 우리는 재시도를 관문 한 곳으로 모으는 중이다
        (지금은 이 경로에 재시도가 없다. 임베딩 429는 상위 호출자에게 그대로 올라간다).
        """
        with llm_limit.thread_slot(self.model):
            # with_raw_response 를 거쳐야 x-ratelimit-* 헤더가 남는다.
            raw = _client.embeddings.with_raw_response.create(input=text, model=self.model)

        # 헤더가 절대값을 주므로 usage로 따로 차감하지 않는다(이중 계산이 된다).
        llm_limit.observe(self.model, raw.headers)
        response = raw.parse()
        return response.data[0].embedding


class MeteredLLM(LLMBase):
    """인덱싱 파이프라인이 쓰는 LLM. 호출을 공용 관문으로 넘긴다.

    라이브러리의 `OpenAILLM`을 상속하지 않고 **직접 구현**한 이유는 하나다. 실제 호출을
    하는 `__ainvoke_v1`/`__ainvoke_v2`가 밑줄 두 개로 시작해 **name mangling**이 걸려
    있어(`_BaseOpenAILLM__ainvoke_v2`) 서브클래스가 오버라이드할 수 없다. 공개
    디스패처인 `ainvoke`를 감싸는 건 가능하지만, 그 시점엔 원본 HTTP 응답이 이미
    `LLMResponse`로 축약된 뒤라 rate limit 헤더에 닿을 방법이 없다.

    직접 구현하면 호출이 `create_completion`을 지나므로 **헤더 계량·세마포어·재시도가
    전부 공짜로 따라온다** — 인덱싱 LLM도 채팅·탐지와 정확히 같은 경로를 탄다.

    ## 라이브러리 계약

    `LLMBase`는 v1(문자열 입력)과 v2(메시지 배열 + response_format) 두 호출 규약을
    **한 메서드에서 입력 타입으로 분기**해 처리하라고 요구한다. 우리 코드가 둘 다 쓴다:
      - v2: 회차 KG 추출 (`extractor.py` — response_format=Neo4jGraph)
      - v1: 회차 요약·전역 요약·description 병합 (`context_service.py`, `resolver.py`)

    ⚠️ **라이브러리 업그레이드 시 동기화 의무**가 있다. 아래 response_format 변환은
    `neo4j_graphrag/llm/openai_llm.py`의 `__ainvoke_v2` 본문을 자구 그대로 옮긴 것이라,
    원본이 바뀌면 여기도 함께 고쳐야 한다(`src/service/index/extractor.py`가 지고 있는
    것과 같은 계약이다).
    """

    # 없으면 LLMEntityRelationExtractor가 use_structured_output=True를 거부한다.
    supports_structured_output: bool = True

    def invoke(self, *args: Any, **kwargs: Any) -> LLMResponse:
        """동기 호출은 쓰이지 않는다. 추상 메서드라 정의는 필요하다.

        조용히 뭔가를 돌려주는 대신 즉시 실패시킨다 — 동기 경로가 생기면 이벤트 루프를
        막게 되므로 드러나는 편이 낫다.
        """
        raise NotImplementedError("MeteredLLM은 비동기 경로(ainvoke)만 지원한다")

    async def ainvoke(
        self,
        input,
        message_history=None,
        system_instruction: str | None = None,
        response_format=None,
        **kwargs: Any,
    ) -> LLMResponse:
        """입력 타입으로 v1/v2를 가른다(라이브러리 디스패처와 같은 규약)."""
        params: dict[str, Any] = dict(self.model_params or {})

        if isinstance(input, str):
            messages = _messages_v1(input, message_history, system_instruction)
        elif isinstance(input, list):
            messages = _messages_v2(input)
            # v2에서는 response_format을 생성자가 아니라 호출 인자로 받는다.
            # model_params에 남아 있으면 충돌하므로 빼낸다(라이브러리와 같은 처리).
            params.pop("response_format", None)
            if response_format is not None:
                kwargs["response_format"] = _to_response_format(response_format)
        else:
            raise ValueError(f"Invalid input type for ainvoke method - {type(input)}")

        response = await create_completion(
            model=self.model_name, messages=messages, **params, **kwargs
        )
        return _to_llm_response(response)


def _messages_v1(input: str, message_history, system_instruction: str | None) -> list[dict]:
    """v1 규약 — 라이브러리가 system/history/user를 조립해 주던 자리."""
    messages: list[dict] = []
    if system_instruction:
        messages.append({"role": "system", "content": system_instruction})
    if message_history:
        # MessageHistory 객체면 .messages, 평범한 리스트면 그대로.
        history = getattr(message_history, "messages", message_history)
        messages.extend(
            {"role": m["role"], "content": m["content"]} for m in history
        )
    messages.append({"role": "user", "content": input})
    return messages


def _messages_v2(input: list) -> list[dict]:
    """v2 규약 — 호출자가 만든 메시지 배열을 그대로 쓴다.

    라이브러리는 openai의 TypedDict로 감싸지만, 그것들은 결국 평범한 dict라 이대로
    넘겨도 동일하다.
    """
    return [{"role": m["role"], "content": m["content"]} for m in input]


def _to_response_format(response_format):
    """Pydantic 모델을 JSON schema로 바꾼다.

    `neo4j_graphrag/llm/openai_llm.py`의 __ainvoke_v2에서 자구 그대로 옮겼다. 원본 주석의
    이유도 그대로다 — beta.parse()는 제약이 많아 JSON schema로 변환해 쓴다.
    """
    if isinstance(response_format, type) and issubclass(response_format, BaseModel):
        return {
            "type": "json_schema",
            "json_schema": {
                "name": response_format.__name__,
                "strict": True,
                "schema": response_format.model_json_schema(),
            },
        }
    # dict 형식(예: {"type": "json_object"})은 그대로 전달한다.
    return response_format


def _to_llm_response(response) -> LLMResponse:
    """OpenAI 응답을 라이브러리가 기대하는 모양으로 줄인다.

    LLMUsage에는 request/response/total 세 칸뿐이라 reasoning·cached 토큰이 여기서
    떨어진다. 우리 회계는 그 전에 관문이 헤더로 이미 끝냈으므로 손실이 없다 — 줄어드는
    것은 라이브러리가 보는 값뿐이다.
    """
    usage = None
    if getattr(response, "usage", None):
        usage = LLMUsage(
            request_tokens=response.usage.prompt_tokens,
            response_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens,
        )
    content = response.choices[0].message.content or ""
    return LLMResponse(content=content, usage=usage)
