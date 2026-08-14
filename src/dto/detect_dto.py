"""설정 오류 탐지 API의 요청·응답 모델. 와이어 포맷은 camelCase다.

status는 DB(detection_jobs.status)와 같은 대문자 어휘를 쓴다 — 이 서비스가 그 테이블에
직접 쓰기 때문에, 대문자와 소문자 두 표현이 한 시스템에 공존할 이유가 없다.
"""

from src.dto.common import CamelModel


class DetectRequest(CamelModel):
    # userId × workId가 KG 테넌트(소설 한 편)를 가리킨다. 인덱싱과 같은 키여야
    # 인덱싱한 그래프를 검사가 찾을 수 있다.
    job_id: str
    user_id: int
    work_id: int
    episode_number: int
    text: str


class JobAck(CamelModel):
    # 인덱싱과 달리 여기서는 job_id를 Spring이 발급한다 — 검사 요청 하나가 회차 하나라
    # 호출자가 부여한 id를 그대로 쓰는 편이 단순하다.
    job_id: str
    status: str


class DetectClaimProgress(CamelModel):
    """검사 중인 claim 하나의 진행 상황. 프론트가 검사가 끝나기 전에 목록을 그리기 위한 것이다.

    claim 추출이 끝나는 순간 전부 status="running"으로 한꺼번에 나타나고, 검증이 끝난 것부터
    하나씩 status="done"으로 바뀐다(claim들은 병렬 검증이라 끝나는 순서는 index 순이 아니다).
    index는 이 배열 안에서 고정이라 프론트가 행을 안정적으로 식별할 수 있다.
    """

    index: int  # 0부터. 검사가 끝날 때까지 이 claim의 고정 식별자다.
    quote: str  # 신규 회차 원문에서 뽑은 서술 그대로
    category: str  # 생사/소유물/능력/관계/소속/시점 등. 추출기가 정하고 미지정이면 "기타"
    status: str  # "RUNNING" | "DONE"
    # 아래 넷은 status="done"이 되기 전까지 전부 null이다(판정 전에는 알 수 없는 값이라서).
    label: str | None = None  # "contradiction" | "consistent" | "unknown"
    established_fact: str | None = None
    # 모델이 "3" 또는 "3화"처럼 돌려줄 수 있어 숫자로 강제하지 않는다 — 조회 API가 판정 결과의
    # 표기 때문에 500을 내면 안 된다.
    source_episode: int | str | None = None
    explanation: str | None = None


class DetectStatus(CamelModel):
    job_id: str
    status: str
    detail: str | None = None
    # claims: 진행 상황(검사 중에도 채워진다). 접수 직후엔 빈 배열이고 절대 null이 아니다.
    # findings: 최종 판정 결과(status="done"일 때만 채워진다). claims와 달리 파이프라인이 만든
    # dict 그대로 나간다 — tool_calls_used·entities처럼 claims에 없는 필드가 더 들어 있다.
    claims: list[DetectClaimProgress] = []
    findings: list[dict] | None = None
