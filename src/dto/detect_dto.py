"""설정 오류 탐지 API의 요청·응답 모델. 와이어 포맷은 camelCase다.

status는 DB(detection_jobs.status)와 같은 대문자 어휘를 쓴다 — 이 서비스가 그 테이블에
직접 쓰기 때문에, 대문자와 소문자 두 표현이 한 시스템에 공존할 이유가 없다.
"""

from src.dto.common import CamelModel


class DetectRequest(CamelModel):
    # userId × workId가 KG 테넌트(소설 한 편)를 가리킨다. 인덱싱과 같은 키여야
    # 인덱싱한 그래프를 검사가 찾을 수 있다.
    #
    # jobId는 Spring이 발급한다 — 검사 요청 하나가 회차 하나라 호출자가 부여한 id를
    # 그대로 쓰는 편이 단순하고, 이 서버가 죽어도 "무엇을 맡겼는지"가 그쪽에 남는다.
    job_id: str
    user_id: int
    work_id: int
    episode_number: int
    text: str


class JobAck(CamelModel):
    job_id: str
    status: str


class DetectFinding(CamelModel):
    """설정 오류로 판정된 claim 하나.

    **오류가 아닌 claim은 여기 오지 않는다.** 화면이 그리는 것이 오류 목록이라, 일치하거나
    근거가 없어 판단할 수 없는 claim까지 내보내면 회차당 수백 건이 오간다. 검사한 총량은
    claimCount가 대신 말해준다.
    """

    claim_id: str  # P1~PN. 추출 순서 = 원고 등장 순서라 화면 정렬에 그대로 쓸 수 있다.
    quote: str  # 원고에서 문제가 된 서술 그대로
    axis: str  # 무엇에 대한 주장인가(예: "서진우의 소속")
    value: str  # 원고가 주장하는 값
    # 근거가 된 원고 줄. `{lineNo, text}` 목록이다. 화면이 본문 위에 하이라이트를 건다.
    # 번호만 보내던 것을 원문과 함께 보내도록 바꿨다 — 줄 번호는 이 서버의 분할
    # (service/detect/lines.py)이 매긴 것이라, 받는 쪽은 그 분할을 재현할 수 없어
    # 91번 줄이 어느 문장인지 알 방법이 없었다.
    lines: list[dict]
    # 지금은 항상 true다(오류만 싣는다). 임계값을 옮기거나 "의심" 등급을 더해도 계약이
    # 안 바뀌도록 필드로 둔다.
    is_error: bool
    reason: str
    # 근거 원문. `{episodeNo, chunkIndex, text}` 목록이다. 좌표만으로는 받는 쪽이 몇 번
    # 조각이 어느 문장인지 알 수 없어(lines와 같은 이유) 원문을 함께 싣는다. 좌표는
    # 그래프의 어느 조각이었는지 되짚는 용도로 남긴다.
    cited: list[dict]


class DetectStatus(CamelModel):
    job_id: str
    # QUEUED | RUNNING | DONE | ERROR
    status: str
    # EXTRACT | RETRIEVE | JUDGE. 진행 중일 때만 값이 있다.
    # 판정은 한 번에 배치로 하므로 claim 단위 진행률이라는 게 없다 — 대신 어느 단계인지만
    # 알린다. DB에는 없는 값이다(메모리 전용).
    phase: str | None = None
    # 검사한 claim 총수. 추출이 끝나야 알 수 있어 그 전에는 null이다.
    claim_count: int | None = None
    # 오류로 판정된 수. DB 컬럼과 같은 의미로 항상 실린다(DONE 전에는 0).
    contradiction_count: int = 0
    # 완료 전에는 null이다. 절대 빈 배열이 아니다 — 빈 배열은 "검사했는데 오류 0건"이라는
    # 다른 뜻이라, 진행 중을 그렇게 표시하면 호출자가 폴링을 멈춘다.
    findings: list[DetectFinding] | None = None
    detail: str | None = None
