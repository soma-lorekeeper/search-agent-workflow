"""인덱싱 API의 요청·응답 모델. 와이어 포맷은 스펙대로 camelCase다."""

from src.dto.common import CamelModel


class IndexEpisode(CamelModel):
    # episodeId는 Spring의 식별자다 — 이 서버는 해석하지 않고 상태 조회에서 그대로 돌려주기만 한다.
    # episodeNo가 실제 인덱싱 단위(lorekeeper의 Chapter.number)다.
    episode_id: int
    episode_no: int
    # text는 없으면 400으로 돌려주려고 일부러 옵셔널로 받는다. 필수로 선언하면 FastAPI가
    # 먼저 422를 내는데, 스펙은 검증 실패를 400 {"detail": ...}로 정해뒀다.
    text: str | None = None


class IndexRequest(CamelModel):
    # userId × workId가 KG 테넌트(소설 한 편)를 가리킨다. 그래프의 모든 노드가 이 키로
    # 표시되고, 모든 읽기가 이 키로 좁혀진다.
    user_id: int
    work_id: int
    # 비었을 때 422가 아니라 400을 주려고 기본값을 둔다(text와 같은 이유).
    episodes: list[IndexEpisode] = []


class IndexAccepted(CamelModel):
    job_id: str
    user_id: int
    work_id: int
    episode_ids: list[int]
    requested_at: str
    remaining_tpm: int


class IndexEpisodeStatus(CamelModel):
    episode_id: int
    # QUEUED | RUNNING | DONE | ERROR. 모든 화가 DONE 또는 ERROR면 그 작업은 끝난 것이다
    # (작업 단위 status 필드는 따로 두지 않는다 — 화별 상태에서 유도되는 값이라 두 곳에
    # 같은 사실을 적어두면 어긋날 수 있다).
    status: str
    error: str | None = None


class IndexJobStatus(CamelModel):
    job_id: str
    user_id: int
    work_id: int
    episodes: list[IndexEpisodeStatus]
