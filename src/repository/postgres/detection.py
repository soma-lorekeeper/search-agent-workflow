"""탐지 결과를 Spring 소유 테이블에 기록한다.

테이블(`detection_jobs`, `detection_findings`)은 Spring이 만들고 소유한다. 작업 행도
Spring이 만들어 jobId와 함께 넘겨준다 — 이 서버는 **자기 jobId의 행을 갱신하고 결과를
넣기만** 한다. 행을 새로 만들지 않는다.

작업의 정체성이 Spring 쪽에 있는 이유는, 이 서버가 재시작해 진행 중이던 작업을 잃어도
"무엇을 언제 맡겼는지"가 남아 있어야 잃어버린 작업을 오류로 확정할 수 있기 때문이다.
"""

from __future__ import annotations

import json
import logging

from src.repository.postgres.client import connect

logger = logging.getLogger("postgres.detection")


def mark_running(job_id: str) -> None:
    """검사를 시작했다고 표시한다.

    재탐지는 Spring이 새 jobId로 새 행을 만들기 때문에, 이 서버가 보는 행은 항상
    QUEUED에서 시작한다 — 완료 상태를 되돌리는 경우가 없어 리셋할 것이 없다.
    """
    _execute(
        "UPDATE detection_jobs SET status='RUNNING', updated_at=now() WHERE job_id=%s",
        (job_id,),
    )


def save_result(job_id: str, claim_count: int, findings: list[dict]) -> None:
    """결과를 한 트랜잭션으로 기록한다.

    findings를 먼저 지우는 것은 방어가 아니라 **필수**다. 이 서버가 재시작하면 Spring은
    조회에서 404를 보고 같은 jobId로 다시 보내는데, 그때 이전 실행이 남긴 행이 있으면
    (job_id, seq) UNIQUE 제약이 INSERT를 거부한다.

    카운트는 status='DONE'과 함께 써야 한다 — 제약(ck_detection_jobs_counts_by_status)이
    DONE이 아닌 행에는 카운트를 못 넣게 막는다.
    """
    rows = [
        (
            job_id,
            i,
            f.get("quote") or "",
            f.get("axis") or "",
            str(f.get("value") or ""),
            json.dumps(f.get("lines") or []),
            json.dumps(f.get("cited") or []),
            f.get("reason") or "",
            f.get("score"),
        )
        for i, f in enumerate(findings, 1)
    ]
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM detection_findings WHERE job_id=%s", (job_id,))
            if rows:
                cur.executemany(
                    "INSERT INTO detection_findings "
                    "(job_id, seq, quote, axis, value, lines, cited, reason, score) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    rows,
                )
            cur.execute(
                "UPDATE detection_jobs SET status='DONE', claim_count=%s, "
                "contradiction_count=%s, completed_at=now(), updated_at=now() WHERE job_id=%s",
                (claim_count, len(findings), job_id),
            )
    except Exception:  # noqa: BLE001 — 저장 실패가 검사 결과를 지우면 안 된다
        logger.exception("탐지 결과 저장 실패 | job_id=%s", job_id)


def mark_error(job_id: str, detail: str) -> None:
    """실패를 기록한다. 카운트는 0/NULL이어야 제약을 통과한다."""
    _execute(
        "UPDATE detection_jobs SET status='ERROR', detail=%s, claim_count=NULL, "
        "contradiction_count=0, completed_at=now(), updated_at=now() WHERE job_id=%s",
        (detail, job_id),
    )


def _execute(sql: str, params: tuple) -> None:
    """상태 갱신 한 줄. 실패해도 예외를 올리지 않는다.

    DB 쓰기가 안 된다고 검사를 중단할 이유가 없다 — 결과는 메모리에 있고 조회로 받아갈 수
    있다. 행이 없어(UPDATE 0건) 조용히 지나가는 경우도 같은 이유로 경고만 남긴다.
    """
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.rowcount == 0:
                logger.warning("탐지 작업 행이 없다 | params=%s", params)
    except Exception:  # noqa: BLE001
        logger.exception("탐지 작업 상태 기록 실패 | params=%s", params)
