"""원고를 라인 번호(L1~LN) 단위로 쪼개고 렌더한다.

라인 번호는 추출기가 "이 claim의 근거가 어느 줄인가"를 밝히는 좌표이고, 화면이 원고 위에
하이라이트를 거는 좌표이기도 하다. **번호는 원고 전체에 한 번만 부여한다** — 조각으로
나눠 추출하더라도 조각마다 1부터 다시 매기면 좌표가 조각 안에서만 유효해져, 합친 뒤에는
어느 줄을 가리키는지 알 수 없게 된다.

전부 평가 하네스(scripts/eval_claims.py)에서 확정한 구현 그대로다.
"""

from __future__ import annotations

# 한 줄의 최대 글자 수. 이보다 길면 글자 수로 강제 분할한다.
LINE_MAX = 200


def split_lines(text: str, max_len: int = LINE_MAX) -> list[str]:
    """원고를 라인 번호(L1~LN)의 단위로 쪼갠다. KSS → 개행 → 글자 수 3단.

    세 단계가 각자 다른 실패를 메운다.
      1. KSS   — 마침표로 끝나는 서술을 문장 단위로 정확히 자른다. 개행만 쓰면
                 "빨리들 찾는 게 좋을 겁니다. 이제 3분밖에 안 남았으니까."가 한 줄로 남는다.
      2. 개행  — KSS는 마침표 없이 이어지는 시스템 메시지·스탯창 블록을 문장 하나로 묶는다
                 (실측 최대 2,859자). 그 덩어리는 실제로는 개행으로 잘 나뉜다.
      3. 글자수 — 위 둘로도 안 잘리는 초장문의 백스톱. 실측에서는 한 번도 발동하지 않았다
                 (개행 분할 후 최대 128자). 단어 중간에서 끊기므로 최후 수단이다.

    KSSSentenceSplitter는 재사용할 수 없다 — 문장을 chunk_size까지 이어 붙여 청크를 만드는
    (`_group_sentences`) 것이 목적이라 문장 목록 자체를 돌려주지 않는다.
    """
    # kss는 import 비용이 커서 호출 시점에 지연 import한다(splitters.py와 같은 방식).
    # backend="mecab" 명시가 필수 — 기본값 auto는 mecab 미설치 시 순수 파이썬 pecab로
    # 조용히 폴백해 수백 배 느려진다.
    import kss

    out: list[str] = []
    for sentence in kss.split_sentences(text, backend="mecab"):
        for piece in sentence.split("\n"):
            piece = piece.strip()
            if not piece:
                continue
            if len(piece) <= max_len:
                out.append(piece)
            else:
                out += [piece[i : i + max_len] for i in range(0, len(piece), max_len)]
    return out


def number_lines(lines: list[str]) -> str:
    """라인 목록을 추출·판정 프롬프트에 실을 `L1: …` 형태로 렌더한다."""
    return "\n".join(f"L{i}: {s}" for i, s in enumerate(lines, 1))
