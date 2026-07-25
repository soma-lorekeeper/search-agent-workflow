# lorekeeper-poc 수정 기록

`lorekeeper-poc/`(이 폴더 안의 clone)는 다른 팀원의 레포라 내부 로직은 원칙적으로
건드리지 않는다. 그래도 실제로 막혀서 고쳐야 했던 것들을 여기 기록한다 — 무엇을,
왜, 어떻게 고쳤는지, 팀원 쪽 원본 레포에는 아직 반영 안 됨(로컬 clone에만 적용).

---

## 2026-07-25 — KSS 문장 분리가 긴 "문장"을 안 쪼개서 임베딩 실패

**파일**: `lorekeeper-poc/poc/src/splitters.py` (`_group_sentences`)

**증상**: `POST /api/index`로 `data/episode1.txt`(전지적 독자 시점 1화)를 인덱싱하면
500 에러. 서버 로그:

```
openai.BadRequestError: Error code: 400 - {'error': {'message':
"Invalid 'input': maximum context length is 8192 tokens.", ...}}
```

**원인**: `KSSSentenceSplitter`가 KSS(mecab 백엔드)로 문장을 나눈 뒤
`_group_sentences()`로 목표 글자 수(chunk_size=100)까지 묶는데, 이 함수는 **문장
하나가 이미 chunk_size보다 커도 쪼개지 않고 그대로 청크로 만든다**. 그런데 전지적
독자 시점 1화 앞부분에는 이런 블록이 있다:

```
<메인 시나리오 # 1 ―가치 증명>
분류 : 메인
난이도 : F
클리어 조건 : 하나 이상의 생명체를 죽이시오.
제한시간 : 30분
```

마침표로 끝나는 문장이 한참 없는 "시스템 메시지/스탯창" 형식(LitRPG 장르 특유의
서술)이라, KSS가 이 블록 전체를 문장 하나로 인식해버린다. 재현 스크립트로 확인:

```python
from lorekeeper.splitters import KSSSentenceSplitter
result = await KSSSentenceSplitter(chunk_size=100, chunk_overlap=0).run(text)
# 수정 전: 최대 청크 길이 13,047자 (chunk_size=100인데도)
```

그 13,047자짜리 청크를 그대로 임베딩(`text-embedding-3-small`, 최대 8,192 토큰)에
넣다가 실패한 것.

**수정**: `_group_sentences()`에 문장 하나가 `chunk_size`를 넘으면
`_split_oversized_sentence()`로 글자 수 기준 강제 분할하는 예외 처리 추가(문장 경계
정보가 없으니 overlap 없이 단순 슬라이싱). 기존 정상 케이스(문장이 chunk_size 이하)
동작은 그대로.

**검증**: 같은 재현 스크립트로 수정 후 재실행 — 최대 청크 길이 13,047자 → **104자**로
정상화(전체 132개 청크 중 100자 살짝 넘는 건 14개뿐, 전부 실제로 긴 정상 문장). 실제
`/api/index` 호출까지는 확인 안 함(비용 발생하는 실제 인덱싱 재시도는 사용자가 직접
진행).

**반영 범위**: 로컬 clone(`lorekeeper-poc/poc/src/splitters.py`)에만 적용. 팀원의
원본 레포에는 반영 안 됨 — 필요하면 팀원에게 알리거나 PR 고려.
