# Claim 추출 → 검색 도달 평가 스크립트 (scripts/eval_claims.py)

## Context

재편된 3종 retriever(hybrid/fact/entity, 합집합 15/15 실측) 위에 올릴 claim 파이프라인의 앞 두 단계를 검증한다. 신 claim 스키마(subject/attribute/value/claim_type/context)와 **결정적 라우팅**(LLM이 도구를 고르지 않고 claim_type이 채널을 결정)이 실제로 커버리지·도달률을 내는지 측정하고, ablation 3종으로 추출 비용 최적화 여지를 확인한다. **judge 판정은 이번 범위에서 제외** — 전 variant를 ①추출 커버리지 + ②검색 도달률까지만 측정한다(사용자 확정).

- 추출 모델: **gpt-5.6-luna, reasoning_effort 파라미터 안 넘김**(API 기본값). 기계적 형태 판정이라는 가설. 결과가 나쁘면(합격 기준 미달) `--extract-effort medium`으로 승격 재실행.
- 규모 전제: 회차 5,000자 내외 ≈ 3.2k tok (실측 0.64 tok/char). TPM 200k 대비 추출 전량 병렬도 무해 — 세마포어·워밍 불필요.

## 산출물 (src 무수정)

- `scripts/eval_claims.py` **단일 파일 신규** — 프롬프트·few-shot·라우팅·채점 전부 포함. `src/`와 lorekeeper는 손대지 않는다.
- `data/eval_claims/` — 스테이지별 아티팩트 + `cache/`(LLM 응답 디스크 캐시).

## 재사용하는 기존 코드

| 무엇 | 어디서 |
|---|---|
| 도구 실행 | `src/contradiction/tools.py` `build_openai_tools()` → `tools_by_name[name].execute(**args)` + `format_tool_result()` (agent.py:150-161과 동일 경로) |
| 청크 분할 | `lorekeeper.splitters.KSSSentenceSplitter(chunk_size=N, chunk_overlap=0)` (pipeline.py:125 패턴) |
| 토큰 집계 | `src/contradiction/usage.py` `from_response()`/`merge()` — cached/reasoning 분리 집계 |
| qrel | `data/eval_retrieval/markers.json` (동결) — 매칭 규약: **공백 정규화(`"".join(s.split())`) 후 부분 문자열** |
| GT | `data/ch6_test_draft*.gt.json` 5개 (errors[].quote / excluded / v3의 hard_negatives) |

## 1. 추출 (stage: extract)

프롬프트 구조 (배경 컨텍스트 없음 — 형태 기준만):

```
system: [추출 기준 4종(절대 기준, 개수 목표 없음)] ← 정적
        [few-shot 7예시]                         ← 정적 (교차-초고 캐시를 위해 전문보다 앞)
        [원고 전문]                               ← 초고마다 변함 (대명사 해소·context 적재 원천)
user:   [청크]  ← "이 범위 안에서만 뽑아라"
```

- 절대 기준 4종: 속성값 특정 / 행위 주체 특정 / 규칙·수치 언급 / 생사·소유·상태 전제. "기준에 걸리는 서술은 빠짐없이". 폭주 안전판만 청크당 10개.
- claim 스키마: `{quote(원문 그대로), subject(대명사 해소된 이름), attribute, value, claim_type(속성값|행위귀속|규칙제약|상태존재), category(기존 GT 분류 호환), context(원고 내부 관련 서술 요약 — v5-GT3류 원고 내부 모순의 유일한 운반로)}`
- **행위귀속의 필드 의미**: attribute=행위구(주어 제외), value=초고가 주장한 주체 **그대로**(틀린 주어도 교정 금지 — few-shot으로 못 박음).
- few-shot 7예시: 전부 **지어낸 가상 소설**(테스트 소설에서 따오면 평가 오염). 기준 4종 각 1 + 대명사 해소·context 적재 + `"claims": []` 0개 케이스 + 주관 평가 음성. 밀도 편차(3/1/0개) 포함 — 개수 앵커링 방지.
- 호출: AsyncOpenAI chat.completions, `response_format=json_object`, `prompt_cache_key="gyeol-eval-claims-extract"`. reasoning_effort는 `--extract-effort` 지정 시에만 kwargs에 포함(pipeline.py:104 패턴).
- 캐시: `data/eval_claims/cache/{sha1(model|effort|system|user)}.json` — 프롬프트 수정 루프에서 미변경 호출 재사용.

## 2. 라우팅·검색 (stage: retrieve) — LLM 없음

claim_type별 고정 라우팅(코드), `--top-k` 기본 3:

| claim_type | 호출 |
|---|---|
| 속성값 | `entity_search(subject)` + `hybrid_search(f"{subject} {attribute}")` |
| 행위귀속 | `fact_search(attribute)` — **주어 없이 행위로** (틀린 주어로 검색하면 반박 근거에 못 감) |
| 규칙제약 | `hybrid_search(f"{attribute} {value}")` + `fact_search(attribute)` |
| 상태존재 | `entity_search(subject)` + `fact_search(f"{subject} {attribute}")` |

- subject가 없으면(해소 실패) entity_search는 생략하고 나머지 채널만.
- 결과는 `format_tool_result()` 텍스트를 채널별로 그대로 저장(마커 매칭은 무가공 원문 전제).
- retrieve는 추출 결과에 의존하므로 **variant마다 재실행**. 비용은 질의 임베딩뿐이라 미미.

## 3. 채점 (stage: score) — LLM 없음

- **① 추출 커버리지**: GT errors[].quote와 claim.quote를 norm() 후 **양방향 부분 문자열**로 매칭. 어느 쪽도 포함 관계가 아니면 미검출로 세되, 사람 확인용으로 (GT, 최근접 claim) 목록을 리포트에 출력.
- **② 검색 도달률**: GT에 매칭된 claim(복수면 evidence 합집합)의 검색 결과 텍스트에 markers.json 마커가 norm() 부분일치로 나타나면 HIT. `v5-GT3(unreachable)`은 도달률 분모에서 제외하고, 대신 **context 필드가 원고 앞부분 근거(스킬 발동 메시지)를 실었는지**를 별도 항목으로 기록.
- **부가 지표**: 총 claim 수 / GT 비매칭 claim 수(밀도), hard_negatives·excluded가 claim으로 뽑힌 수(참고용 — 뽑히는 것 자체는 정상, judge 단계 대비 기록만), variant별 토큰·호출 수(usage 합산).
- 리포트: 버전(v1~v5) × 지표 표 + variant 비교표. **v1→v5 단조 비열화** 확인(깨지면 난이도 외 축의 결함 신호 — ch6_test_draft_errors.md:96).

## Variants (전부 ①②까지)

| variant | 청킹 | system의 전문 | 추출 범위 |
|---|---|---|---|
| `baseline` | 2,500자 | 있음 | 청크 |
| `half-chunk` | **1,250자** | 있음 | 청크 |
| `no-draft` | 2,500자 | **없음** | 청크 |
| `whole` | **없음** | (user가 전문) | 전문 1회 호출 |

예상 관전 포인트: half-chunk는 커버리지↑ 여부, no-draft는 v4(subject 해소)·v5(context 적재) 붕괴 여부, whole은 5k 규모에서 열화가 실재하는지 + 최저 비용.

## CLI

```
eval_claims.py --stage {extract,retrieve,score,all} --variant {baseline,half-chunk,no-draft,whole}
               --drafts v1,v2,v3,v4,v5 (기본 전부) --top-k 3 --extract-effort (기본 미지정)
```

아티팩트: `data/eval_claims/claims_{variant}_{draft}.json`, `evidence_{variant}_{draft}.json`, `report_{variant}.md`

## 합격 기준·승격 규칙

- baseline 커버리지 **≥ 23/25** (v1~v3만 보면 ≥ 14/15). 미달 시 luna를 `--extract-effort medium`으로 승격 재실행 → 그래도 미달이면 half-chunk 결과와 교차 확인 후 보고.
- 도달률(자동 질의): v1~v3 **≥ 13/15** (과거 수동 질의 합집합 15/15가 상한 참고치). v4·v5는 최초 측정이라 기준선 수립이 목적.
- 최종 보고: variant별 (커버리지, 도달률, 초고당 input/cached/output 토큰) 비교표 + 권장 구성 결론.

## 검증 실행 순서

```bash
# 1. 베이스라인 풀 런
PYTHONPATH=. .venv/bin/python scripts/eval_claims.py --stage all --variant baseline
# 2. 커버리지 미달 시에만: effort 승격 재실행
PYTHONPATH=. .venv/bin/python scripts/eval_claims.py --stage all --variant baseline --extract-effort medium
# 3. ablation 3종
PYTHONPATH=. .venv/bin/python scripts/eval_claims.py --stage all --variant half-chunk
PYTHONPATH=. .venv/bin/python scripts/eval_claims.py --stage all --variant no-draft
PYTHONPATH=. .venv/bin/python scripts/eval_claims.py --stage all --variant whole
# 4. 비교표 확인: data/eval_claims/report_*.md
```

## 함정 메모

- few-shot을 테스트 소설(ch1~6·GT 초고)에서 따오지 말 것 — 평가 오염.
- 도구 결과의 근거 원문은 무가공이어야 마커 매칭이 성립(재편 때 확정된 포맷 계약 — 건드리지 않음).
- `whole` variant의 청크당 상한(10개)은 전문 기준으로 환산(예: 40개)해야 안전판이 추출을 자르지 않는다.
- gpt-5.6-luna가 `reasoning_effort` 미지정을 어떻게 받는지는 첫 호출에서 확인 — 파라미터 자체를 kwargs에서 빼는 방식(pipeline.py:102-105와 동일)이라 API 기본값 적용.
