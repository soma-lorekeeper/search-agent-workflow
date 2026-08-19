# Lorekeeper Chatting API Spec

| | |
|---|---|
| 버전 | v1 (2026-08-19) |
| 대상 독자 | Spring 서버 개발팀 |
| 범위 | **AI 채팅 API만.** 인덱싱은 `docs/indexing-api-spec.md`, 탐지는 `docs/detecting-api-spec.md` 참고 |

## 1. 개요

작가의 질문 하나에 근거 있는 답을 만든다. 에이전트가 지식 그래프(인물 상태·사건)와 원고 DB를 직접 조회해 답하고, 무엇을 찾아봤는지(`toolCalls`)를 함께 내려보낸다.

```
Spring ──POST /api/chat──▶ Python ──▶ Neo4j (KG 조회) / PostgreSQL (원고 조회) / OpenAI
       ◀──200 (답변 완성본)──┘
```

인덱싱·탐지와 동작 모델이 다르다.

| | 인덱싱·탐지 | 채팅 |
|---|---|---|
| 처리 방식 | 202 접수 후 비동기 + 폴링 | **동기** — 요청 안에서 끝까지 처리해 한 번에 응답 (스트리밍 아님) |
| 서버 상태 | 잡 상태를 메모리/DB에 유지 | **무상태** — 대화 기록을 서버에 남기지 않는다 |

대화 기록의 진실의 원천은 Spring의 `chat_messages` 테이블이다. 매 요청에 지금까지의 대화 전체를 실어 보내면 이 서버는 답만 만들어 준다 — 그래서 이 서버가 재시작해도 대화가 끊기지 않는다.

응답까지 걸리는 시간은 도구 호출 횟수에 비례한다(0~5회, 모델이 스스로 결정). 잡담이면 수 초, KG를 여러 번 뒤지는 질문이면 수십 초까지 걸릴 수 있으므로 호출 측 HTTP 타임아웃을 넉넉히(권장 120초) 잡는다.

## 2. 공통 규약

- **소설 식별**: `userId` × `workId` 조합이 소설 한 편을 unique하게 구분한다(인덱싱·탐지와 같은 규약). 에이전트의 KG·원고 조회가 전부 이 테넌트 키로 격리되므로, **인덱싱할 때와 같은 값을 보내야 그 그래프를 본다.** 도구 스키마에는 이 값이 일부러 없다 — 모델이 남의 작품 번호를 지어내 조회할 여지를 막기 위해 서버가 요청 값을 직접 주입한다.
- **필드 표기**: camelCase — 요청·응답 전 필드. 이 서버의 모든 API(indexing·detecting·chat) 공통 규약이다
- **에러 본문**: [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457) — `{ "type", "title", "status", "detail", ...확장 }` + `Content-Type: application/problem+json`. 상세 스키마와 `type` 목록은 `docs/indexing-api-spec.md` 2.1과 공통이다
- **인증**: 없음(인덱싱·탐지와 동일한 전제 — 내부 서버)

## 3. `POST /api/chat` — 대화 한 턴

### Request

```json
{
  "userId": 42,
  "workId": 7,
  "sessionId": 3,
  "messages": [
    { "role": "user", "content": "서진우가 처음 각성한 게 몇 화였지?" },
    { "role": "assistant", "content": "3화입니다. …" },
    { "role": "user", "content": "그때 옆에 누가 있었어?" }
  ],
  "context": {
    "editingEpisode": { "number": 6, "title": "추격", "text": "6화 원고 전문…" },
    "viewingEpisodeNumber": 5
  }
}
```

| 필드 | 타입 | 설명 |
|---|---|---|
| `userId` | int | 소설 소유자 |
| `workId` | int | 작품 |
| `sessionId` | int | 채팅 세션 식별자. 서버는 로깅·추적에만 쓴다(세션 상태를 들고 있지 않다) |
| `messages` | array | 지금까지의 **대화 전체**. 마지막 항목이 이번에 답할 사용자 발화 |
| `messages[].role` | string | `user` 또는 `assistant`. 그 외 값은 `user`로 취급된다 |
| `messages[].content` | string | 발화 내용 |
| `context` | object | 회차 컨텍스트. **생략 가능** — 편집기를 열지 않고 질문만 하는 경우도 성립한다 |
| `context.editingEpisode` | object \| null | 집필 중인 회차. `number`(int, DRAFT면 null) / `title` / `text`(원고 **전문**, 발췌 아님) |
| `context.viewingEpisodeNumber` | int \| null | 화면에 열어 둔 회차 번호 |

**회차에 얽힌 개념은 셋인데 요청에 실리는 건 둘뿐이다.** 셋째인 "인덱싱된 회차"(조회 가능한 범위)는 일부러 계약에 없다 — 그건 Neo4j 그래프의 사실이고, 요청이 들고 온 값은 인덱싱이 진행되는 동안 곧바로 낡는다. 에이전트가 매 턴 그래프에 직접 묻는다. 요청에 계약 밖 필드(예: `indexedEpisodes`)를 실어도 무시된다.

`editingEpisode.text`에 전문을 싣는 것은 의도된 설계다 — 지금 고쳐 쓰는 회차만큼은 모델이 도구를 거치지 않고 통째로 읽는다. 그 대가로 요청이 커지므로 전문은 이 필드에만 싣는다.

### Response `200 OK`

```json
{
  "content": "3화 각성 장면에는 한서연이 함께 있었습니다. …",
  "toolCalls": [
    { "name": "fact_search", "summary": "'서진우 각성' 사실 3건 조회", "status": "DONE" }
  ],
  "suggestedTitle": null
}
```

| 필드 | 타입 | 설명 |
|---|---|---|
| `content` | string | 답변 본문(자연어) |
| `toolCalls` | array | 이번 턴에 에이전트가 실제로 호출한 도구 내역(호출 순). 프론트가 "무엇을 찾아봤는지"를 보여주는 용도다 — 근거 없는 답변처럼 보이지 않게 하는 게 목적. 잡담이면 빈 배열 |
| `toolCalls[].name` | string | 도구 이름. 아래 도구 목록의 어휘 |
| `toolCalls[].summary` | string | 무엇을 조회했는지 한 줄 요약(한국어, 사람이 읽는 용도) |
| `toolCalls[].status` | string | `DONE` \| `FAILED`. 도구가 실패해도 대화는 계속되고(근거 부족으로 답함) 여기 FAILED로 남는다 |
| `suggestedTitle` | string \| null | 세션 제목 제안(최대 20자). **대화 첫 턴에만** 채워진다 — `messages` 길이가 2 이하일 때. 이후 턴과 제목 생성 실패 시에는 null. 저장할지 말지는 Spring이 정한다 |

도구는 5종이고 한 턴에 **최대 5회** 호출된다(초과분은 모델이 지금까지의 근거로 답하도록 유도된다).

> **이름 변경(파괴적).** KG 3종의 이름에서 `kg_` 접두가 빠졌다(`kg_hybrid_search` → `hybrid_search` 등) — retrieval 계층과 어휘를 통일했다. `toolCalls[].name`으로 도구별 아이콘·문구를 매핑하는 프론트가 있다면 대응이 필요하다.

| `toolCalls[].name` | 하는 일 |
|---|---|
| `hybrid_search` | 벡터+풀텍스트 결합 검색으로 원문 조각과 관련 그래프 반환 |
| `fact_search` | 정제된 사실(사건·인물 상태)을 검색. 근거 원문이 딸려온다 |
| `entity_search` | 인물·아이템·조직·장소 하나를 이름으로 정확 조회 |
| `episode_manuscript` | 회차 제목·원고 본문(앞부분)을 원고 DB에서 그대로 조회 |
| `work_settings` | 작품 기본 정보(제목) 조회 |

### Response `422 Unprocessable Entity` — 스키마 불일치

요청 본문이 계약과 다르다(필수 필드 누락, 타입 오류). Spring 쪽 요청 조립 버그를 뜻한다. 본문은 problem+json이고 pydantic 오류 배열이 `errors` 확장 필드에 실린다(`docs/indexing-api-spec.md` 2.1).

### Response `500 Internal Server Error`

잡히지 않은 서버 오류 — LLM 호출이 재시도 후에도 실패한 경우(크레딧 소진 포함)가 대표적이다. `detail`은 고정 문구 `"Internal server error"`이고 내부 메시지는 노출되지 않는다(로그로만 남는다).

**429는 없다.** 인덱싱·탐지와 달리 채팅에는 접수 게이트가 없다 — LLM 호출이 몰리면 내부 관문(동시 호출 상한)에서 거절이 아니라 **대기**하므로, 호출자에게는 그냥 응답이 느려지는 것으로 보인다.

## 4. 예시 시나리오

### 4.1 첫 턴 — 제목 제안

```
Spring: POST /api/chat {userId:42, workId:7, sessionId:3,
                        messages:[{role:"user", content:"서진우 소개해줘"}]}
Python: 200 {content:"서진우는 …", toolCalls:[{name:"entity_search", …, status:"DONE"}],
             suggestedTitle:"서진우 인물 소개"}
        → Spring이 세션 제목으로 저장할지 결정
```

### 4.2 이후 턴 — 대화 전체를 다시 실어 보낸다

```
Spring: POST /api/chat {…, messages:[유저, AI, 유저, AI, 새 질문]}   ← 기록 전체
Python: 200 {content:"…", toolCalls:[…], suggestedTitle:null}       ← 제목은 다시 짓지 않음
```

### 4.3 도구 실패 — 대화는 계속된다

```
(Neo4j 순단 등으로 조회 실패)
Python: 200 {content:"자료를 확인할 수 없어 …", 
             toolCalls:[{name:"hybrid_search", summary:"hybrid_search 조회 실패", status:"FAILED"}],
             suggestedTitle:null}
```

5xx가 아니라 200이다 — 도구 실패는 "근거 부족"으로 취급해 답변에 반영하고, 실패 사실은 `toolCalls[].status`로 드러낸다.
