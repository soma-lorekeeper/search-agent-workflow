-- MySQL은 "기본정보"(화별 원문/요약)만 보관한다. 인물/아이템/관계 등 그래프 성격 데이터는
-- 전부 Neo4j(graphdb.py)에만 있다 — 예전처럼 MySQL과 Neo4j에 이중 저장하지 않는다.
CREATE TABLE IF NOT EXISTS episodes (
    episode INT PRIMARY KEY,
    title VARCHAR(255),
    raw_text LONGTEXT NOT NULL,
    summary TEXT,
    indexed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
