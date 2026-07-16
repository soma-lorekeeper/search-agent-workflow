CREATE TABLE IF NOT EXISTS entities (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    episode_introduced INT,
    description TEXT,
    UNIQUE KEY uniq_name_type (name, type)
);

CREATE TABLE IF NOT EXISTS facts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    subject_id INT NOT NULL,
    predicate VARCHAR(100) NOT NULL,
    object_id INT NULL,
    object_text VARCHAR(500) NULL,
    episode INT NOT NULL,
    valid_from_episode INT,
    valid_until_episode INT,
    note TEXT,
    FOREIGN KEY (subject_id) REFERENCES entities(id),
    FOREIGN KEY (object_id) REFERENCES entities(id)
);

CREATE TABLE IF NOT EXISTS episode_summaries (
    episode INT PRIMARY KEY,
    summary TEXT
);
