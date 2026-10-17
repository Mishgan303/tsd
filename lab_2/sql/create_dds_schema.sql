CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dim_user (
    user_sk BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ,
    dds_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (btrim(user_id) <> '')
);

CREATE TABLE IF NOT EXISTS dds.dim_user_profile_history (
    user_profile_sk BIGSERIAL PRIMARY KEY,
    user_sk BIGINT NOT NULL REFERENCES dds.dim_user(user_sk),
    about TEXT,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    UNIQUE (user_sk, valid_from),
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_user_profile_open
    ON dds.dim_user_profile_history (user_sk)
    WHERE valid_to IS NULL;

CREATE TABLE IF NOT EXISTS dds.fact_user_snapshot (
    user_sk BIGINT NOT NULL REFERENCES dds.dim_user(user_sk),
    source_loaded_at TIMESTAMPTZ NOT NULL,
    karma INTEGER NOT NULL,
    karma_delta INTEGER,
    submitted_count INTEGER NOT NULL CHECK (submitted_count >= 0),
    submitted_count_delta INTEGER,
    dds_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_sk, source_loaded_at)
);

CREATE TABLE IF NOT EXISTS dds.dim_story (
    story_sk BIGSERIAL PRIMARY KEY,
    story_id BIGINT NOT NULL UNIQUE,
    author_user_sk BIGINT REFERENCES dds.dim_user(user_sk),
    title TEXT NOT NULL,
    url TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    dds_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (btrim(title) <> '')
);

CREATE INDEX IF NOT EXISTS ix_dim_story_author
    ON dds.dim_story (author_user_sk);

CREATE TABLE IF NOT EXISTS dds.fact_story_snapshot (
    story_sk BIGINT NOT NULL REFERENCES dds.dim_story(story_sk),
    source_loaded_at TIMESTAMPTZ NOT NULL,
    score INTEGER NOT NULL CHECK (score >= 0),
    score_delta INTEGER,
    descendants INTEGER NOT NULL CHECK (descendants >= 0),
    descendants_delta INTEGER,
    loaded_comments_count INTEGER NOT NULL CHECK (loaded_comments_count >= 0),
    bad_word_comments_count INTEGER NOT NULL
        CHECK (bad_word_comments_count >= 0),
    dds_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (story_sk, source_loaded_at)
);

CREATE TABLE IF NOT EXISTS dds.fact_comment (
    comment_id BIGINT PRIMARY KEY,
    story_sk BIGINT NOT NULL REFERENCES dds.dim_story(story_sk),
    author_user_sk BIGINT REFERENCES dds.dim_user(user_sk),
    text TEXT,
    bad_word BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL,
    dds_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_comment_story
    ON dds.fact_comment (story_sk);

CREATE TABLE IF NOT EXISTS dds.pipeline_state (
    source_name TEXT PRIMARY KEY,
    last_processed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_name IN ('stories', 'users', 'comments'))
);

INSERT INTO dds.pipeline_state (source_name, last_processed_at)
VALUES ('stories', NULL), ('users', NULL), ('comments', NULL)
ON CONFLICT (source_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS dds.data_quality_results (
    run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status BOOLEAN NOT NULL,
    actual_value TEXT NOT NULL,
    expected_rule TEXT NOT NULL,
    details TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);
