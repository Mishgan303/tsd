CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS marts_staging;

CREATE TABLE IF NOT EXISTS marts.story_dynamics (
    story_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    source_domain TEXT,
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    author_id TEXT,
    story_created_at TIMESTAMPTZ NOT NULL,
    snapshot_at TIMESTAMPTZ NOT NULL,
    age_hours NUMERIC NOT NULL CHECK (age_hours >= 0),
    score INTEGER NOT NULL CHECK (score >= 0),
    score_delta INTEGER,
    descendants INTEGER NOT NULL CHECK (descendants >= 0),
    descendants_delta INTEGER,
    loaded_comments_count INTEGER NOT NULL CHECK (loaded_comments_count >= 0),
    PRIMARY KEY (story_id, snapshot_at)
);

CREATE TABLE IF NOT EXISTS marts.current_story_ranking (
    story_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    source_domain TEXT,
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    author_id TEXT,
    story_created_at TIMESTAMPTZ NOT NULL,
    last_snapshot_at TIMESTAMPTZ NOT NULL,
    age_hours NUMERIC NOT NULL CHECK (age_hours >= 0),
    score INTEGER NOT NULL CHECK (score >= 0),
    score_delta INTEGER,
    descendants INTEGER NOT NULL CHECK (descendants >= 0),
    descendants_delta INTEGER,
    loaded_comments_count INTEGER NOT NULL CHECK (loaded_comments_count >= 0),
    score_rank BIGINT NOT NULL CHECK (score_rank >= 1),
    PRIMARY KEY (story_id)
);

CREATE TABLE IF NOT EXISTS marts.author_dynamics (
    user_id TEXT NOT NULL,
    user_created_at TIMESTAMPTZ,
    snapshot_at TIMESTAMPTZ NOT NULL,
    karma INTEGER NOT NULL,
    karma_delta INTEGER,
    submitted_count INTEGER NOT NULL CHECK (submitted_count >= 0),
    submitted_count_delta INTEGER,
    PRIMARY KEY (user_id, snapshot_at)
);

CREATE TABLE IF NOT EXISTS marts.topic_performance (
    snapshot_date DATE NOT NULL,
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    stories_count BIGINT NOT NULL CHECK (stories_count >= 0),
    authors_count BIGINT NOT NULL CHECK (authors_count >= 0),
    average_score NUMERIC NOT NULL CHECK (average_score >= 0),
    maximum_score INTEGER NOT NULL CHECK (maximum_score >= 0),
    average_comments NUMERIC NOT NULL CHECK (average_comments >= 0),
    total_score_growth BIGINT NOT NULL,
    total_comments_growth BIGINT NOT NULL,
    PRIMARY KEY (snapshot_date, topic)
);

CREATE TABLE IF NOT EXISTS marts.daily_activity (
    activity_date DATE NOT NULL,
    published_stories_count BIGINT NOT NULL CHECK (published_stories_count >= 0),
    created_comments_count BIGINT NOT NULL CHECK (created_comments_count >= 0),
    story_authors_count BIGINT NOT NULL CHECK (story_authors_count >= 0),
    comment_authors_count BIGINT NOT NULL CHECK (comment_authors_count >= 0),
    active_users_count BIGINT NOT NULL CHECK (active_users_count >= 0),
    PRIMARY KEY (activity_date)
);

CREATE TABLE IF NOT EXISTS marts_staging.story_dynamics (
    story_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    source_domain TEXT,
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    author_id TEXT,
    story_created_at TIMESTAMPTZ NOT NULL,
    snapshot_at TIMESTAMPTZ NOT NULL,
    age_hours NUMERIC NOT NULL CHECK (age_hours >= 0),
    score INTEGER NOT NULL CHECK (score >= 0),
    score_delta INTEGER,
    descendants INTEGER NOT NULL CHECK (descendants >= 0),
    descendants_delta INTEGER,
    loaded_comments_count INTEGER NOT NULL CHECK (loaded_comments_count >= 0),
    PRIMARY KEY (story_id, snapshot_at)
);

CREATE TABLE IF NOT EXISTS marts_staging.current_story_ranking (
    story_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    source_domain TEXT,
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    author_id TEXT,
    story_created_at TIMESTAMPTZ NOT NULL,
    last_snapshot_at TIMESTAMPTZ NOT NULL,
    age_hours NUMERIC NOT NULL CHECK (age_hours >= 0),
    score INTEGER NOT NULL CHECK (score >= 0),
    score_delta INTEGER,
    descendants INTEGER NOT NULL CHECK (descendants >= 0),
    descendants_delta INTEGER,
    loaded_comments_count INTEGER NOT NULL CHECK (loaded_comments_count >= 0),
    score_rank BIGINT NOT NULL CHECK (score_rank >= 1),
    PRIMARY KEY (story_id)
);

CREATE TABLE IF NOT EXISTS marts_staging.author_dynamics (
    user_id TEXT NOT NULL,
    user_created_at TIMESTAMPTZ,
    snapshot_at TIMESTAMPTZ NOT NULL,
    karma INTEGER NOT NULL,
    karma_delta INTEGER,
    submitted_count INTEGER NOT NULL CHECK (submitted_count >= 0),
    submitted_count_delta INTEGER,
    PRIMARY KEY (user_id, snapshot_at)
);

CREATE TABLE IF NOT EXISTS marts_staging.topic_performance (
    snapshot_date DATE NOT NULL,
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    stories_count BIGINT NOT NULL CHECK (stories_count >= 0),
    authors_count BIGINT NOT NULL CHECK (authors_count >= 0),
    average_score NUMERIC NOT NULL CHECK (average_score >= 0),
    maximum_score INTEGER NOT NULL CHECK (maximum_score >= 0),
    average_comments NUMERIC NOT NULL CHECK (average_comments >= 0),
    total_score_growth BIGINT NOT NULL,
    total_comments_growth BIGINT NOT NULL,
    PRIMARY KEY (snapshot_date, topic)
);

CREATE TABLE IF NOT EXISTS marts_staging.daily_activity (
    activity_date DATE NOT NULL,
    published_stories_count BIGINT NOT NULL CHECK (published_stories_count >= 0),
    created_comments_count BIGINT NOT NULL CHECK (created_comments_count >= 0),
    story_authors_count BIGINT NOT NULL CHECK (story_authors_count >= 0),
    comment_authors_count BIGINT NOT NULL CHECK (comment_authors_count >= 0),
    active_users_count BIGINT NOT NULL CHECK (active_users_count >= 0),
    PRIMARY KEY (activity_date)
);

CREATE TABLE IF NOT EXISTS marts_staging.story_topics (
    story_id BIGINT NOT NULL,
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    PRIMARY KEY (story_id)
);

-- Hacker News can return negative karma for penalized users. These ALTERs
-- also migrate databases created by an earlier version of this schema.
ALTER TABLE marts.author_dynamics
    DROP CONSTRAINT IF EXISTS author_dynamics_karma_check;
ALTER TABLE marts_staging.author_dynamics
    DROP CONSTRAINT IF EXISTS author_dynamics_karma_check;

CREATE INDEX IF NOT EXISTS ix_story_dynamics_topic_snapshot
    ON marts.story_dynamics (topic, snapshot_at);
CREATE INDEX IF NOT EXISTS ix_story_dynamics_author_snapshot
    ON marts.story_dynamics (author_id, snapshot_at);
CREATE INDEX IF NOT EXISTS ix_current_story_ranking_score_rank
    ON marts.current_story_ranking (score_rank);
CREATE INDEX IF NOT EXISTS ix_author_dynamics_user_snapshot
    ON marts.author_dynamics (user_id, snapshot_at);
CREATE INDEX IF NOT EXISTS ix_topic_performance_topic_date
    ON marts.topic_performance (topic, snapshot_date);

