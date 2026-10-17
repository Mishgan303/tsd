TRUNCATE TABLE marts.story_dynamics;
TRUNCATE TABLE marts.current_story_ranking;
TRUNCATE TABLE marts.author_dynamics;
TRUNCATE TABLE marts.topic_performance;
TRUNCATE TABLE marts.daily_activity;

INSERT INTO marts.story_dynamics (
    story_id, title, url, source_domain, topic, author_id,
    story_created_at, snapshot_at, age_hours, score, score_delta,
    descendants, descendants_delta, loaded_comments_count
)
SELECT
    story_id, title, url, source_domain, topic, author_id,
    story_created_at, snapshot_at, age_hours, score, score_delta,
    descendants, descendants_delta, loaded_comments_count
FROM marts_staging.story_dynamics;

INSERT INTO marts.current_story_ranking (
    story_id, title, url, source_domain, topic, author_id,
    story_created_at, last_snapshot_at, age_hours, score, score_delta,
    descendants, descendants_delta, loaded_comments_count, score_rank
)
SELECT
    story_id, title, url, source_domain, topic, author_id,
    story_created_at, last_snapshot_at, age_hours, score, score_delta,
    descendants, descendants_delta, loaded_comments_count, score_rank
FROM marts_staging.current_story_ranking;

INSERT INTO marts.author_dynamics (
    user_id, user_created_at, snapshot_at, karma, karma_delta,
    submitted_count, submitted_count_delta
)
SELECT
    user_id, user_created_at, snapshot_at, karma, karma_delta,
    submitted_count, submitted_count_delta
FROM marts_staging.author_dynamics;

INSERT INTO marts.topic_performance (
    snapshot_date, topic, stories_count, authors_count, average_score,
    maximum_score, average_comments, total_score_growth,
    total_comments_growth
)
SELECT
    snapshot_date, topic, stories_count, authors_count, average_score,
    maximum_score, average_comments, total_score_growth,
    total_comments_growth
FROM marts_staging.topic_performance;

INSERT INTO marts.daily_activity (
    activity_date, published_stories_count, created_comments_count,
    story_authors_count, comment_authors_count, active_users_count
)
SELECT
    activity_date, published_stories_count, created_comments_count,
    story_authors_count, comment_authors_count, active_users_count
FROM marts_staging.daily_activity;
