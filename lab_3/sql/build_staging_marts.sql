TRUNCATE TABLE
    marts_staging.story_dynamics,
    marts_staging.current_story_ranking,
    marts_staging.author_dynamics,
    marts_staging.topic_performance,
    marts_staging.daily_activity;

INSERT INTO marts_staging.story_dynamics (
    story_id,
    title,
    url,
    source_domain,
    topic,
    author_id,
    story_created_at,
    snapshot_at,
    age_hours,
    score,
    score_delta,
    descendants,
    descendants_delta,
    loaded_comments_count
)
SELECT
    story.story_id,
    story.title,
    story.url,
    CASE
        WHEN story.url IS NULL THEN NULL
        ELSE NULLIF(
            split_part(
                split_part(
                    regexp_replace(
                        story.url,
                        '^[A-Za-z][A-Za-z0-9+.-]*://',
                        ''
                    ),
                    '/',
                    1
                ),
                ':',
                1
            ),
            ''
        )
    END AS source_domain,
    topics.topic,
    author.user_id AS author_id,
    story.created_at AS story_created_at,
    snapshot.source_loaded_at AS snapshot_at,
    GREATEST(
        EXTRACT(EPOCH FROM (snapshot.source_loaded_at - story.created_at)) / 3600,
        0
    ) AS age_hours,
    snapshot.score,
    snapshot.score_delta,
    snapshot.descendants,
    snapshot.descendants_delta,
    snapshot.loaded_comments_count
FROM dds.dim_story AS story
JOIN dds.fact_story_snapshot AS snapshot
    ON snapshot.story_sk = story.story_sk
JOIN marts_staging.story_topics AS topics
    ON topics.story_id = story.story_id
LEFT JOIN dds.dim_user AS author
    ON author.user_sk = story.author_user_sk;

INSERT INTO marts_staging.current_story_ranking (
    story_id,
    title,
    url,
    source_domain,
    topic,
    author_id,
    story_created_at,
    last_snapshot_at,
    age_hours,
    score,
    score_delta,
    descendants,
    descendants_delta,
    loaded_comments_count,
    score_rank
)
WITH latest_per_story AS (
    SELECT
        dynamics.*,
        ROW_NUMBER() OVER (
            PARTITION BY story_id
            ORDER BY snapshot_at DESC
        ) AS latest_row
    FROM marts_staging.story_dynamics AS dynamics
),
ranked AS (
    SELECT
        latest_per_story.*,
        RANK() OVER (ORDER BY score DESC, story_id) AS score_rank
    FROM latest_per_story
    WHERE latest_row = 1
)
SELECT
    story_id,
    title,
    url,
    source_domain,
    topic,
    author_id,
    story_created_at,
    snapshot_at,
    age_hours,
    score,
    score_delta,
    descendants,
    descendants_delta,
    loaded_comments_count,
    score_rank
FROM ranked;

INSERT INTO marts_staging.author_dynamics (
    user_id,
    user_created_at,
    snapshot_at,
    karma,
    karma_delta,
    submitted_count,
    submitted_count_delta
)
SELECT
    users.user_id,
    users.created_at,
    snapshots.source_loaded_at,
    snapshots.karma,
    snapshots.karma_delta,
    snapshots.submitted_count,
    snapshots.submitted_count_delta
FROM dds.dim_user AS users
JOIN dds.fact_user_snapshot AS snapshots
    ON snapshots.user_sk = users.user_sk;

INSERT INTO marts_staging.topic_performance (
    snapshot_date,
    topic,
    stories_count,
    authors_count,
    average_score,
    maximum_score,
    average_comments,
    total_score_growth,
    total_comments_growth
)
WITH daily_snapshots AS (
    SELECT
        dynamics.*,
        (snapshot_at AT TIME ZONE 'UTC')::date AS snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY
                story_id,
                (snapshot_at AT TIME ZONE 'UTC')::date
            ORDER BY snapshot_at DESC
        ) AS latest_row
    FROM marts_staging.story_dynamics AS dynamics
),
daily_state AS (
    SELECT
        snapshot_date,
        topic,
        COUNT(*) AS stories_count,
        COUNT(DISTINCT author_id) AS authors_count,
        AVG(score) AS average_score,
        MAX(score) AS maximum_score,
        AVG(descendants) AS average_comments
    FROM daily_snapshots
    WHERE latest_row = 1
    GROUP BY snapshot_date, topic
),
daily_growth AS (
    SELECT
        (snapshot_at AT TIME ZONE 'UTC')::date AS snapshot_date,
        topic,
        SUM(COALESCE(score_delta, 0)) AS total_score_growth,
        SUM(COALESCE(descendants_delta, 0)) AS total_comments_growth
    FROM marts_staging.story_dynamics
    GROUP BY (snapshot_at AT TIME ZONE 'UTC')::date, topic
)
SELECT
    state.snapshot_date,
    state.topic,
    state.stories_count,
    state.authors_count,
    state.average_score,
    state.maximum_score,
    state.average_comments,
    growth.total_score_growth,
    growth.total_comments_growth
FROM daily_state AS state
JOIN daily_growth AS growth
    ON growth.snapshot_date = state.snapshot_date
   AND growth.topic = state.topic;

INSERT INTO marts_staging.daily_activity (
    activity_date,
    published_stories_count,
    created_comments_count,
    story_authors_count,
    comment_authors_count,
    active_users_count
)
WITH activity_dates AS (
    SELECT (created_at AT TIME ZONE 'UTC')::date AS activity_date
    FROM dds.dim_story
    UNION
    SELECT (created_at AT TIME ZONE 'UTC')::date AS activity_date
    FROM dds.fact_comment
),
story_activity AS (
    SELECT
        (created_at AT TIME ZONE 'UTC')::date AS activity_date,
        COUNT(*) AS published_stories_count,
        COUNT(DISTINCT author_user_sk) AS story_authors_count
    FROM dds.dim_story
    GROUP BY (created_at AT TIME ZONE 'UTC')::date
),
comment_activity AS (
    SELECT
        (created_at AT TIME ZONE 'UTC')::date AS activity_date,
        COUNT(*) AS created_comments_count,
        COUNT(DISTINCT author_user_sk) AS comment_authors_count
    FROM dds.fact_comment
    GROUP BY (created_at AT TIME ZONE 'UTC')::date
),
active_users AS (
    SELECT
        activity_date,
        COUNT(*) AS active_users_count
    FROM (
        SELECT
            (created_at AT TIME ZONE 'UTC')::date AS activity_date,
            author_user_sk
        FROM dds.dim_story
        WHERE author_user_sk IS NOT NULL
        UNION
        SELECT
            (created_at AT TIME ZONE 'UTC')::date AS activity_date,
            author_user_sk
        FROM dds.fact_comment
        WHERE author_user_sk IS NOT NULL
    ) AS unique_daily_users
    GROUP BY activity_date
)
SELECT
    dates.activity_date,
    COALESCE(stories.published_stories_count, 0),
    COALESCE(comments.created_comments_count, 0),
    COALESCE(stories.story_authors_count, 0),
    COALESCE(comments.comment_authors_count, 0),
    COALESCE(users.active_users_count, 0)
FROM activity_dates AS dates
LEFT JOIN story_activity AS stories USING (activity_date)
LEFT JOIN comment_activity AS comments USING (activity_date)
LEFT JOIN active_users AS users USING (activity_date);
