-- Примеры запросов к DDS второй лабораторной.
-- Значения 101 и 'alice' замените на существующие story_id и user_id.

-- 1. Рост рейтинга и числа комментариев выбранной публикации.
SELECT
    story.story_id,
    story.title,
    snapshot.source_loaded_at,
    snapshot.score,
    snapshot.score_delta,
    snapshot.descendants,
    snapshot.descendants_delta,
    snapshot.loaded_comments_count,
    snapshot.bad_word_comments_count
FROM dds.dim_story AS story
JOIN dds.fact_story_snapshot AS snapshot USING (story_sk)
WHERE story.story_id = 101
ORDER BY snapshot.source_loaded_at;

-- 2. Последнее состояние авторов с karma >= 300.
WITH latest_user_snapshot AS (
    SELECT DISTINCT ON (user_sk)
        user_sk,
        source_loaded_at,
        karma,
        karma_delta,
        submitted_count,
        submitted_count_delta
    FROM dds.fact_user_snapshot
    ORDER BY user_sk, source_loaded_at DESC
)
SELECT
    users.user_id,
    snapshot.source_loaded_at,
    snapshot.karma,
    snapshot.karma_delta,
    snapshot.submitted_count,
    snapshot.submitted_count_delta
FROM latest_user_snapshot AS snapshot
JOIN dds.dim_user AS users USING (user_sk)
WHERE snapshot.karma >= 300
ORDER BY snapshot.karma DESC, users.user_id;

-- 3. История роста karma выбранного пользователя.
SELECT
    users.user_id,
    snapshot.source_loaded_at,
    snapshot.karma,
    snapshot.karma_delta,
    snapshot.submitted_count,
    snapshot.submitted_count_delta
FROM dds.dim_user AS users
JOIN dds.fact_user_snapshot AS snapshot USING (user_sk)
WHERE users.user_id = 'alice'
ORDER BY snapshot.source_loaded_at;

-- 4. Последний снимок публикаций: наиболее популярные сверху.
WITH latest_story_snapshot AS (
    SELECT DISTINCT ON (story_sk)
        story_sk,
        source_loaded_at,
        score,
        score_delta,
        descendants,
        loaded_comments_count
    FROM dds.fact_story_snapshot
    ORDER BY story_sk, source_loaded_at DESC
)
SELECT
    story.story_id,
    story.title,
    author.user_id AS author,
    snapshot.source_loaded_at,
    snapshot.score,
    snapshot.score_delta,
    snapshot.descendants,
    snapshot.loaded_comments_count
FROM latest_story_snapshot AS snapshot
JOIN dds.dim_story AS story USING (story_sk)
LEFT JOIN dds.dim_user AS author
    ON author.user_sk = story.author_user_sk
ORDER BY snapshot.score DESC, story.story_id;

-- 5. История изменений описания пользователя (SCD Type 2).
SELECT
    users.user_id,
    profile.about,
    profile.valid_from,
    profile.valid_to,
    profile.valid_to IS NULL AS is_current
FROM dds.dim_user AS users
JOIN dds.dim_user_profile_history AS profile USING (user_sk)
WHERE users.user_id = 'alice'
ORDER BY profile.valid_from;

-- 6. Последние результаты Data Quality.
SELECT
    run_id,
    check_name,
    status,
    actual_value,
    expected_rule,
    details,
    checked_at
FROM dds.data_quality_results
ORDER BY checked_at DESC, check_name
LIMIT 50;
