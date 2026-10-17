from __future__ import annotations

from dataclasses import dataclass


class DataQualityError(RuntimeError):
    """Raised when at least one blocking staging check fails."""


@dataclass(frozen=True)
class QualityResult:
    """Describe one blocking quality rule and its observed result."""

    name: str
    status: bool
    actual_value: str
    expected_rule: str
    details: str | None = None


def _count(conn, query: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone()[0]


def _result(name: str, violations: int, expected_rule: str) -> QualityResult:
    return QualityResult(
        name=name,
        status=violations == 0,
        actual_value=str(violations),
        expected_rule=expected_rule,
    )


def run_staging_checks(conn) -> list[QualityResult]:
    """Run all blocking checks against the fully rebuilt staging marts."""
    checks = [
        (
            "uniqueness.primary_keys",
            """
            SELECT COALESCE(SUM(duplicate_count), 0)::bigint
            FROM (
                SELECT COUNT(*) - 1 AS duplicate_count
                FROM marts_staging.story_dynamics
                GROUP BY story_id, snapshot_at HAVING COUNT(*) > 1
                UNION ALL
                SELECT COUNT(*) - 1 FROM marts_staging.current_story_ranking
                GROUP BY story_id HAVING COUNT(*) > 1
                UNION ALL
                SELECT COUNT(*) - 1 FROM marts_staging.author_dynamics
                GROUP BY user_id, snapshot_at HAVING COUNT(*) > 1
                UNION ALL
                SELECT COUNT(*) - 1 FROM marts_staging.topic_performance
                GROUP BY snapshot_date, topic HAVING COUNT(*) > 1
                UNION ALL
                SELECT COUNT(*) - 1 FROM marts_staging.daily_activity
                GROUP BY activity_date HAVING COUNT(*) > 1
            ) AS duplicates
            """,
            "all five staging grains are unique",
        ),
        (
            "completeness.required_values",
            """
            SELECT (
                (SELECT COUNT(*) FROM marts_staging.story_dynamics
                 WHERE story_id IS NULL OR title IS NULL OR topic IS NULL
                    OR story_created_at IS NULL OR snapshot_at IS NULL
                    OR age_hours IS NULL OR score IS NULL
                    OR descendants IS NULL OR loaded_comments_count IS NULL)
              + (SELECT COUNT(*) FROM marts_staging.current_story_ranking
                 WHERE story_id IS NULL OR title IS NULL OR topic IS NULL
                    OR story_created_at IS NULL OR last_snapshot_at IS NULL
                    OR age_hours IS NULL OR score IS NULL
                    OR descendants IS NULL OR loaded_comments_count IS NULL
                    OR score_rank IS NULL)
              + (SELECT COUNT(*) FROM marts_staging.author_dynamics
                 WHERE user_id IS NULL OR snapshot_at IS NULL OR karma IS NULL
                    OR submitted_count IS NULL)
              + (SELECT COUNT(*) FROM marts_staging.topic_performance
                 WHERE snapshot_date IS NULL OR topic IS NULL
                    OR stories_count IS NULL OR authors_count IS NULL
                    OR average_score IS NULL OR maximum_score IS NULL
                    OR average_comments IS NULL OR total_score_growth IS NULL
                    OR total_comments_growth IS NULL)
              + (SELECT COUNT(*) FROM marts_staging.daily_activity
                 WHERE activity_date IS NULL
                    OR published_stories_count IS NULL
                    OR created_comments_count IS NULL
                    OR story_authors_count IS NULL
                    OR comment_authors_count IS NULL
                    OR active_users_count IS NULL)
            )::bigint
            """,
            "required keys, topics and measures are not NULL",
        ),
        (
            "validity.non_negative_measures",
            """
            SELECT (
                (SELECT COUNT(*) FROM marts_staging.story_dynamics
                 WHERE age_hours < 0 OR score < 0 OR descendants < 0
                    OR loaded_comments_count < 0)
              + (SELECT COUNT(*) FROM marts_staging.current_story_ranking
                 WHERE age_hours < 0 OR score < 0 OR descendants < 0
                    OR loaded_comments_count < 0 OR score_rank < 1)
              + (SELECT COUNT(*) FROM marts_staging.author_dynamics
                 WHERE submitted_count < 0)
              + (SELECT COUNT(*) FROM marts_staging.topic_performance
                 WHERE stories_count < 0 OR authors_count < 0
                    OR average_score < 0 OR maximum_score < 0
                    OR average_comments < 0)
              + (SELECT COUNT(*) FROM marts_staging.daily_activity
                 WHERE published_stories_count < 0
                    OR created_comments_count < 0
                    OR story_authors_count < 0
                    OR comment_authors_count < 0
                    OR active_users_count < 0)
            )::bigint
            """,
            "ratings, ages and counts are non-negative",
        ),
        (
            "reconciliation.story_dynamics",
            """
            SELECT ABS(
                (SELECT COUNT(*) FROM dds.fact_story_snapshot)
              - (SELECT COUNT(*) FROM marts_staging.story_dynamics)
            )::bigint
            """,
            "story dynamics count equals DDS story snapshot count",
        ),
        (
            "reconciliation.author_dynamics",
            """
            SELECT ABS(
                (SELECT COUNT(*) FROM dds.fact_user_snapshot)
              - (SELECT COUNT(*) FROM marts_staging.author_dynamics)
            )::bigint
            """,
            "author dynamics count equals DDS user snapshot count",
        ),
        (
            "reconciliation.story_topics",
            """
            SELECT COUNT(*)::bigint
            FROM (
                SELECT COALESCE(story.story_id, topics.story_id)
                FROM dds.dim_story AS story
                FULL OUTER JOIN marts_staging.story_topics AS topics
                    USING (story_id)
                WHERE story.story_id IS NULL OR topics.story_id IS NULL
            ) AS mismatches
            """,
            "every DDS story has exactly one staging topic",
        ),
        (
            "reconciliation.current_story_ranking",
            """
            SELECT ABS(
                (SELECT COUNT(DISTINCT story_sk)
                 FROM dds.fact_story_snapshot)
              - (SELECT COUNT(*)
                 FROM marts_staging.current_story_ranking)
            )::bigint
            """,
            "ranking has one row for every story with a snapshot",
        ),
        (
            "consistency.current_story_measures",
            """
            WITH latest AS (
                SELECT
                    story.story_id,
                    snapshot.source_loaded_at,
                    snapshot.score,
                    snapshot.score_delta,
                    snapshot.descendants,
                    snapshot.descendants_delta,
                    snapshot.loaded_comments_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY story.story_id
                        ORDER BY snapshot.source_loaded_at DESC
                    ) AS row_number
                FROM dds.dim_story AS story
                JOIN dds.fact_story_snapshot AS snapshot
                    ON snapshot.story_sk = story.story_sk
            )
            SELECT COUNT(*)::bigint
            FROM latest
            LEFT JOIN marts_staging.current_story_ranking AS ranking
                USING (story_id)
            WHERE latest.row_number = 1
              AND (
                    ranking.story_id IS NULL
                 OR ranking.last_snapshot_at IS DISTINCT FROM latest.source_loaded_at
                 OR ranking.score IS DISTINCT FROM latest.score
                 OR ranking.score_delta IS DISTINCT FROM latest.score_delta
                 OR ranking.descendants IS DISTINCT FROM latest.descendants
                 OR ranking.descendants_delta IS DISTINCT FROM latest.descendants_delta
                 OR ranking.loaded_comments_count
                    IS DISTINCT FROM latest.loaded_comments_count
              )
            """,
            "ranking measures equal each story's latest DDS snapshot",
        ),
        (
            "validity.non_blank_topics",
            """
            SELECT (
                (SELECT COUNT(*) FROM marts_staging.story_dynamics
                 WHERE btrim(topic) = '')
              + (SELECT COUNT(*) FROM marts_staging.current_story_ranking
                 WHERE btrim(topic) = '')
              + (SELECT COUNT(*) FROM marts_staging.topic_performance
                 WHERE btrim(topic) = '')
            )::bigint
            """,
            "topics in every topic-based mart are non-blank",
        ),
        (
            "consistency.score_rank",
            """
            SELECT COUNT(*)::bigint
            FROM (
                SELECT
                    story_id,
                    score_rank,
                    RANK() OVER (ORDER BY score DESC, story_id) AS expected_rank
                FROM marts_staging.current_story_ranking
            ) AS ranks
            WHERE score_rank <> expected_rank
            """,
            "score rank starts at one and follows score DESC, story_id",
        ),
    ]

    return [
        _result(name, _count(conn, query), expected_rule)
        for name, query, expected_rule in checks
    ]


def assert_staging_quality(results: list[QualityResult]) -> None:
    """Raise one error containing every failed blocking check name."""
    failures = [result.name for result in results if not result.status]
    if failures:
        raise DataQualityError(
            "blocking staging checks failed: " + ", ".join(failures)
        )
