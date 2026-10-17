from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from lib.models import QualityResult


class DataQualityError(RuntimeError):
    """Сигнализирует о неуспешной критической DQ-проверке."""


def _count(cursor, sql: str, params: tuple = ()) -> int:
    """Выполняет SQL-запрос и возвращает первое значение как целый счётчик."""
    cursor.execute(sql, params)
    return int(cursor.fetchone()[0] or 0)


def _result(
    check_name: str,
    violations: int,
    expected_rule: str,
    *,
    blocking: bool = True,
    details: str | None = None,
) -> QualityResult:
    """Формирует результат DQ-проверки по числу найденных нарушений."""
    return QualityResult(
        check_name=check_name,
        status=violations == 0,
        actual_value=str(violations),
        expected_rule=expected_rule,
        details=details,
        blocking=blocking,
    )


def _source_exists(cursor, table_name: str) -> bool:
    """Проверяет наличие ODS-таблицы в PostgreSQL."""
    cursor.execute("SELECT to_regclass(%s)", (table_name,))
    return cursor.fetchone()[0] is not None


def _reconciliation_result(
    cursor,
    *,
    source_name: str,
    table_name: str,
    check_name: str,
    sql: str,
    upper_bound: datetime | None,
) -> QualityResult:
    """Сверяет обработанные строки ODS с соответствующими записями DDS."""
    expected_rule = f"all processed {source_name} rows exist in DDS"
    if not _source_exists(cursor, table_name):
        return _result(
            check_name,
            int(upper_bound is not None),
            expected_rule,
            details=(
                f"source table {table_name} is missing"
                if upper_bound is not None
                else None
            ),
        )
    if upper_bound is None:
        return _result(check_name, 0, expected_rule)
    violations = _count(cursor, sql, (upper_bound,))
    return _result(check_name, violations, expected_rule)


def run_quality_checks(
    conn,
    *,
    source_upper_bounds: dict[str, datetime | None] | None = None,
) -> list[QualityResult]:
    """Выполняет полный набор проверок качества и сверки ODS→DDS."""
    with conn.cursor() as cursor:
        if source_upper_bounds is None:
            cursor.execute(
                "SELECT source_name, last_processed_at FROM dds.pipeline_state"
            )
            source_upper_bounds = dict(cursor.fetchall())

        completeness = _count(
            cursor,
            """
            SELECT
                (SELECT count(*) FROM dds.dim_user
                 WHERE user_id IS NULL OR btrim(user_id) = '')
              + (SELECT count(*) FROM dds.dim_story
                 WHERE story_id IS NULL OR title IS NULL
                    OR btrim(title) = '' OR created_at IS NULL)
              + (SELECT count(*) FROM dds.fact_user_snapshot
                 WHERE user_sk IS NULL OR source_loaded_at IS NULL
                    OR karma IS NULL OR submitted_count IS NULL)
              + (SELECT count(*) FROM dds.fact_story_snapshot
                 WHERE story_sk IS NULL OR source_loaded_at IS NULL
                    OR score IS NULL OR descendants IS NULL
                    OR loaded_comments_count IS NULL
                    OR bad_word_comments_count IS NULL)
              + (SELECT count(*) FROM dds.fact_comment
                 WHERE comment_id IS NULL OR story_sk IS NULL
                    OR bad_word IS NULL OR created_at IS NULL
                    OR source_loaded_at IS NULL)
            """,
        )
        uniqueness = _count(
            cursor,
            """
            SELECT COALESCE(sum(extra_rows), 0)
            FROM (
                SELECT count(*) - 1 AS extra_rows
                FROM dds.dim_user GROUP BY user_id HAVING count(*) > 1
                UNION ALL
                SELECT count(*) - 1
                FROM dds.dim_story GROUP BY story_id HAVING count(*) > 1
                UNION ALL
                SELECT count(*) - 1
                FROM dds.fact_comment GROUP BY comment_id HAVING count(*) > 1
                UNION ALL
                SELECT count(*) - 1
                FROM dds.fact_user_snapshot
                GROUP BY user_sk, source_loaded_at HAVING count(*) > 1
                UNION ALL
                SELECT count(*) - 1
                FROM dds.fact_story_snapshot
                GROUP BY story_sk, source_loaded_at HAVING count(*) > 1
            ) duplicates
            """,
        )
        orphan_facts = _count(
            cursor,
            """
            SELECT
                (SELECT count(*)
                 FROM dds.fact_user_snapshot f
                 LEFT JOIN dds.dim_user d USING (user_sk)
                 WHERE d.user_sk IS NULL)
              + (SELECT count(*)
                 FROM dds.dim_user_profile_history p
                 LEFT JOIN dds.dim_user d USING (user_sk)
                 WHERE d.user_sk IS NULL)
              + (SELECT count(*)
                 FROM dds.dim_story s
                 LEFT JOIN dds.dim_user u ON u.user_sk = s.author_user_sk
                 WHERE s.author_user_sk IS NOT NULL AND u.user_sk IS NULL)
              + (SELECT count(*)
                 FROM dds.fact_story_snapshot f
                 LEFT JOIN dds.dim_story d USING (story_sk)
                 WHERE d.story_sk IS NULL)
              + (SELECT count(*)
                 FROM dds.fact_comment c
                 LEFT JOIN dds.dim_story s USING (story_sk)
                 WHERE s.story_sk IS NULL)
              + (SELECT count(*)
                 FROM dds.fact_comment c
                 LEFT JOIN dds.dim_user u ON u.user_sk = c.author_user_sk
                 WHERE c.author_user_sk IS NOT NULL AND u.user_sk IS NULL)
            """,
        )
        non_negative = _count(
            cursor,
            """
            SELECT
                (SELECT count(*) FROM dds.fact_user_snapshot
                 WHERE submitted_count < 0)
              + (SELECT count(*) FROM dds.fact_story_snapshot
                 WHERE score < 0 OR descendants < 0
                    OR loaded_comments_count < 0
                    OR bad_word_comments_count < 0)
            """,
        )
        future_timestamps = _count(
            cursor,
            """
            SELECT
                (SELECT count(*) FROM dds.dim_user WHERE created_at > now())
              + (SELECT count(*) FROM dds.dim_story WHERE created_at > now())
              + (SELECT count(*) FROM dds.fact_comment
                 WHERE created_at > now() OR source_loaded_at > now())
              + (SELECT count(*) FROM dds.fact_user_snapshot
                 WHERE source_loaded_at > now())
              + (SELECT count(*) FROM dds.fact_story_snapshot
                 WHERE source_loaded_at > now())
              + (SELECT count(*) FROM dds.dim_user_profile_history
                 WHERE valid_from > now() OR valid_to > now())
            """,
        )
        single_open = _count(
            cursor,
            """
            SELECT count(*) FROM (
                SELECT user_sk
                FROM dds.dim_user_profile_history
                WHERE valid_to IS NULL
                GROUP BY user_sk
                HAVING count(*) > 1
            ) violations
            """,
        )
        overlapping = _count(
            cursor,
            """
            SELECT count(*)
            FROM dds.dim_user_profile_history left_version
            JOIN dds.dim_user_profile_history right_version
              ON left_version.user_sk = right_version.user_sk
             AND left_version.user_profile_sk < right_version.user_profile_sk
             AND left_version.valid_from
                 < COALESCE(right_version.valid_to, 'infinity')
             AND right_version.valid_from
                 < COALESCE(left_version.valid_to, 'infinity')
            """,
        )
        snapshot_duplicates = _count(
            cursor,
            """
            SELECT count(*) FROM (
                SELECT user_sk, source_loaded_at
                FROM dds.fact_user_snapshot
                GROUP BY user_sk, source_loaded_at HAVING count(*) > 1
                UNION ALL
                SELECT story_sk, source_loaded_at
                FROM dds.fact_story_snapshot
                GROUP BY story_sk, source_loaded_at HAVING count(*) > 1
            ) duplicates
            """,
        )
        comment_anomalies = _count(
            cursor,
            """
            SELECT count(*) FROM dds.fact_story_snapshot
            WHERE bad_word_comments_count > loaded_comments_count
               OR loaded_comments_count > descendants
            """,
        )

        results = [
            _result(
                "completeness.required_values",
                completeness,
                "required DDS keys and measures are not NULL or blank",
            ),
            _result(
                "uniqueness.natural_and_snapshot_keys",
                uniqueness,
                "natural and compound snapshot keys are unique",
            ),
            _result(
                "referential_integrity.orphan_facts",
                orphan_facts,
                "all DDS foreign keys resolve",
            ),
            _result(
                "validity.non_negative_measures",
                non_negative,
                "counts, score and descendants are non-negative",
            ),
            _result(
                "validity.timestamps_not_in_future",
                future_timestamps,
                "business and source timestamps are not in the future",
            ),
            _result(
                "scd2.single_open_version",
                single_open,
                "each user has at most one open profile version",
            ),
            _result(
                "scd2.non_overlapping_intervals",
                overlapping,
                "profile history intervals do not overlap",
            ),
            _reconciliation_result(
                cursor,
                source_name="users",
                table_name="public.users",
                check_name="reconciliation.ods_users_to_dds",
                upper_bound=source_upper_bounds.get("users"),
                sql="""
                    SELECT count(*)
                    FROM public.users source
                    LEFT JOIN dds.dim_user users
                      ON users.user_id = source.id
                    LEFT JOIN dds.fact_user_snapshot snapshot
                      ON snapshot.user_sk = users.user_sk
                     AND snapshot.source_loaded_at =
                         source.loaded_at AT TIME ZONE 'UTC'
                    WHERE source.loaded_at AT TIME ZONE 'UTC'
                          <= %s::timestamptz
                      AND snapshot.user_sk IS NULL
                """,
            ),
            _reconciliation_result(
                cursor,
                source_name="stories",
                table_name="public.stories",
                check_name="reconciliation.ods_stories_to_dds",
                upper_bound=source_upper_bounds.get("stories"),
                sql="""
                    SELECT count(*)
                    FROM public.stories source
                    LEFT JOIN dds.dim_story stories
                      ON stories.story_id = source.id
                    LEFT JOIN dds.fact_story_snapshot snapshot
                      ON snapshot.story_sk = stories.story_sk
                     AND snapshot.source_loaded_at =
                         source.loaded_at AT TIME ZONE 'UTC'
                    WHERE source.loaded_at AT TIME ZONE 'UTC'
                          <= %s::timestamptz
                      AND snapshot.story_sk IS NULL
                """,
            ),
            _reconciliation_result(
                cursor,
                source_name="comments",
                table_name="public.comments",
                check_name="reconciliation.ods_comments_to_dds",
                upper_bound=source_upper_bounds.get("comments"),
                sql="""
                    SELECT count(*)
                    FROM public.comments source
                    LEFT JOIN dds.fact_comment comment
                      ON comment.comment_id = source.id
                    WHERE source.loaded_at AT TIME ZONE 'UTC'
                          <= %s::timestamptz
                      AND comment.comment_id IS NULL
                """,
            ),
            _result(
                "history.snapshot_key_uniqueness",
                snapshot_duplicates,
                "retries never create duplicate snapshot grains",
            ),
            _result(
                "anomaly.comment_counts",
                comment_anomalies,
                "bad_word <= loaded first-level comments <= descendants",
                blocking=False,
                details=(
                    "WARNING: Hacker News story and comment API calls are not atomic"
                    if comment_anomalies
                    else None
                ),
            ),
        ]
    return results


def persist_quality_results(
    conn, run_id: str, results: Iterable[QualityResult]
) -> None:
    """Идемпотентно сохраняет результаты DQ-проверок для конкретного запуска."""
    try:
        with conn.cursor() as cursor:
            for result in results:
                cursor.execute(
                    """
                    INSERT INTO dds.data_quality_results (
                        run_id,
                        check_name,
                        status,
                        actual_value,
                        expected_rule,
                        details
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, check_name) DO UPDATE SET
                        status = EXCLUDED.status,
                        actual_value = EXCLUDED.actual_value,
                        expected_rule = EXCLUDED.expected_rule,
                        details = EXCLUDED.details,
                        checked_at = now()
                    """,
                    (
                        run_id,
                        result.check_name,
                        result.status,
                        result.actual_value,
                        result.expected_rule,
                        result.details,
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def assert_blocking_checks(results: Iterable[QualityResult]) -> None:
    """Поднимает `DataQualityError` при неуспешной блокирующей проверке."""
    failed = sorted(
        result.check_name
        for result in results
        if not result.status and result.blocking
    )
    if failed:
        raise DataQualityError(
            f"blocking data quality checks failed: {', '.join(failed)}"
        )
