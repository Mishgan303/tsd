from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from lab3_lib.topic_classifier import classify_topic

Classifier = Callable[[str, str | None], str]
MART_TABLES = (
    "story_dynamics",
    "current_story_ranking",
    "author_dynamics",
    "topic_performance",
    "daily_activity",
)


def refresh_story_topics(
    conn,
    classifier: Classifier = classify_topic,
) -> int:
    """Rebuild staging topics for all unique DDS publications."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT story_id, title, url
            FROM dds.dim_story
            ORDER BY story_id
            """
        )
        stories = cursor.fetchall()

        classified = []
        for story_id, title, url in stories:
            topic = classifier(title, url)
            if not isinstance(topic, str) or not topic.strip():
                raise ValueError("classifier must return a non-empty string")
            classified.append((story_id, topic.strip()))

        cursor.execute("TRUNCATE TABLE marts_staging.story_topics")
        if classified:
            cursor.executemany(
                """
                INSERT INTO marts_staging.story_topics (story_id, topic)
                VALUES (%s, %s)
                """,
                classified,
            )

    return len(classified)


def execute_sql_file(conn, path: Path) -> None:
    """Execute one trusted UTF-8 SQL file on the supplied transaction."""
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cursor:
        cursor.execute(sql)


def _table_counts(conn, schema_name: str) -> dict[str, int]:
    counts = {}
    with conn.cursor() as cursor:
        for table_name in MART_TABLES:
            cursor.execute(f"SELECT count(*) FROM {schema_name}.{table_name}")
            counts[table_name] = cursor.fetchone()[0]
    return counts


def build_staging_marts(conn, path: Path) -> dict[str, int]:
    """Fully rebuild the five staging marts and return their row counts."""
    execute_sql_file(conn, path)
    return _table_counts(conn, "marts_staging")


def publish_marts(conn, path: Path) -> dict[str, int]:
    """Replace all five final marts within the caller-owned transaction."""
    execute_sql_file(conn, path)
    return _table_counts(conn, "marts")
