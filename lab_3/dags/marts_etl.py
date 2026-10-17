from __future__ import annotations

import logging
from pathlib import Path

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from lab3_lib.marts import (
    build_staging_marts,
    execute_sql_file,
    publish_marts,
    refresh_story_topics,
)
from lab3_lib.quality import assert_staging_quality, run_staging_checks

POSTGRES_CONNECTION_ID = "postgres_ods"
SCHEMA_PATH = Path("/opt/airflow/sql/lab_3/create_marts_schema.sql")
BUILD_PATH = Path("/opt/airflow/sql/lab_3/build_staging_marts.sql")
PUBLISH_PATH = Path("/opt/airflow/sql/lab_3/publish_marts.sql")
LOGGER = logging.getLogger(__name__)
DDS_TABLE_FQNS = [
    "tsd_postgres.ods.dds.dim_user",
    "tsd_postgres.ods.dds.dim_user_profile_history",
    "tsd_postgres.ods.dds.fact_user_snapshot",
    "tsd_postgres.ods.dds.dim_story",
    "tsd_postgres.ods.dds.fact_story_snapshot",
    "tsd_postgres.ods.dds.fact_comment",
]
MART_TABLE_FQNS = [
    "tsd_postgres.ods.marts.story_dynamics",
    "tsd_postgres.ods.marts.current_story_ranking",
    "tsd_postgres.ods.marts.author_dynamics",
    "tsd_postgres.ods.marts.topic_performance",
    "tsd_postgres.ods.marts.daily_activity",
]


def _connection():
    """Open the shared ODS/DDS/marts PostgreSQL connection."""
    return PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID).get_conn()


@dag(
    dag_id="marts_etl",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 3, tz="Europe/Moscow"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["hacker-news", "marts", "lab3"],
)
def marts_etl():
    """Fully rebuild, validate and atomically publish five analytical marts."""

    @task(task_id="ensure_marts_schema")
    def ensure_marts_schema_task() -> None:
        """Create the marts and staging schemas, tables, checks and indexes."""
        conn = _connection()
        try:
            execute_sql_file(conn, SCHEMA_PATH)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(task_id="classify_story_topics")
    def classify_story_topics_task() -> int:
        """Classify every unique DDS story into a non-empty staging topic."""
        conn = _connection()
        try:
            count = refresh_story_topics(conn)
            conn.commit()
            return count
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(
        task_id="build_staging_marts",
        inlets=[{"tables": DDS_TABLE_FQNS}],
    )
    def build_staging_marts_task() -> dict[str, int]:
        """Fully rebuild all five staging marts from DDS and story topics."""
        conn = _connection()
        try:
            counts = build_staging_marts(conn, BUILD_PATH)
            conn.commit()
            return counts
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(task_id="validate_staging_marts")
    def validate_staging_marts_task() -> int:
        """Run blocking staging checks before any final mart is changed."""
        conn = _connection()
        try:
            results = run_staging_checks(conn)
            for result in results:
                LOGGER.info(
                    "quality check name=%s status=%s actual=%s expected=%s",
                    result.name,
                    result.status,
                    result.actual_value,
                    result.expected_rule,
                )
            assert_staging_quality(results)
            return len(results)
        finally:
            conn.close()

    @task(
        task_id="publish_marts",
        outlets=[{"tables": MART_TABLE_FQNS}],
    )
    def publish_marts_task() -> dict[str, int]:
        """Replace all five final marts in one caller-owned transaction."""
        conn = _connection()
        try:
            counts = publish_marts(conn, PUBLISH_PATH)
            conn.commit()
            return counts
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    schema = ensure_marts_schema_task()
    topics = classify_story_topics_task()
    staging = build_staging_marts_task()
    quality = validate_staging_marts_task()
    published = publish_marts_task()

    schema >> topics >> staging >> quality >> published


marts_etl()
