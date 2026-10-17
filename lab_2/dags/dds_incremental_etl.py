from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from lib.incremental import (
    capture_source_windows,
    ensure_schema,
    load_comments,
    load_dim_stories,
    load_dim_users,
    load_story_snapshots,
    load_user_profile_history,
    load_user_snapshots,
    source_changed,
    update_pipeline_state,
    window_from_dict,
    window_to_dict,
)

SCHEMA_PATH = Path("/opt/airflow/sql/lab_2/create_dds_schema.sql")
POSTGRES_CONNECTION_ID = "postgres_ods"
ODS_USERS_FQN = "tsd_postgres.ods.public.users"
ODS_STORIES_FQN = "tsd_postgres.ods.public.stories"
ODS_COMMENTS_FQN = "tsd_postgres.ods.public.comments"
DDS_DIM_USER_FQN = "tsd_postgres.ods.dds.dim_user"
DDS_USER_PROFILE_FQN = "tsd_postgres.ods.dds.dim_user_profile_history"
DDS_USER_SNAPSHOT_FQN = "tsd_postgres.ods.dds.fact_user_snapshot"
DDS_DIM_STORY_FQN = "tsd_postgres.ods.dds.dim_story"
DDS_STORY_SNAPSHOT_FQN = "tsd_postgres.ods.dds.fact_story_snapshot"
DDS_COMMENT_FQN = "tsd_postgres.ods.dds.fact_comment"


def _connection():
    """Открывает подключение к PostgreSQL через Airflow Connection."""
    return PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID).get_conn()


def _deserialize_windows(payload: dict[str, dict[str, str | None]]):
    """Восстанавливает окна загрузки из переданных через XCom словарей."""
    return {name: window_from_dict(value) for name, value in payload.items()}


@dag(
    dag_id="dds_incremental_etl",
    schedule="0 */8 * * *",
    start_date=pendulum.datetime(2026, 9, 24, tz="Europe/Moscow"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["hacker-news", "dds", "lab2"],
)
def dds_incremental_etl():
    """Инкрементально переносит изменения ODS в DDS и запускает DQ-проверку."""

    @task(task_id="ensure_dds_schema")
    def ensure_dds_schema_task() -> None:
        """Создаёт схему и таблицы DDS, если они ещё не существуют."""
        conn = _connection()
        ensure_schema(conn, SCHEMA_PATH)
        conn.close()

    @task(task_id="check_ods_changes")
    def check_ods_changes_task(
        _schema_ready: None,
    ) -> dict[str, dict[str, str | None]]:
        """Фиксирует границы новых данных ODS для текущего запуска."""
        conn = _connection()
        try:
            windows = capture_source_windows(conn)
            return {name: window_to_dict(value) for name, value in windows.items()}
        finally:
            conn.close()

    @task.branch(task_id="choose_load_path")
    def choose_load_path(payload: dict[str, dict[str, str | None]]) -> str:
        """Выбирает загрузку DDS при наличии изменений, иначе ветку no_changes."""
        windows = _deserialize_windows(payload)
        if any(source_changed(window) for window in windows.values()):
            return "load_dim_user"
        return "no_changes"

    @task(
        task_id="load_dim_user",
        inlets=[
            {"tables": [ODS_USERS_FQN, ODS_STORIES_FQN, ODS_COMMENTS_FQN]}
        ],
        outlets=[{"tables": [DDS_DIM_USER_FQN]}],
    )
    def load_dim_user_task(payload: dict[str, dict[str, str | None]]) -> int:
        """Загружает в `dim_user` авторов из новых users, stories и comments."""
        conn = _connection()
        try:
            return load_dim_users(conn, _deserialize_windows(payload))
        finally:
            conn.close()

    @task(
        task_id="load_user_profile_history",
        inlets=[{"tables": [ODS_USERS_FQN, DDS_DIM_USER_FQN]}],
        outlets=[{"tables": [DDS_USER_PROFILE_FQN]}],
    )
    def load_user_profile_history_task(
        payload: dict[str, dict[str, str | None]],
    ) -> int:
        """Сохраняет историю изменений поля `about` по правилам SCD2."""
        conn = _connection()
        try:
            users_window = _deserialize_windows(payload)["users"]
            return load_user_profile_history(conn, users_window)
        finally:
            conn.close()

    @task(
        task_id="load_fact_user_snapshot",
        inlets=[{"tables": [ODS_USERS_FQN, DDS_DIM_USER_FQN]}],
        outlets=[{"tables": [DDS_USER_SNAPSHOT_FQN]}],
    )
    def load_fact_user_snapshot_task(
        payload: dict[str, dict[str, str | None]],
    ) -> int:
        """Записывает снимки кармы и числа публикаций пользователей."""
        conn = _connection()
        try:
            users_window = _deserialize_windows(payload)["users"]
            return load_user_snapshots(conn, users_window)
        finally:
            conn.close()

    @task(
        task_id="load_dim_story",
        inlets=[{"tables": [ODS_STORIES_FQN, DDS_DIM_USER_FQN]}],
        outlets=[{"tables": [DDS_DIM_STORY_FQN]}],
    )
    def load_dim_story_task(
        payload: dict[str, dict[str, str | None]],
    ) -> int:
        """Загружает описательные атрибуты публикаций в `dim_story`."""
        conn = _connection()
        try:
            stories_window = _deserialize_windows(payload)["stories"]
            return load_dim_stories(conn, stories_window)
        finally:
            conn.close()

    @task(
        task_id="load_fact_comment",
        inlets=[
            {
                "tables": [
                    ODS_COMMENTS_FQN,
                    DDS_DIM_USER_FQN,
                    DDS_DIM_STORY_FQN,
                ]
            }
        ],
        outlets=[{"tables": [DDS_COMMENT_FQN]}],
    )
    def load_fact_comment_task(
        payload: dict[str, dict[str, str | None]],
    ) -> int:
        """Загружает комментарии первого уровня в `fact_comment`."""
        conn = _connection()
        try:
            comments_window = _deserialize_windows(payload)["comments"]
            return load_comments(conn, comments_window)
        finally:
            conn.close()

    @task(
        task_id="load_fact_story_snapshot",
        inlets=[
            {
                "tables": [
                    ODS_STORIES_FQN,
                    DDS_DIM_STORY_FQN,
                    DDS_COMMENT_FQN,
                ]
            }
        ],
        outlets=[{"tables": [DDS_STORY_SNAPSHOT_FQN]}],
    )
    def load_fact_story_snapshot_task(
        payload: dict[str, dict[str, str | None]],
    ) -> int:
        """Записывает снимки рейтинга и числа комментариев публикаций."""
        conn = _connection()
        try:
            stories_window = _deserialize_windows(payload)["stories"]
            return load_story_snapshots(conn, stories_window)
        finally:
            conn.close()

    @task(task_id="update_pipeline_state")
    def update_pipeline_state_task(
        payload: dict[str, dict[str, str | None]],
    ) -> int:
        """Перемещает watermark источников после успешной DQ-проверки."""
        conn = _connection()
        try:
            return update_pipeline_state(conn, _deserialize_windows(payload))
        finally:
            conn.close()

    schema_ready = ensure_dds_schema_task()
    payload = check_ods_changes_task(schema_ready)
    branch = choose_load_path(payload)

    no_changes = EmptyOperator(task_id="no_changes")
    finish = EmptyOperator(
        task_id="finish",
        trigger_rule="none_failed_min_one_success",
    )

    users = load_dim_user_task(payload)
    profiles = load_user_profile_history_task(payload)
    user_snapshots = load_fact_user_snapshot_task(payload)
    stories = load_dim_story_task(payload)
    comments = load_fact_comment_task(payload)
    story_snapshots = load_fact_story_snapshot_task(payload)
    pipeline_state = update_pipeline_state_task(payload)
    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="dds_data_quality",
        conf={
            "source_run_id": "{{ run_id }}",
            "source_windows": payload,
        },
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=10,
    )
    trigger_marts = TriggerDagRunOperator(
        task_id="trigger_marts_etl",
        trigger_dag_id="marts_etl",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=10,
    )

    branch >> [no_changes, users]
    no_changes >> finish
    (
        users
        >> profiles
        >> user_snapshots
        >> stories
        >> comments
        >> story_snapshots
        >> trigger_data_quality
        >> trigger_marts
        >> pipeline_state
        >> finish
    )


dds_incremental_etl()
