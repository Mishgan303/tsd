from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowException
from lib.incremental import window_from_dict
from lib.quality import (
    DataQualityError,
    assert_blocking_checks,
    persist_quality_results,
    run_quality_checks,
)

POSTGRES_CONNECTION_ID = "postgres_ods"


@dag(
    dag_id="dds_data_quality",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 24, tz="Europe/Moscow"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["hacker-news", "data-quality", "lab2"],
)
def dds_data_quality():
    """Проверяет качество DDS и сохраняет результаты каждой проверки."""

    @task(task_id="run_and_record_quality_checks")
    def run_and_record_quality_checks() -> int:
        """Выполняет DQ-проверки, пишет итоги и останавливает DAG при ошибке."""
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        result_run_id = conf.get("source_run_id", context["run_id"])
        source_windows = conf.get("source_windows")
        source_upper_bounds = None
        if source_windows is not None:
            source_upper_bounds = {
                source_name: window_from_dict(window).upper_bound
                for source_name, window in source_windows.items()
            }

        conn = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID).get_conn()
        try:
            results = run_quality_checks(
                conn,
                source_upper_bounds=source_upper_bounds,
            )
            persist_quality_results(conn, result_run_id, results)
            try:
                assert_blocking_checks(results)
            except DataQualityError as error:
                raise AirflowException(str(error)) from error
            return len(results)
        finally:
            conn.close()

    run_and_record_quality_checks()


dds_data_quality()
