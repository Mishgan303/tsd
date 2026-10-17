import json
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag, task

HN_API_URL = "https://hacker-news.firebaseio.com/v0"
ODS_STORIES_FQN = "tsd_postgres.ods.public.stories"


def _top_story_ids(payload: object) -> list[int]:
    """Проверяет ответ top stories и возвращает первые 100 ID в исходном порядке."""
    story_ids = payload[:100]
    if any(
        isinstance(story_id, bool) or not isinstance(story_id, int) or story_id <= 0
        for story_id in story_ids
    ):
        raise ValueError("Hacker News top stories contain an invalid ID")
    if len(story_ids) != len(set(story_ids)):
        raise ValueError("Hacker News top stories contain duplicate IDs")

    return story_ids


@dag(
    dag_id="stories_etl",
    schedule=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["hacker-news", "etl", "lab1"],
)
def stories_etl():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_stories() -> list[dict]:
        """Получает один согласованный top-100 набор публикаций Hacker News."""
        response = requests.get(
            f"{HN_API_URL}/topstories.json",
            timeout=30,
        )
        response.raise_for_status()

        stories = []
        for story_id in _top_story_ids(response.json()):
            response = requests.get(
                f"{HN_API_URL}/item/{story_id}.json",
                timeout=30,
            )
            response.raise_for_status()
            story = response.json()
            if story is not None:
                stories.append(story)

        if not stories:
            raise ValueError("Hacker News returned no available stories")
        return stories

    @task
    def transform_stories(stories: list[dict]) -> list[dict]:
        """Приводит сырые публикации к единой структуре ODS."""
        transformed = []
        loaded_at = datetime.utcnow().isoformat()

        for story in stories:
            transformed.append(
                {
                    "id": story["id"],
                    "title": story.get("title"),
                    "author": story.get("by"),
                    "url": story.get("url"),
                    "score": story.get("score", 0),
                    "descendants": story.get("descendants", 0),
                    "created_at": datetime.fromtimestamp(story["time"]).isoformat(),
                    "loaded_at": loaded_at,
                }
            )

        return transformed

    @task
    def collect_story_ids(stories: list[dict]) -> list[int]:
        """Возвращает небольшой список ID для запуска дочерних DAG."""
        story_ids = [story["id"] for story in stories]
        if len(story_ids) != len(set(story_ids)):
            raise ValueError("Transformed stories contain duplicate IDs")
        return story_ids

    @task(outlets=[{"tables": [ODS_STORIES_FQN]}])
    def load_postgres(stories: list[dict]) -> None:
        """Идемпотентно загружает публикации в PostgreSQL ODS."""
        hook = PostgresHook(postgres_conn_id="postgres_ods")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stories (
                    id BIGINT PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    url TEXT,
                    score INTEGER,
                    descendants INTEGER,
                    created_at TIMESTAMP,
                    loaded_at TIMESTAMP
                );
                """
            )

            for story in stories:
                cursor.execute(
                    """
                    INSERT INTO stories (
                        id,
                        title,
                        author,
                        url,
                        score,
                        descendants,
                        created_at,
                        loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id)
                    DO UPDATE SET
                        score = EXCLUDED.score,
                        descendants = EXCLUDED.descendants,
                        loaded_at = EXCLUDED.loaded_at;
                    """,
                    (
                        story["id"],
                        story["title"],
                        story["author"],
                        story["url"],
                        story["score"],
                        story["descendants"],
                        story["created_at"],
                        story["loaded_at"],
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    @task
    def load_mongo(stories: list[dict]) -> None:
        """Идемпотентно загружает публикации в MongoDB ODS."""
        hook = MongoHook(mongo_conn_id="mongo_hn")
        collection = hook.get_collection(
            mongo_collection="stories",
            mongo_db="hackernews",
        )

        for story in stories:
            collection.update_one(
                {"id": story["id"]},
                {"$set": story},
                upsert=True,
            )

    @task
    def load_minio(stories: list[dict]) -> None:
        """Сохраняет JSON-снимок публикаций в MinIO."""
        hook = S3Hook(aws_conn_id="minio_s3")
        bucket_name = "hackernews"
        client = hook.get_conn()
        existing_buckets = [
            bucket["Name"] for bucket in client.list_buckets()["Buckets"]
        ]
        if bucket_name not in existing_buckets:
            client.create_bucket(Bucket=bucket_name)

        filename = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        hook.load_string(
            string_data=json.dumps(stories, ensure_ascii=False, indent=2),
            bucket_name=bucket_name,
            key=f"raw/stories/{filename}.json",
            replace=False,
        )

    raw_stories = extract_stories()
    clean_stories = transform_stories(raw_stories)
    story_ids = collect_story_ids(clean_stories)

    story_loads = [
        load_postgres(clean_stories),
        load_mongo(clean_stories),
        load_minio(clean_stories),
    ]

    trigger_users = TriggerDagRunOperator(
        task_id="trigger_users_etl",
        trigger_dag_id="users_etl",
        conf={"story_ids": story_ids},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=10,
    )
    trigger_comments = TriggerDagRunOperator(
        task_id="trigger_comments_etl",
        trigger_dag_id="comments_etl",
        conf={"story_ids": story_ids},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=10,
    )

    for story_load in story_loads:
        story_load >> [trigger_users, trigger_comments]
    story_ids >> [trigger_users, trigger_comments]


stories_etl()
