import json
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

HN_API_URL = "https://hacker-news.firebaseio.com/v0"
ODS_USERS_FQN = "tsd_postgres.ods.public.users"


def _story_ids_from_conf(dag_run: object) -> list[int]:
    """Проверяет контракт `dag_run.conf` и возвращает согласованный список ID."""
    conf = getattr(dag_run, "conf", None)
    if not isinstance(conf, dict):
        raise ValueError("dag_run.conf must be an object with story_ids")

    story_ids = conf.get("story_ids")
    if not isinstance(story_ids, list) or not 1 <= len(story_ids) <= 100:
        raise ValueError("story_ids must be a non-empty list of at most 100 IDs")
    if any(
        isinstance(story_id, bool)
        or not isinstance(story_id, int)
        or story_id <= 0
        for story_id in story_ids
    ):
        raise ValueError("story_ids must contain only positive integers")
    if len(story_ids) != len(set(story_ids)):
        raise ValueError("story_ids must not contain duplicates")

    return story_ids


@dag(
    dag_id="users_etl",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["hacker-news", "etl", "lab1"],
)
def users_etl():

    @task
    def validate_story_ids() -> list[int]:
        """Читает и проверяет список публикаций, переданный родительским DAG."""
        return _story_ids_from_conf(get_current_context().get("dag_run"))

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_data(story_ids: list[int]) -> list[dict]:
        """Находит авторов публикаций в ODS и получает их профили из API."""
        postgres_hook = PostgresHook(postgres_conn_id="postgres_ods")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT id, author
                FROM public.stories
                WHERE id = ANY(%s);
                """,
                (story_ids,),
            )
            story_rows = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

        authors_by_story = {story_id: author for story_id, author in story_rows}
        missing_story_ids = [
            story_id for story_id in story_ids if story_id not in authors_by_story
        ]
        if missing_story_ids:
            raise ValueError(
                "Stories are missing from PostgreSQL ODS: "
                f"{missing_story_ids}"
            )

        usernames = []
        seen_usernames = set()
        for story_id in story_ids:
            username = authors_by_story[story_id]
            if username and username not in seen_usernames:
                seen_usernames.add(username)
                usernames.append(username)

        users = []
        for username in usernames:
            response = requests.get(
                f"{HN_API_URL}/user/{username}.json",
                timeout=30,
            )
            response.raise_for_status()
            user = response.json()
            if user is not None:
                users.append(user)

        return users

    @task
    def transform_data(data: list[dict]) -> list[dict]:
        """Приводит профили пользователей к единой структуре ODS."""
        transformed = []
        loaded_at = datetime.utcnow().isoformat()

        for user in data:
            transformed.append(
                {
                    "id": user["id"],
                    "karma": user.get("karma", 0),
                    "about": user.get("about"),
                    "submitted_count": len(user.get("submitted", [])),
                    "created_at": datetime.fromtimestamp(user["created"]).isoformat(),
                    "loaded_at": loaded_at,
                }
            )

        return transformed

    @task(outlets=[{"tables": [ODS_USERS_FQN]}])
    def load_postgres(data: list[dict]) -> None:
        """Идемпотентно загружает профили пользователей в PostgreSQL ODS."""
        postgres_hook = PostgresHook(postgres_conn_id="postgres_ods")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    karma INTEGER,
                    about TEXT,
                    submitted_count INTEGER,
                    created_at TIMESTAMP,
                    loaded_at TIMESTAMP
                );
                """
            )

            for user in data:
                cursor.execute(
                    """
                    INSERT INTO users (
                        id,
                        karma,
                        about,
                        submitted_count,
                        created_at,
                        loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id)
                    DO UPDATE SET
                        karma = EXCLUDED.karma,
                        about = EXCLUDED.about,
                        submitted_count = EXCLUDED.submitted_count,
                        loaded_at = EXCLUDED.loaded_at;
                    """,
                    (
                        user["id"],
                        user["karma"],
                        user["about"],
                        user["submitted_count"],
                        user["created_at"],
                        user["loaded_at"],
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
    def load_mongo(data: list[dict]) -> None:
        """Идемпотентно загружает профили пользователей в MongoDB ODS."""
        mongo_hook = MongoHook(mongo_conn_id="mongo_hn")
        collection = mongo_hook.get_collection(
            mongo_collection="users",
            mongo_db="hackernews",
        )

        for user in data:
            collection.update_one(
                {"id": user["id"]},
                {"$set": user},
                upsert=True,
            )

    @task
    def load_minio(data: list[dict]) -> None:
        """Сохраняет JSON-снимок профилей пользователей в MinIO."""
        minio_hook = S3Hook(aws_conn_id="minio_s3")
        bucket_name = "hackernews"
        client = minio_hook.get_conn()
        existing_buckets = [
            bucket["Name"] for bucket in client.list_buckets()["Buckets"]
        ]
        if bucket_name not in existing_buckets:
            client.create_bucket(Bucket=bucket_name)

        filename = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        minio_hook.load_string(
            string_data=json.dumps(data, ensure_ascii=False, indent=2),
            bucket_name=bucket_name,
            key=f"raw/users/{filename}.json",
            replace=False,
        )

    story_ids = validate_story_ids()
    raw_users = extract_data(story_ids)
    transformed_users = transform_data(raw_users)

    load_postgres(transformed_users)
    load_mongo(transformed_users)
    load_minio(transformed_users)


users_etl()
