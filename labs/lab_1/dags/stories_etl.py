import json
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

HN_API_URL = "https://hacker-news.firebaseio.com/v0"


@dag(
    dag_id="stories_etl",
    schedule=timedelta(hours=1),
    catchup=False,
    tags=["hacker-news", "etl", "lab1"],
)
def stories_etl():

    @task
    def extract_stories():
        """
        Получаем данные из Hacker News API.
        """
        response = requests.get(
            f"{HN_API_URL}/topstories.json",
            timeout=30,
        )

        response.raise_for_status()

        story_ids = response.json()

        story_ids = story_ids[:20]

        stories = []

        for story_id in story_ids:
            response = requests.get(
                f"{HN_API_URL}/item/{story_id}.json",
                timeout=30,
            )

            response.raise_for_status()

            story = response.json()

            if story is not None:
                stories.append(story)

        return stories

    @task
    def transform_stories(stories: list[dict]):
        """
        Приводим сырые JSON Hacker News
        к нашей единой структуре.
        """

        transformed = []

        for story in stories:
            transformed.append(
                {
                    "id": story["id"],
                    "title": story.get("title"),
                    "author": story.get("by"),
                    "url": story.get("url"),
                    "score": story.get("score", 0),
                    "descendants": story.get("descendants", 0),
                    # В API время хранится как UNIX timestamp.
                    "created_at": datetime.fromtimestamp(story["time"]).isoformat(),
                    # Добавляем время именно нашего ETL.
                    # Оно пригодится для истории загрузок.
                    "loaded_at": datetime.utcnow().isoformat(),
                }
            )

        return transformed

    @task
    def load_postgres(stories: list[dict]):

        hook = PostgresHook(postgres_conn_id="postgres_ods")

        conn = hook.get_conn()

        cursor = conn.cursor()

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
            # ON CONFLICT нужен, потому что следующий запуск DAG
            # снова может получить те же самые story ID.
            #
            # Без него мы получили бы ошибку PRIMARY KEY.
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

        cursor.close()
        conn.close()

    # -------------------------
    # 4. LOAD → MONGODB
    # -------------------------

    @task
    def load_mongo(stories: list[dict]):

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
    def load_minio(stories: list[dict]):

        hook = S3Hook(aws_conn_id="minio_s3")

        bucket_name = "hackernews"

        client = hook.get_conn()

        existing_buckets = [
            bucket["Name"] for bucket in client.list_buckets()["Buckets"]
        ]

        if bucket_name not in existing_buckets:
            client.create_bucket(Bucket=bucket_name)

        json_data = json.dumps(
            stories,
            ensure_ascii=False,
            indent=2,
        )

        filename = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

        key = f"raw/stories/{filename}.json"

        hook.load_string(
            string_data=json_data,
            bucket_name=bucket_name,
            key=key,
            replace=False,
        )

    raw_stories = extract_stories()

    clean_stories = transform_stories(raw_stories)

    load_postgres(clean_stories)
    load_mongo(clean_stories)
    load_minio(clean_stories)


stories_etl()
