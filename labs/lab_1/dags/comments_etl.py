import json
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

HN_API_URL = "https://hacker-news.firebaseio.com/v0"


url = "https://raw.githubusercontent.com/dsojevic/profanity-list/main/en.txt"


@dag(dag_id="comments_etl", schedule=timedelta(hours=1), catchup=False)
def comments_etl():

    @task
    def extract_data():

        response = requests.get(
            f"{HN_API_URL}/topstories.json",
            timeout=30,
        )

        response.raise_for_status()

        story_ids = response.json()
        story_ids = story_ids[:20]
        data_extract = []

        for story_id in story_ids:
            response_item = requests.get(
                f"{HN_API_URL}/item/{story_id}.json",
                timeout=30,
            )
            response_item.raise_for_status()
            item_json = response_item.json()

            for comment_id in item_json.get("kids", []):
                response_comment = requests.get(
                    f"{HN_API_URL}/item/{comment_id}.json",
                    timeout=30,
                )
                response_comment.raise_for_status()
                comment = response_comment.json()

                data_extract.append(comment)

        return data_extract

    @task
    def transform_data(data: list[dict]):
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        bad_words = response.text.splitlines()
        transformed = []
        for comment in data:
            bad_word = False
            text = comment.get("text", "").lower()

            found = [word for word in bad_words if word in text]

            if found:
                bad_word = True

            transformed.append(
                {
                    "id": comment["id"],
                    "parent": comment.get("parent"),
                    "author": comment.get("by"),
                    "bad_word": bad_word,
                    "text": comment.get("text"),
                    "created_at": datetime.fromtimestamp(comment["time"]).isoformat(),
                    "loaded_at": datetime.utcnow().isoformat(),
                }
            )

        return transformed

    @task
    def load_postgres(data: list[dict]):

        postgres_hook = PostgresHook(postgres_conn_id="postgres_ods")

        conn = postgres_hook.get_conn()

        cursor = conn.cursor()

        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS comments (
                    id BIGINT PRIMARY KEY,
                    parent BIGINT,
                    author TEXT,
                    bad_word BOOL,
                    text TEXT,
                    created_at TIMESTAMP,
                    loaded_at TIMESTAMP
                );
                """
        )

        for comment in data:
            cursor.execute(
                """
                    INSERT INTO comments (
                        id,
                        parent,
                        author,
                        bad_word,
                        text,
                        created_at,
                        loaded_at

                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)

                ON CONFLICT (id)
                DO UPDATE SET
                    loaded_at = EXCLUDED.loaded_at;
                """,
                (
                    comment["id"],
                    comment["parent"],
                    comment["author"],
                    comment["bad_word"],
                    comment["text"],
                    comment["created_at"],
                    comment["loaded_at"],
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()

    @task
    def load_mongo(data: list[dict]):

        mongo_hook = MongoHook(mongo_conn_id="mongo_hn")

        collection = mongo_hook.get_collection(
            mongo_collection="comments",
            mongo_db="hackernews",
        )

        for comment in data:
            collection.update_one(
                {"id": comment["id"]},
                {"$set": comment},
                upsert=True,
            )

    @task
    def load_minio(data: list[dict]):

        minio_hook = S3Hook(aws_conn_id="minio_s3")

        bucket_name = "hackernews"

        client = minio_hook.get_conn()

        exsiting_bucket = [
            bucket["Name"] for bucket in client.list_buckets()["Buckets"]
        ]

        if bucket_name not in exsiting_bucket:
            client.create_bucket(Bucket=bucket_name)

        json_data = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        )

        filename = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

        key = f"raw/comments/{filename}.json"

        minio_hook.load_string(
            string_data=json_data, bucket_name=bucket_name, key=key, replace=False
        )

    raw_comment = extract_data()

    transform_comment = transform_data(raw_comment)

    load_postgres(transform_comment)
    load_mongo(transform_comment)
    load_minio(transform_comment)


comments_etl()
