import json
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

HN_API_URL = "https://hacker-news.firebaseio.com/v0"


@dag(dag_id="users_etl", schedule=timedelta(hours=1), catchup=False)
def users_etl():

    @task
    def extract_data():

        response = requests.get(
            f"{HN_API_URL}/topstories.json",
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()[:30]

        usernames = set()

        for item in data:
            response_story = requests.get(
                f"{HN_API_URL}/item/{item}.json",
                timeout=30,
            )
            response_story.raise_for_status()

            story = response_story.json()

            if story and story.get("by"):
                usernames.add(story["by"])

        data_extract = []
        for username in usernames:
            response_user = requests.get(
                f"{HN_API_URL}/user/{username}.json",
                timeout=30,
            )
            response_user.raise_for_status()

            user = response_user.json()

            if user and user.get("karma", 0) >= 1500:
                data_extract.append(user)

        return data_extract

    @task
    def transform_data(data: list[dict]):
        transformed = []
        for user in data:
            transformed.append(
                {
                    "id": user["id"],
                    "karma": user.get("karma", 0),
                    "about": user.get("about"),
                    "submitted_count": len(user.get("submitted", [])),
                    "created_at": datetime.fromtimestamp(user["created"]).isoformat(),
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
        cursor.close()
        conn.close()

    @task
    def load_mongo(data: list[dict]):

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

        key = f"raw/users/{filename}.json"

        minio_hook.load_string(
            string_data=json_data, bucket_name=bucket_name, key=key, replace=False
        )

    raw_users = extract_data()

    transform_users = transform_data(raw_users)

    load_postgres(transform_users)
    load_mongo(transform_users)
    load_minio(transform_users)


users_etl()
