import json
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

HN_API_URL = "https://hacker-news.firebaseio.com/v0"
ODS_COMMENTS_FQN = "tsd_postgres.ods.public.comments"
BAD_WORDS_URL = (
    "https://raw.githubusercontent.com/dsojevic/profanity-list/main/en.txt"
)
COMMENTS_BUCKET = "hackernews"
HTTP_RETRY_COUNT = 4
HTTP_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)


def _build_http_session() -> requests.Session:
    """Создаёт одну HTTP-сессию с повторами временных сбоев."""
    retry = Retry(
        total=HTTP_RETRY_COUNT,
        connect=HTTP_RETRY_COUNT,
        read=HTTP_RETRY_COUNT,
        status=HTTP_RETRY_COUNT,
        other=HTTP_RETRY_COUNT,
        backoff_factor=0.5,
        status_forcelist=HTTP_RETRY_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


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


def _read_comments_from_manifest(manifest: object) -> list[dict]:
    """Читает комментарии из MinIO и проверяет целостность manifest."""
    if not isinstance(manifest, dict) or set(manifest) != {
        "bucket",
        "key",
        "row_count",
    }:
        raise ValueError("Comment manifest must contain bucket, key and row_count")

    bucket = manifest["bucket"]
    key = manifest["key"]
    row_count = manifest["row_count"]
    if not isinstance(bucket, str) or not bucket:
        raise ValueError("Comment manifest bucket must be a non-empty string")
    if not isinstance(key, str) or not key:
        raise ValueError("Comment manifest key must be a non-empty string")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise ValueError("Comment manifest row_count must be a non-negative integer")

    hook = S3Hook(aws_conn_id="minio_s3")
    comments = json.loads(hook.read_key(key=key, bucket_name=bucket))
    if not isinstance(comments, list):
        raise ValueError("Comment manifest object must contain a JSON list")
    if len(comments) != row_count:
        raise ValueError(
            "Comment manifest row_count does not match the stored JSON object"
        )
    return comments


@dag(
    dag_id="comments_etl",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["hacker-news", "etl", "lab1"],
)
def comments_etl():

    @task
    def validate_story_ids() -> list[int]:
        """Читает и проверяет список публикаций, переданный родительским DAG."""
        return _story_ids_from_conf(get_current_context().get("dag_run"))

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_transform_to_minio(story_ids: list[int]) -> dict:
        """Получает прямые комментарии, очищает их и сохраняет JSON в MinIO."""
        with _build_http_session() as session:
            bad_words_response = session.get(BAD_WORDS_URL, timeout=10)
            bad_words_response.raise_for_status()
            bad_words = [
                word.lower()
                for word in bad_words_response.text.splitlines()
                if word.strip()
            ]

            transformed = []
            loaded_at = datetime.utcnow().isoformat()

            for story_id in story_ids:
                story_response = session.get(
                    f"{HN_API_URL}/item/{story_id}.json",
                    timeout=30,
                )
                story_response.raise_for_status()
                story = story_response.json()
                if story is None:
                    continue

                for comment_id in story.get("kids", []):
                    comment_response = session.get(
                        f"{HN_API_URL}/item/{comment_id}.json",
                        timeout=30,
                    )
                    comment_response.raise_for_status()
                    comment = comment_response.json()
                    if comment is None:
                        continue
                    if comment.get("parent") != story_id:
                        raise ValueError(
                            f"Comment {comment.get('id')} has a parent outside "
                            "the requested cohort"
                        )

                    text = comment.get("text") or ""
                    normalized_text = text.lower()
                    transformed.append(
                        {
                            "id": comment["id"],
                            "parent": comment["parent"],
                            "author": comment.get("by"),
                            "bad_word": any(
                                word in normalized_text for word in bad_words
                            ),
                            "text": comment.get("text"),
                            "created_at": datetime.fromtimestamp(
                                comment["time"]
                            ).isoformat(),
                            "loaded_at": loaded_at,
                        }
                    )

        hook = S3Hook(aws_conn_id="minio_s3")
        client = hook.get_conn()
        existing_buckets = [
            bucket["Name"] for bucket in client.list_buckets()["Buckets"]
        ]
        if COMMENTS_BUCKET not in existing_buckets:
            client.create_bucket(Bucket=COMMENTS_BUCKET)

        run_id = str(get_current_context()["run_id"])
        key = f"raw/comments/{run_id.replace('/', '_')}.json"
        hook.load_string(
            string_data=json.dumps(transformed, ensure_ascii=False, indent=2),
            bucket_name=COMMENTS_BUCKET,
            key=key,
            replace=True,
        )
        return {
            "bucket": COMMENTS_BUCKET,
            "key": key,
            "row_count": len(transformed),
        }

    @task(outlets=[{"tables": [ODS_COMMENTS_FQN]}])
    def load_postgres(manifest: dict) -> None:
        """Читает MinIO manifest и идемпотентно загружает комментарии в PostgreSQL."""
        comments = _read_comments_from_manifest(manifest)
        postgres_hook = PostgresHook(postgres_conn_id="postgres_ods")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
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

            for comment in comments:
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
                        parent = EXCLUDED.parent,
                        author = EXCLUDED.author,
                        bad_word = EXCLUDED.bad_word,
                        text = EXCLUDED.text,
                        created_at = EXCLUDED.created_at,
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
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    @task
    def load_mongo(manifest: dict) -> None:
        """Читает MinIO manifest и идемпотентно загружает комментарии в MongoDB."""
        comments = _read_comments_from_manifest(manifest)
        mongo_hook = MongoHook(mongo_conn_id="mongo_hn")
        collection = mongo_hook.get_collection(
            mongo_collection="comments",
            mongo_db="hackernews",
        )

        for comment in comments:
            collection.update_one(
                {"id": comment["id"]},
                {"$set": comment},
                upsert=True,
            )

    story_ids = validate_story_ids()
    manifest = extract_transform_to_minio(story_ids)

    load_postgres(manifest)
    load_mongo(manifest)


comments_etl()
