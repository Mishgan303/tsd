import json
import statistics
import time

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from pymongo import UpdateOne

# ============================================================
# НАСТРОЙКИ
# ============================================================

DATA_PATH = "/opt/airflow/dags/data/hackernews_stories_100k.json"

POSTGRES_TABLE = "benchmark_stories"

MONGO_DB = "hackernews"
MONGO_COLLECTION = "benchmark_stories"

MINIO_BUCKET = "hackernews"
MINIO_KEY = "benchmark/stories_100k.ndjson"

WRITE_REPEATS = 3
UPSERT_REPEATS = 3
READ_REPEATS = 10
FULL_READ_REPEATS = 5

UPSERT_COUNT = 10_000
FILTER_SCORE = 100

# Для одинаковости данных во всех хранилищах.
BENCHMARK_LOADED_AT = "2026-09-18T00:00:00+00:00"

EXPECTED_RECORDS = 62_872
# ============================================================
# ОБЩИЕ ФУНКЦИИ
# ============================================================


def load_records():
    with open(DATA_PATH, "r", encoding="utf-8") as file:
        raw = json.load(file)

    records = []

    for row in raw:
        records.append(
            {
                "id": int(row["id"]),
                "title": row.get("title"),
                "author": row.get("author") or row.get("by"),
                "url": row.get("url"),
                # BigQuery JSON может сохранить INTEGER как строку.
                "score": int(row["score"]) if row.get("score") is not None else 0,
                "descendants": (
                    int(row["descendants"]) if row.get("descendants") is not None else 0
                ),
                "created_at": row.get("created_at") or row.get("timestamp"),
                "loaded_at": BENCHMARK_LOADED_AT,
            }
        )

    return records


def median(values):
    return statistics.median(values)


def make_updates(records, delta):
    """
    Имитируем повторный ETL:
    первые 10k stories уже существуют,
    но score / descendants изменились.
    """

    result = []

    for row in records[:UPSERT_COUNT]:
        updated = row.copy()

        updated["score"] += delta
        updated["descendants"] += delta

        result.append(updated)

    return result


# ============================================================
# POSTGRESQL
# ============================================================


def benchmark_postgres():

    records = load_records()

    hook = PostgresHook(postgres_conn_id="postgres_ods")

    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            id BIGINT PRIMARY KEY,
            title TEXT,
            author TEXT,
            url TEXT,
            score INTEGER,
            descendants INTEGER,
            created_at TIMESTAMPTZ,
            loaded_at TIMESTAMPTZ
        );
        """
    )

    conn.commit()

    values = [
        (
            x["id"],
            x["title"],
            x["author"],
            x["url"],
            x["score"],
            x["descendants"],
            x["created_at"],
            x["loaded_at"],
        )
        for x in records
    ]

    # --------------------------------------------------------
    # WRITE 100k
    # --------------------------------------------------------

    write_times = []

    for _ in range(WRITE_REPEATS):
        # Очистку не измеряем.
        cursor.execute(f"TRUNCATE TABLE {POSTGRES_TABLE}")

        conn.commit()

        start = time.perf_counter()

        cursor.executemany(
            f"""
            INSERT INTO {POSTGRES_TABLE}
            (
                id,
                title,
                author,
                url,
                score,
                descendants,
                created_at,
                loaded_at
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            """,
            values,
        )

        conn.commit()

        write_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # UPSERT 10k
    # --------------------------------------------------------

    upsert_times = []

    for repeat in range(UPSERT_REPEATS):
        updates = make_updates(
            records,
            repeat + 1,
        )

        update_values = [
            (
                x["id"],
                x["title"],
                x["author"],
                x["url"],
                x["score"],
                x["descendants"],
                x["created_at"],
                x["loaded_at"],
            )
            for x in updates
        ]

        start = time.perf_counter()

        cursor.executemany(
            f"""
            INSERT INTO {POSTGRES_TABLE}
            (
                id,
                title,
                author,
                url,
                score,
                descendants,
                created_at,
                loaded_at
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )

            ON CONFLICT (id)
            DO UPDATE SET
                score = EXCLUDED.score,
                descendants = EXCLUDED.descendants,
                loaded_at = EXCLUDED.loaded_at
            """,
            update_values,
        )

        conn.commit()

        upsert_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # READ BY ID
    # --------------------------------------------------------

    test_id = records[len(records) // 2]["id"]

    read_times = []

    for _ in range(READ_REPEATS):
        start = time.perf_counter()

        cursor.execute(
            f"""
            SELECT *
            FROM {POSTGRES_TABLE}
            WHERE id = %s
            """,
            (test_id,),
        )

        cursor.fetchone()

        read_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # FILTER
    # --------------------------------------------------------

    filter_times = []

    for _ in range(READ_REPEATS):
        start = time.perf_counter()

        cursor.execute(
            f"""
            SELECT *
            FROM {POSTGRES_TABLE}
            WHERE score >= %s
            """,
            (FILTER_SCORE,),
        )

        cursor.fetchall()

        filter_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # FULL READ
    # --------------------------------------------------------

    full_times = []

    for _ in range(FULL_READ_REPEATS):
        start = time.perf_counter()

        cursor.execute(f"SELECT * FROM {POSTGRES_TABLE}")

        cursor.fetchall()

        full_times.append(time.perf_counter() - start)

    # Размер таблицы + индексов.
    cursor.execute(
        """
        SELECT pg_total_relation_size(%s::regclass)
        """,
        (POSTGRES_TABLE,),
    )

    storage_size = cursor.fetchone()[0]

    # --------------------------------------------------------
    # SCHEMA FLEXIBILITY
    # --------------------------------------------------------

    # PostgreSQL требует изменить схему таблицы.
    cursor.execute(
        f"""
        ALTER TABLE {POSTGRES_TABLE}
        ADD COLUMN IF NOT EXISTS category TEXT;
        """
    )

    conn.commit()

    schema_flexibility = "Requires ALTER TABLE before adding a new field"

    usability = (
        "SQL, strict schema, indexes, joins and constraints; "
        "convenient for structured analytical data"
    )

    conn.close()

    return {
        "storage": "PostgreSQL",
        "write": median(write_times),
        "upsert": median(upsert_times),
        "read_by_id": median(read_times),
        "filter": median(filter_times),
        "full_read": median(full_times),
        "storage_bytes": storage_size,
        "schema_flexibility": schema_flexibility,
        "usability": usability,
    }


# ============================================================
# MONGODB
# ============================================================


def benchmark_mongo():

    records = load_records()

    hook = MongoHook(mongo_conn_id="mongo_hn")

    collection = hook.get_collection(
        mongo_collection=MONGO_COLLECTION,
        mongo_db=MONGO_DB,
    )

    # Начинаем с чистой collection.
    collection.drop()

    # Аналог PRIMARY KEY в PostgreSQL.
    collection.create_index(
        "id",
        unique=True,
    )

    # --------------------------------------------------------
    # WRITE
    # --------------------------------------------------------

    write_times = []

    for _ in range(WRITE_REPEATS):
        collection.delete_many({})

        start = time.perf_counter()

        collection.insert_many(
            records,
            ordered=False,
        )

        write_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # UPSERT
    # --------------------------------------------------------

    upsert_times = []

    for repeat in range(UPSERT_REPEATS):
        updates = make_updates(
            records,
            repeat + 1,
        )

        operations = [
            UpdateOne(
                {"id": row["id"]},
                {"$set": row},
                upsert=True,
            )
            for row in updates
        ]

        start = time.perf_counter()

        collection.bulk_write(
            operations,
            ordered=False,
        )

        upsert_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # READ BY ID
    # --------------------------------------------------------

    test_id = records[len(records) // 2]["id"]

    read_times = []

    for _ in range(READ_REPEATS):
        start = time.perf_counter()

        collection.find_one({"id": test_id})

        read_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # FILTER
    # --------------------------------------------------------

    filter_times = []

    for _ in range(READ_REPEATS):
        start = time.perf_counter()

        list(collection.find({"score": {"$gte": FILTER_SCORE}}))

        filter_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # FULL READ
    # --------------------------------------------------------

    full_times = []

    for _ in range(FULL_READ_REPEATS):
        start = time.perf_counter()

        list(collection.find({}))

        full_times.append(time.perf_counter() - start)

    stats = collection.database.command(
        "collStats",
        MONGO_COLLECTION,
    )

    storage_size = stats.get("storageSize", 0) + stats.get("totalIndexSize", 0)

    collection.update_one(
        {"id": records[0]["id"]},
        {"$set": {"category": "test"}},
    )

    schema_flexibility = "New fields can be added without schema migration"

    usability = (
        "Flexible JSON-like documents and simple queries; "
        "convenient when record structure changes"
    )

    return {
        "storage": "MongoDB",
        "write": median(write_times),
        "upsert": median(upsert_times),
        "read_by_id": median(read_times),
        "filter": median(filter_times),
        "full_read": median(full_times),
        "storage_bytes": storage_size,
        "schema_flexibility": schema_flexibility,
        "usability": usability,
    }


# ============================================================
# MINIO
# ============================================================


def benchmark_minio():

    records = load_records()

    hook = S3Hook(aws_conn_id="minio_s3")

    client = hook.get_conn()

    buckets = [x["Name"] for x in client.list_buckets()["Buckets"]]

    if MINIO_BUCKET not in buckets:
        client.create_bucket(Bucket=MINIO_BUCKET)

    def to_ndjson(data):
        """
        MinIO хранит весь dataset одним объектом.
        """

        return (
            "\n".join(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                for row in data
            )
            + "\n"
        ).encode("utf-8")

    payload = to_ndjson(records)

    # --------------------------------------------------------
    # WRITE
    # --------------------------------------------------------

    write_times = []

    for _ in range(WRITE_REPEATS):
        start = time.perf_counter()

        client.put_object(
            Bucket=MINIO_BUCKET,
            Key=MINIO_KEY,
            Body=payload,
            ContentType="application/x-ndjson",
        )

        write_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # UPSERT
    # --------------------------------------------------------
    #
    # В object storage нет UPDATE строки.
    # Поэтому изменяем данные и перезаписываем весь объект.
    # --------------------------------------------------------

    upsert_times = []

    for repeat in range(UPSERT_REPEATS):
        changed = list(records)

        changed[:UPSERT_COUNT] = make_updates(
            records,
            repeat + 1,
        )

        changed_payload = to_ndjson(changed)

        start = time.perf_counter()

        client.put_object(
            Bucket=MINIO_BUCKET,
            Key=MINIO_KEY,
            Body=changed_payload,
            ContentType="application/x-ndjson",
        )

        upsert_times.append(time.perf_counter() - start)

    # После upsert читаем актуальный объект.
    test_id = records[len(records) // 2]["id"]

    # --------------------------------------------------------
    # READ BY ID
    # --------------------------------------------------------

    read_times = []

    for _ in range(READ_REPEATS):
        start = time.perf_counter()

        response = client.get_object(
            Bucket=MINIO_BUCKET,
            Key=MINIO_KEY,
        )

        body = response["Body"]

        try:
            for line in body.iter_lines():
                if not line:
                    continue

                row = json.loads(line)

                if row["id"] == test_id:
                    break

        finally:
            body.close()

        read_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # FILTER
    # --------------------------------------------------------

    filter_times = []

    for _ in range(READ_REPEATS):
        start = time.perf_counter()

        response = client.get_object(
            Bucket=MINIO_BUCKET,
            Key=MINIO_KEY,
        )

        body = response["Body"]

        try:
            for line in body.iter_lines():
                if not line:
                    continue

                row = json.loads(line)

                if row["score"] >= FILTER_SCORE:
                    pass

        finally:
            body.close()

        filter_times.append(time.perf_counter() - start)

    # --------------------------------------------------------
    # FULL READ
    # --------------------------------------------------------

    full_times = []

    for _ in range(FULL_READ_REPEATS):
        start = time.perf_counter()

        response = client.get_object(
            Bucket=MINIO_BUCKET,
            Key=MINIO_KEY,
        )

        body = response["Body"]

        try:
            for line in body.iter_lines():
                if line:
                    json.loads(line)

        finally:
            body.close()

        full_times.append(time.perf_counter() - start)

    info = client.head_object(
        Bucket=MINIO_BUCKET,
        Key=MINIO_KEY,
    )

    storage_size = info["ContentLength"]

    # У MinIO вообще нет схемы БД.
    # Любое поле можно добавить в JSON,
    # но для изменения существующего файла
    # объект приходится перезаписывать.

    schema_flexibility = (
        "No database schema, but changing records requires rewriting the object"
    )

    usability = (
        "Simple object storage and S3 API; "
        "good for files and raw data, "
        "but inconvenient for filtering and row-level updates"
    )

    return {
        "storage": "MinIO",
        "write": median(write_times),
        "upsert": median(upsert_times),
        "read_by_id": median(read_times),
        "filter": median(filter_times),
        "full_read": median(full_times),
        "storage_bytes": storage_size,
        "schema_flexibility": schema_flexibility,
        "usability": usability,
    }


# ============================================================
# AIRFLOW DAG
# ============================================================


@dag(
    dag_id="benchmark_db",
    schedule=None,
    catchup=False,
    tags=["benchmark", "hacker-news", "lab1"],
)
def benchmark_db():

    @task
    def check_dataset():
        """
        Проверяем dataset до benchmark.
        Большие данные через XCom не передаём.
        """

        records = load_records()

        if len(records) != EXPECTED_RECORDS:
            raise ValueError(f"Expected {EXPECTED_RECORDS} records, got {len(records)}")

        print(f"Dataset OK: {len(records):,} stories")

    @task
    def postgres_task():
        return benchmark_postgres()

    @task
    def mongo_task(previous):
        # previous нужен только для последовательности DAG.
        return benchmark_mongo()

    @task
    def minio_task(previous):
        return benchmark_minio()

    @task
    def collect_results(
        postgres,
        mongo,
        minio,
    ):

        results = [
            postgres,
            mongo,
            minio,
        ]

        print()
        print("=" * 80)
        print("HACKER NEWS DATABASE BENCHMARK")
        print("=" * 80)

        for result in results:
            print()
            print(result["storage"])
            print("-" * 40)

            print(f"WRITE:       {result['write']:.6f} s")

            print(f"UPSERT:      {result['upsert']:.6f} s")

            print(f"READ BY ID:  {result['read_by_id']:.6f} s")

            print(f"FILTER:      {result['filter']:.6f} s")

            print(f"FULL READ:   {result['full_read']:.6f} s")

            print(f"SIZE:        {result['storage_bytes']:,} bytes")
            print(f"SCHEMA:      {result['schema_flexibility']}")

            print(f"USABILITY:   {result['usability']}")

            print()
            print("=" * 80)

    # Последовательный benchmark:
    #
    # PostgreSQL → MongoDB → MinIO
    #
    # Они не конкурируют друг с другом за ресурсы.

    dataset = check_dataset()

    pg = postgres_task()

    dataset >> pg

    mongo = mongo_task(pg)

    minio = minio_task(mongo)

    collect_results(
        pg,
        mongo,
        minio,
    )


benchmark_db()
