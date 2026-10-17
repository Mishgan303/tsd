from __future__ import annotations

from datetime import datetime
from pathlib import Path

from lib.cleaning import as_utc, clean_optional_text, clean_required_text
from lib.models import SourceWindow

SOURCE_TABLES = {
    "stories": "public.stories",
    "users": "public.users",
    "comments": "public.comments",
}

SOURCE_COLUMNS = {
    "stories": (
        "id",
        "title",
        "author",
        "url",
        "score",
        "descendants",
        "created_at",
        "loaded_at",
    ),
    "users": (
        "id",
        "karma",
        "about",
        "submitted_count",
        "created_at",
        "loaded_at",
    ),
    "comments": (
        "id",
        "parent",
        "author",
        "bad_word",
        "text",
        "created_at",
        "loaded_at",
    ),
}


def ensure_schema(conn, schema_path: Path) -> None:
    """Создаёт объекты DDS из SQL-файла в одной транзакции."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(schema_path.read_text())
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def capture_source_windows(conn) -> dict[str, SourceWindow]:
    """Фиксирует watermark и max(loaded_at) для каждого ODS-источника."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT source_name, last_processed_at FROM dds.pipeline_state"
        )
        watermarks = {name: value for name, value in cursor.fetchall()}

        windows: dict[str, SourceWindow] = {}
        for source_name, table_name in SOURCE_TABLES.items():
            cursor.execute("SELECT to_regclass(%s)", (table_name,))
            table_exists = cursor.fetchone()[0] is not None
            upper_bound = None
            if table_exists:
                cursor.execute(
                    f"SELECT max(loaded_at) AT TIME ZONE 'UTC' FROM {table_name}"
                )
                upper_bound = cursor.fetchone()[0]
            windows[source_name] = SourceWindow(
                source_name=source_name,
                lower_bound=(
                    as_utc(watermarks[source_name])
                    if watermarks[source_name] is not None
                    else None
                ),
                upper_bound=as_utc(upper_bound) if upper_bound is not None else None,
            )
    return windows


def source_changed(window: SourceWindow) -> bool:
    """Определяет, содержит ли окно новые данные ODS."""
    return window.upper_bound is not None and (
        window.lower_bound is None or window.upper_bound > window.lower_bound
    )


def select_source_rows(conn, window: SourceWindow) -> list[dict[str, object]]:
    """Читает строки ODS в полуоткрытом инкрементальном окне `(lower, upper]`."""
    if window.source_name not in SOURCE_TABLES:
        raise ValueError(f"unknown source: {window.source_name}")
    if window.upper_bound is None:
        return []

    table_name = SOURCE_TABLES[window.source_name]
    columns = SOURCE_COLUMNS[window.source_name]
    select_columns = ", ".join(columns[:-1])
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT {select_columns}, loaded_at AT TIME ZONE 'UTC' AS loaded_at
            FROM {table_name}
            WHERE (%s::timestamptz IS NULL
                   OR loaded_at AT TIME ZONE 'UTC' > %s::timestamptz)
              AND loaded_at AT TIME ZONE 'UTC' <= %s::timestamptz
            ORDER BY loaded_at, id
            """,
            (window.lower_bound, window.lower_bound, window.upper_bound),
        )
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def window_to_dict(window: SourceWindow) -> dict[str, str | None]:
    """Сериализует окно загрузки в JSON-совместимый словарь для XCom."""
    return {
        "source_name": window.source_name,
        "lower_bound": (
            window.lower_bound.isoformat() if window.lower_bound is not None else None
        ),
        "upper_bound": (
            window.upper_bound.isoformat() if window.upper_bound is not None else None
        ),
    }


def window_from_dict(payload: dict[str, str | None]) -> SourceWindow:
    """Десериализует XCom-словарь в объект `SourceWindow`."""
    source_name = payload.get("source_name")
    if source_name is None:
        raise ValueError("source_name is required")
    lower_bound = payload.get("lower_bound")
    upper_bound = payload.get("upper_bound")
    return SourceWindow(
        source_name=source_name,
        lower_bound=(
            as_utc(datetime.fromisoformat(lower_bound)) if lower_bound else None
        ),
        upper_bound=(
            as_utc(datetime.fromisoformat(upper_bound)) if upper_bound else None
        ),
    )


def _required_integer(value: object, *, field: str, entity: str) -> int:
    """Преобразует обязательное значение в int и отклоняет None и bool."""
    if value is None or isinstance(value, bool):
        raise ValueError(f"invalid {field} for {entity}")
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid {field} for {entity}") from error


def load_dim_users(conn, windows: dict[str, SourceWindow]) -> int:
    """Вставляет или дополняет `dim_user` авторами из всех ODS-источников."""
    candidates: dict[str, datetime | None] = {}
    source_fields = {
        "users": "id",
        "stories": "author",
        "comments": "author",
    }
    for source_name, field_name in source_fields.items():
        window = windows[source_name]
        for row in select_source_rows(conn, window):
            user_id = clean_optional_text(row[field_name])
            if user_id is None:
                continue
            created_at = None
            if source_name == "users" and row["created_at"] is not None:
                created_at = as_utc(row["created_at"])
            if user_id not in candidates or candidates[user_id] is None:
                candidates[user_id] = created_at

    try:
        with conn.cursor() as cursor:
            for user_id in sorted(candidates):
                cursor.execute(
                    """
                    INSERT INTO dds.dim_user (user_id, created_at)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        created_at = COALESCE(
                            dds.dim_user.created_at,
                            EXCLUDED.created_at
                        )
                    """,
                    (user_id, candidates[user_id]),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(candidates)


def load_user_profile_history(conn, users_window: SourceWindow) -> int:
    """Закрывает прежнюю и открывает новую SCD2-версию при изменении `about`."""
    inserted = 0
    rows = select_source_rows(conn, users_window)
    try:
        with conn.cursor() as cursor:
            for row in rows:
                user_id = clean_optional_text(row["id"])
                if user_id is None:
                    raise ValueError("user row has blank id")
                loaded_at = as_utc(row["loaded_at"])
                about = clean_optional_text(row["about"], strip_html=True)
                cursor.execute(
                    "SELECT user_sk FROM dds.dim_user WHERE user_id = %s",
                    (user_id,),
                )
                user = cursor.fetchone()
                if user is None:
                    raise ValueError(f"user dimension is missing for {user_id}")
                user_sk = user[0]
                cursor.execute(
                    """
                    SELECT about, valid_from
                    FROM dds.dim_user_profile_history
                    WHERE user_sk = %s AND valid_to IS NULL
                    FOR UPDATE
                    """,
                    (user_sk,),
                )
                current = cursor.fetchone()
                if current is None:
                    cursor.execute(
                        """
                        INSERT INTO dds.dim_user_profile_history
                            (user_sk, about, valid_from)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_sk, valid_from) DO NOTHING
                        """,
                        (user_sk, about, loaded_at),
                    )
                    inserted += cursor.rowcount
                    continue
                if current[0] == about:
                    continue
                if loaded_at <= current[1]:
                    raise ValueError(
                        f"profile timestamp for {user_id} is not increasing"
                    )
                cursor.execute(
                    """
                    UPDATE dds.dim_user_profile_history
                    SET valid_to = %s
                    WHERE user_sk = %s AND valid_to IS NULL
                    """,
                    (loaded_at, user_sk),
                )
                cursor.execute(
                    """
                    INSERT INTO dds.dim_user_profile_history
                        (user_sk, about, valid_from)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_sk, valid_from) DO NOTHING
                    """,
                    (user_sk, about, loaded_at),
                )
                inserted += cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted


def load_user_snapshots(conn, users_window: SourceWindow) -> int:
    """Записывает снимки karma/submitted_count и их приращения."""
    inserted = 0
    rows = select_source_rows(conn, users_window)
    try:
        with conn.cursor() as cursor:
            for row in rows:
                user_id = clean_optional_text(row["id"])
                if user_id is None:
                    raise ValueError("user row has blank id")
                karma = _required_integer(row["karma"], field="karma", entity=user_id)
                submitted_count = _required_integer(
                    row["submitted_count"],
                    field="submitted_count",
                    entity=user_id,
                )
                if submitted_count < 0:
                    raise ValueError(
                        f"negative submitted_count for user {user_id}"
                    )
                loaded_at = as_utc(row["loaded_at"])
                cursor.execute(
                    "SELECT user_sk FROM dds.dim_user WHERE user_id = %s",
                    (user_id,),
                )
                user = cursor.fetchone()
                if user is None:
                    raise ValueError(f"user dimension is missing for {user_id}")
                user_sk = user[0]
                cursor.execute(
                    """
                    SELECT karma, submitted_count
                    FROM dds.fact_user_snapshot
                    WHERE user_sk = %s AND source_loaded_at < %s
                    ORDER BY source_loaded_at DESC
                    LIMIT 1
                    """,
                    (user_sk, loaded_at),
                )
                previous = cursor.fetchone()
                karma_delta = karma - previous[0] if previous else None
                submitted_delta = (
                    submitted_count - previous[1] if previous else None
                )
                cursor.execute(
                    """
                    INSERT INTO dds.fact_user_snapshot (
                        user_sk,
                        source_loaded_at,
                        karma,
                        karma_delta,
                        submitted_count,
                        submitted_count_delta
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_sk, source_loaded_at) DO NOTHING
                    """,
                    (
                        user_sk,
                        loaded_at,
                        karma,
                        karma_delta,
                        submitted_count,
                        submitted_delta,
                    ),
                )
                inserted += cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted


def _lookup_user_sk(cursor, user_id: str | None) -> int | None:
    """Находит суррогатный ключ автора или возвращает None для отсутствующего ID."""
    if user_id is None:
        return None
    cursor.execute(
        "SELECT user_sk FROM dds.dim_user WHERE user_id = %s",
        (user_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"user dimension is missing for {user_id}")
    return row[0]


def load_dim_stories(conn, stories_window: SourceWindow) -> int:
    """Вставляет или обновляет описательные атрибуты публикаций в `dim_story`."""
    processed = 0
    rows = select_source_rows(conn, stories_window)
    try:
        with conn.cursor() as cursor:
            for row in rows:
                story_id = _required_integer(
                    row["id"], field="id", entity="story"
                )
                try:
                    title = clean_required_text(row["title"])
                except ValueError as error:
                    raise ValueError(f"story {story_id} has blank title") from error
                author = clean_optional_text(row["author"])
                author_user_sk = _lookup_user_sk(cursor, author)
                url = clean_optional_text(row["url"])
                if row["created_at"] is None:
                    raise ValueError(f"story {story_id} has no created_at")
                created_at = as_utc(row["created_at"])
                cursor.execute(
                    """
                    INSERT INTO dds.dim_story (
                        story_id,
                        author_user_sk,
                        title,
                        url,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (story_id) DO UPDATE SET
                        author_user_sk = EXCLUDED.author_user_sk,
                        title = EXCLUDED.title,
                        url = EXCLUDED.url,
                        dds_loaded_at = now()
                    """,
                    (story_id, author_user_sk, title, url, created_at),
                )
                processed += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return processed


def load_comments(conn, comments_window: SourceWindow) -> int:
    """Вставляет новые комментарии первого уровня в `fact_comment`."""
    inserted = 0
    rows = select_source_rows(conn, comments_window)
    try:
        with conn.cursor() as cursor:
            for row in rows:
                comment_id = _required_integer(
                    row["id"], field="id", entity="comment"
                )
                story_id = _required_integer(
                    row["parent"],
                    field="parent",
                    entity=f"comment {comment_id}",
                )
                cursor.execute(
                    "SELECT story_sk FROM dds.dim_story WHERE story_id = %s",
                    (story_id,),
                )
                story = cursor.fetchone()
                if story is None:
                    raise ValueError(
                        f"comment {comment_id} references unknown story {story_id}"
                    )
                author = clean_optional_text(row["author"])
                author_user_sk = _lookup_user_sk(cursor, author)
                bad_word = row["bad_word"]
                if not isinstance(bad_word, bool):
                    raise ValueError(f"invalid bad_word for comment {comment_id}")
                if row["created_at"] is None:
                    raise ValueError(f"comment {comment_id} has no created_at")
                created_at = as_utc(row["created_at"])
                loaded_at = as_utc(row["loaded_at"])
                text = clean_optional_text(row["text"], strip_html=True)
                cursor.execute(
                    """
                    INSERT INTO dds.fact_comment (
                        comment_id,
                        story_sk,
                        author_user_sk,
                        text,
                        bad_word,
                        created_at,
                        source_loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (comment_id) DO NOTHING
                    """,
                    (
                        comment_id,
                        story[0],
                        author_user_sk,
                        text,
                        bad_word,
                        created_at,
                        loaded_at,
                    ),
                )
                inserted += cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted


def load_story_snapshots(conn, stories_window: SourceWindow) -> int:
    """Записывает снимки score/descendants, их дельты и счётчики комментариев."""
    inserted = 0
    rows = select_source_rows(conn, stories_window)
    try:
        with conn.cursor() as cursor:
            for row in rows:
                story_id = _required_integer(
                    row["id"], field="id", entity="story"
                )
                score = _required_integer(
                    row["score"], field="score", entity=f"story {story_id}"
                )
                descendants = _required_integer(
                    row["descendants"],
                    field="descendants",
                    entity=f"story {story_id}",
                )
                if score < 0:
                    raise ValueError(f"negative score for story {story_id}")
                if descendants < 0:
                    raise ValueError(f"negative descendants for story {story_id}")
                loaded_at = as_utc(row["loaded_at"])
                cursor.execute(
                    "SELECT story_sk FROM dds.dim_story WHERE story_id = %s",
                    (story_id,),
                )
                story = cursor.fetchone()
                if story is None:
                    raise ValueError(f"story dimension is missing for {story_id}")
                story_sk = story[0]
                cursor.execute(
                    """
                    SELECT score, descendants
                    FROM dds.fact_story_snapshot
                    WHERE story_sk = %s AND source_loaded_at < %s
                    ORDER BY source_loaded_at DESC
                    LIMIT 1
                    """,
                    (story_sk, loaded_at),
                )
                previous = cursor.fetchone()
                score_delta = score - previous[0] if previous else None
                descendants_delta = (
                    descendants - previous[1] if previous else None
                )
                cursor.execute(
                    """
                    SELECT count(*), count(*) FILTER (WHERE bad_word)
                    FROM dds.fact_comment
                    WHERE story_sk = %s
                    """,
                    (story_sk,),
                )
                loaded_count, bad_word_count = cursor.fetchone()
                cursor.execute(
                    """
                    INSERT INTO dds.fact_story_snapshot (
                        story_sk,
                        source_loaded_at,
                        score,
                        score_delta,
                        descendants,
                        descendants_delta,
                        loaded_comments_count,
                        bad_word_comments_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (story_sk, source_loaded_at) DO NOTHING
                    """,
                    (
                        story_sk,
                        loaded_at,
                        score,
                        score_delta,
                        descendants,
                        descendants_delta,
                        loaded_count,
                        bad_word_count,
                    ),
                )
                inserted += cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted


def update_pipeline_state(
    conn, windows: dict[str, SourceWindow]
) -> int:
    """Продвигает watermark каждого изменившегося ODS-источника."""
    updated = 0
    try:
        with conn.cursor() as cursor:
            for source_name in sorted(windows):
                window = windows[source_name]
                if source_name not in SOURCE_TABLES:
                    raise ValueError(f"unknown source: {source_name}")
                if not source_changed(window):
                    continue
                cursor.execute(
                    """
                    UPDATE dds.pipeline_state
                    SET last_processed_at = %s,
                        updated_at = now()
                    WHERE source_name = %s
                      AND (
                          last_processed_at IS NULL
                          OR last_processed_at < %s
                      )
                    """,
                    (window.upper_bound, source_name, window.upper_bound),
                )
                updated += cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return updated
