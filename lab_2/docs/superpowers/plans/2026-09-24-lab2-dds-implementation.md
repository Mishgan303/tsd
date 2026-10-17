# Lab 2 DDS and Data Quality Implementation Plan

> **Superseded:** схема с отдельным Airflow ЛР2 заменена единым корневым Compose. Актуальная спецификация: `docs/superpowers/specs/2026-09-25-shared-compose-design.md`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Построить в `lab_2` автономный Airflow-процесс ODS → DDS, который раз в 8 часов сохраняет историю публикаций и пользователей, ведёт SCD2 для профиля и запускает отдельную проверку качества данных.

**Architecture:** ЛР-2 запускает собственный Airflow 3.3.1 с LocalExecutor и собственной metadata database, но через внешнюю сеть `lab_1_default` читает и дополняет ту же PostgreSQL `postgres-ods` из ЛР-1. Инкрементальная загрузка фиксирует верхние границы `loaded_at`, записывает нормализованные измерения и факты в схему `dds`, обновляет watermarks только после успешной загрузки и затем запускает отдельный DQ DAG.

**Tech Stack:** Python 3.10+ (minor container Python задаёт официальный образ Apache Airflow 3.3.1), PostgreSQL 16, `apache-airflow-providers-postgres==7.0.2`, `apache-airflow-providers-standard==1.19.0`, psycopg, pytest, Ruff, Docker Compose.

**Spec:** `lab_2/docs/superpowers/specs/2026-09-24-lab2-dds-design.md`

## Global Constraints

- Рабочая область реализации — только `lab_2`, кроме одного согласованного изменения фильтра в `lab_1/dags/user_etl.py`.
- Источники — только `public.stories`, `public.users`, `public.comments`; комментарии — только первого уровня, уже отобранные ЛР-1.
- DDS хранится в схеме `dds` той же PostgreSQL; отдельную PostgreSQL с копией ODS не создавать.
- ЛР-2 имеет отдельные Airflow metadata database и UI на порту `8083`; к ODS подключается через external network `lab_1_default` и hostname `postgres-ods`.
- Расписание `dds_incremental_etl`: `0 */8 * * *`, timezone `Europe/Moscow`, `catchup=False`, `max_active_runs=1`.
- `karma >= 300` применяется только в аналитических запросах; ODS должна сохранять всех найденных авторов top stories.
- Таблицы `dim_word`, `bridge_story_word` и любые другие сущности анализа слов не создавать.
- Наборы строк через XCom не передавать: только ISO timestamps, watermarks, cutoffs и небольшие счётчики.
- Наивные timestamps из ODS интерпретировать как UTC и хранить в DDS как `TIMESTAMPTZ`.
- Watermarks обновлять только после успешной загрузки всех изменившихся источников; повторная обработка окна обязана быть идемпотентной.
- ЛР-1 перезаписывает текущее состояние ODS каждый час, поэтому запуск ЛР-2 раз в 8 часов сохраняет наблюдаемые 8-часовые снимки, но не может восстановить промежуточные часовые состояния.
- Реализация должна оставаться учебной и объяснимой: без ORM, миграционного фреймворка, очереди, Celery, Redis и дополнительных хранилищ.
- Корень `/Users/mishgan/Desktop/mag/perv/tsd` сейчас не является Git-репозиторием; команды commit в плане выполнять только если к моменту реализации пользователь сам создаст или подключит репозиторий.

## Review Focus

- Первый запуск при ещё не созданных ODS-таблицах должен создать `dds`, признать источники пустыми и завершиться без ошибки; это фиксирует тест Task 4.
- Строка, появившаяся после зафиксированного cutoff, должна остаться на следующий запуск, а не потеряться; это фиксирует тест Task 4.
- Повтор одного и того же окна, включая одинаковый `source_loaded_at`, не должен менять количество измерений, снимков, комментариев и SCD2-версий; это фиксируют тесты Tasks 5 и 6.
- Переход `about` между `NULL`, пустым HTML и непустым текстом должен создавать версию только при изменении очищенного значения; это фиксирует тест Task 5.
- Комментарий с неизвестным `parent` не должен молча исчезать или продвигать watermark; загрузка обязана упасть до `update_pipeline_state`; это фиксируют тесты Task 6 и Task 8.

---

## File Map

### Изменяемый файл ЛР-1

- `lab_1/dags/user_etl.py` — убрать фильтр `karma >= 500`, не меняя извлечение, остальные хранилища и расписание.

### Runtime и конфигурация ЛР-2

- `lab_2/Dockerfile` — Airflow 3.3.1 с зафиксированными provider-зависимостями.
- `lab_2/docker-compose.yaml` — metadata PostgreSQL и минимальный LocalExecutor Airflow stack, подключённый к `lab_1_default`.
- `lab_2/docker-compose.test.yaml` — отдельная PostgreSQL 16 для воспроизводимых интеграционных тестов.
- `lab_2/.env.example` — несекретные примеры UID, Airflow admin и ODS connection URI.
- `lab_2/.gitignore` — логи, caches, локальный `.env`.
- `lab_2/requirements.txt` — runtime provider-зависимости.
- `lab_2/requirements-dev.txt` — pytest, Ruff и psycopg для локальных тестов.
- `lab_2/pyproject.toml` — конфигурация pytest и Ruff.

### SQL и библиотека загрузки

- `lab_2/sql/create_dds_schema.sql` — все восемь таблиц, ограничения, индексы и начальные строки `pipeline_state`.
- `lab_2/sql/analytics_examples.sql` — запросы роста score, karma и интереса к публикациям с порогом `karma >= 300`.
- `lab_2/dags/.airflowignore` — не давать scheduler разбирать библиотечные модули как DAG-файлы.
- `lab_2/dags/lib/__init__.py` — пакет внутренних функций DAG.
- `lab_2/dags/lib/models.py` — `SourceWindow` и `QualityResult`.
- `lab_2/dags/lib/cleaning.py` — очистка текста и UTC-конвертация.
- `lab_2/dags/lib/incremental.py` — schema bootstrap, source windows, loaders и watermark update.
- `lab_2/dags/lib/quality.py` — SQL-проверки, сохранение результатов и итоговый pass/fail.

### DAG и документация

- `lab_2/dags/dds_incremental_etl.py` — основной DAG `0 */8 * * *`.
- `lab_2/dags/dds_data_quality.py` — отдельный ручной DQ DAG, запускаемый основным DAG.
- `lab_2/README.md` — архитектура, запуск, демонстрация и известное ограничение 8-часовой выборки.

### Тесты

- `lab_2/tests/conftest.py` — DSN, очистка БД и фикстуры ODS.
- `lab_2/tests/test_database_fixture.py` — smoke-test тестовой PostgreSQL.
- `lab_2/tests/test_lab1_user_filter.py` — регрессия: ODS больше не отбрасывает пользователей по karma.
- `lab_2/tests/test_schema.py` — таблицы, PK/FK, checks и partial unique index.
- `lab_2/tests/test_cleaning.py` — HTML, пробелы, пустые значения, UTC.
- `lab_2/tests/test_incremental_windows.py` — cutoffs, пустые источники и deferred rows.
- `lab_2/tests/test_user_loading.py` — пользователи, SCD2, snapshots и deltas.
- `lab_2/tests/test_story_loading.py` — stories, comments, агрегаты, retries и watermarks.
- `lab_2/tests/test_dag_structure.py` — расписание, timezone, порядок задач и DQ trigger.
- `lab_2/tests/test_quality.py` — DQ pass/fail, сохранение результатов и аномалии.

## Task 1: Создать воспроизводимый test harness

**Files:**
- Create: `lab_2/pyproject.toml`
- Create: `lab_2/requirements.txt`
- Create: `lab_2/requirements-dev.txt`
- Create: `lab_2/docker-compose.test.yaml`
- Create: `lab_2/.gitignore`
- Create: `lab_2/tests/conftest.py`
- Create: `lab_2/tests/test_database_fixture.py`

**Interfaces:**
- Consumes: Docker и свободный порт `55432` на host.
- Produces: fixture `db_conn() -> psycopg.Connection`, fixture `install_ods_tables(db_conn) -> None`, fixtures `schema_path() -> Path` and `schema_sql(schema_path) -> str`, environment variable `TEST_DATABASE_DSN`.

- [ ] **Step 1: Зафиксировать runtime и dev dependencies**

`requirements.txt`:

```text
apache-airflow-providers-postgres==7.0.2
apache-airflow-providers-standard==1.19.0
```

`requirements-dev.txt`:

```text
psycopg[binary]==3.3.5
pytest>=8.4,<9
ruff>=0.13,<1
```

`pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["dags"]

[tool.ruff]
target-version = "py310"
line-length = 88

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
```

- [ ] **Step 2: Описать изолированную тестовую PostgreSQL**

`docker-compose.test.yaml` должен содержать один сервис:

```yaml
services:
  postgres-test:
    image: postgres:16
    environment:
      POSTGRES_USER: ods_test
      POSTGRES_PASSWORD: ods_test
      POSTGRES_DB: ods_test
    ports:
      - "55432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ods_test -d ods_test"]
      interval: 2s
      timeout: 3s
      retries: 20
```

- [ ] **Step 3: Написать fixture и первый smoke-test**

`tests/conftest.py` должен подключаться к `TEST_DATABASE_DSN`, а перед каждым DB-тестом выполнять:

```python
conn.execute("DROP SCHEMA IF EXISTS dds CASCADE")
for table in ("comments", "stories", "users"):
    conn.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE")
conn.commit()
```

`install_ods_tables` создаёт точные текущие ODS-контракты:

```sql
CREATE TABLE public.users (
    id TEXT PRIMARY KEY,
    karma INTEGER,
    about TEXT,
    submitted_count INTEGER,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP
);
CREATE TABLE public.stories (
    id BIGINT PRIMARY KEY,
    title TEXT,
    author TEXT,
    url TEXT,
    score INTEGER,
    descendants INTEGER,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP
);
CREATE TABLE public.comments (
    id BIGINT PRIMARY KEY,
    parent BIGINT,
    author TEXT,
    bad_word BOOLEAN,
    text TEXT,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP
);
```

Добавить ленивые fixtures, которые понадобятся после Task 3:

```python
@pytest.fixture
def schema_path() -> Path:
    return Path(__file__).parents[1] / "sql/create_dds_schema.sql"


@pytest.fixture
def schema_sql(schema_path: Path) -> str:
    return schema_path.read_text()
```

Smoke-test:

```python
def test_database_fixture_is_clean(db_conn, install_ods_tables):
    assert db_conn.execute(
        "SELECT to_regclass('public.stories')"
    ).fetchone()[0] == "stories"
    assert db_conn.execute("SELECT count(*) FROM public.stories").fetchone()[0] == 0
```

- [ ] **Step 4: Запустить test harness**

Run:

```bash
cd lab_2
docker compose -f docker-compose.test.yaml up -d --wait
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_database_fixture.py -v
```

Expected: `1 passed`.

- [ ] **Step 5: Добавить локальные артефакты в `.gitignore`**

```gitignore
.env
.venv/
__pycache__/
.pytest_cache/
.ruff_cache/
logs/
*.pyc
```

- [ ] **Step 6: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_2/pyproject.toml lab_2/requirements.txt lab_2/requirements-dev.txt lab_2/docker-compose.test.yaml lab_2/.gitignore lab_2/tests
git commit -m "test: add lab2 postgres harness"
```

Expected: commit создаётся только после `git rev-parse --is-inside-work-tree` со значением `true`; иначе этот шаг пропускается без `git init`.

## Task 2: Убрать ODS-фильтр karma без побочных изменений

**Files:**
- Modify: `lab_1/dags/user_etl.py:50-53`
- Create: `lab_2/tests/test_lab1_user_filter.py`

**Interfaces:**
- Consumes: текущий `extract_data` DAG ЛР-1.
- Produces: ODS получает любого существующего автора top story независимо от `karma`; порог остаётся только в `analytics_examples.sql`.

- [ ] **Step 1: Написать статический регрессионный тест**

```python
from pathlib import Path


def test_user_etl_does_not_filter_authors_by_karma():
    source = (Path(__file__).parents[2] / "lab_1/dags/user_etl.py").read_text()
    assert 'if user and user.get("karma", 0) >= 500:' not in source
    assert "if user:" in source
    assert "data_extract.append(user)" in source
```

- [ ] **Step 2: Запустить тест и увидеть ожидаемое падение**

Run: `cd lab_2 && .venv/bin/pytest tests/test_lab1_user_filter.py -v`

Expected: FAIL на первом assertion, потому что в текущем файле есть `karma >= 500`.

- [ ] **Step 3: Выполнить минимальное изменение**

Заменить только условие:

```python
if user:
    data_extract.append(user)
```

Не менять `[:20]`, MongoDB, MinIO, PostgreSQL SQL и расписание ЛР-1.

- [ ] **Step 4: Запустить регрессионный тест**

Run: `cd lab_2 && .venv/bin/pytest tests/test_lab1_user_filter.py -v`

Expected: PASS.

- [ ] **Step 5: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_1/dags/user_etl.py lab_2/tests/test_lab1_user_filter.py
git commit -m "fix: keep all top story authors in ods"
```

## Task 3: Создать DDS schema contract

**Files:**
- Create: `lab_2/sql/create_dds_schema.sql`
- Create: `lab_2/tests/test_schema.py`

**Interfaces:**
- Consumes: PostgreSQL 16 connection.
- Produces: `ensure_schema(conn, schema_path: Path) -> None` in Task 4 relies on idempotent SQL; таблицы `dds.dim_user`, `dds.dim_user_profile_history`, `dds.fact_user_snapshot`, `dds.dim_story`, `dds.fact_story_snapshot`, `dds.fact_comment`, `dds.pipeline_state`, `dds.data_quality_results`.

- [ ] **Step 1: Написать failing schema tests**

Тест должен выполнить будущий SQL дважды и проверить точный набор таблиц:

```python
EXPECTED_TABLES = {
    "data_quality_results",
    "dim_story",
    "dim_user",
    "dim_user_profile_history",
    "fact_comment",
    "fact_story_snapshot",
    "fact_user_snapshot",
    "pipeline_state",
}


def test_schema_is_idempotent_and_complete(db_conn, schema_sql):
    db_conn.execute(schema_sql)
    db_conn.execute(schema_sql)
    rows = db_conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'dds'"
    ).fetchall()
    assert {row[0] for row in rows} == EXPECTED_TABLES
```

Добавить отдельные тесты, которые:

- вставляют два `dim_user` с одинаковым `user_id` и ожидают `UniqueViolation`;
- вставляют две открытые SCD2-версии одного `user_sk` и ожидают `UniqueViolation`;
- вставляют отрицательные `score`, `descendants`, `submitted_count`, `loaded_comments_count` и ожидают `CheckViolation`;
- проверяют ровно три seeded строки `pipeline_state`: `stories`, `users`, `comments` с `last_processed_at IS NULL`.

- [ ] **Step 2: Запустить тест и подтвердить красное состояние**

Run: `cd lab_2 && TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_schema.py -v`

Expected: FAIL с `FileNotFoundError` для `sql/create_dds_schema.sql`.

- [ ] **Step 3: Реализовать таблицы измерений и SCD2**

В `create_dds_schema.sql` создать `dds.dim_user`, `dds.dim_user_profile_history`, `dds.dim_story` в соответствии со spec. Обязательные фрагменты:

```sql
CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dim_user (
    user_sk BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ,
    dds_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dds.dim_user_profile_history (
    user_profile_sk BIGSERIAL PRIMARY KEY,
    user_sk BIGINT NOT NULL REFERENCES dds.dim_user(user_sk),
    about TEXT,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    UNIQUE (user_sk, valid_from),
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_user_profile_open
ON dds.dim_user_profile_history (user_sk)
WHERE valid_to IS NULL;
```

- [ ] **Step 4: Реализовать факты, checks и technical tables**

Добавить обе snapshot-таблицы с составными PK, `fact_comment` с PK `comment_id`, FK из facts в dimensions и checks `>= 0`. Технические таблицы и seed:

```sql
CREATE TABLE IF NOT EXISTS dds.pipeline_state (
    source_name TEXT PRIMARY KEY,
    last_processed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_name IN ('stories', 'users', 'comments'))
);

INSERT INTO dds.pipeline_state (source_name, last_processed_at)
VALUES ('stories', NULL), ('users', NULL), ('comments', NULL)
ON CONFLICT (source_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS dds.data_quality_results (
    run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status BOOLEAN NOT NULL,
    actual_value TEXT NOT NULL,
    expected_rule TEXT NOT NULL,
    details TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);
```

- [ ] **Step 5: Запустить schema tests**

Run: `cd lab_2 && TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_schema.py -v`

Expected: все schema tests PASS.

- [ ] **Step 6: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_2/sql/create_dds_schema.sql lab_2/tests/test_schema.py
git commit -m "feat: define normalized dds schema"
```

## Task 4: Реализовать очистку и фиксированные source windows

**Files:**
- Create: `lab_2/dags/.airflowignore`
- Create: `lab_2/dags/lib/__init__.py`
- Create: `lab_2/dags/lib/models.py`
- Create: `lab_2/dags/lib/cleaning.py`
- Create: `lab_2/dags/lib/incremental.py`
- Create: `lab_2/tests/test_cleaning.py`
- Create: `lab_2/tests/test_incremental_windows.py`

**Interfaces:**
- Consumes: DB-API 2.0 connection с `cursor`, `commit`, `rollback`, schema SQL from Task 3.
- Produces: `SourceWindow(source_name: str, lower_bound: datetime | None, upper_bound: datetime | None)`, `clean_optional_text(value: str | None, *, strip_html: bool = False) -> str | None`, `clean_required_text(value: str | None) -> str`, `as_utc(value: datetime) -> datetime`, `ensure_schema(conn, schema_path: Path) -> None`, `capture_source_windows(conn) -> dict[str, SourceWindow]`, `select_source_rows(conn, window: SourceWindow) -> list[dict[str, object]]`, `window_to_dict(window) -> dict[str, str | None]`, `window_from_dict(payload) -> SourceWindow`, `source_changed(window) -> bool`.

- [ ] **Step 1: Написать cleaning tests**

```python
from datetime import UTC, datetime

from lib.cleaning import as_utc, clean_optional_text, clean_required_text


def test_clean_optional_text_strips_html_entities_and_spaces():
    assert clean_optional_text(" <p>Hello&nbsp;  world</p> ", strip_html=True) == "Hello world"


def test_clean_optional_text_maps_blank_html_to_none():
    assert clean_optional_text("<p> &nbsp; </p>", strip_html=True) is None


def test_required_text_rejects_blank_value():
    with pytest.raises(ValueError, match="required text is blank"):
        clean_required_text("   ")


def test_as_utc_treats_naive_ods_value_as_utc():
    value = as_utc(datetime(2026, 9, 24, 8, 0))
    assert value == datetime(2026, 9, 24, 8, 0, tzinfo=UTC)
```

- [ ] **Step 2: Написать source-window tests, включая Review Focus cases**

```python
def test_missing_ods_tables_are_unchanged(db_conn, schema_path):
    ensure_schema(db_conn, schema_path)
    windows = capture_source_windows(db_conn)
    assert all(window.upper_bound is None for window in windows.values())
    assert not any(source_changed(window) for window in windows.values())


def test_row_after_captured_cutoff_is_deferred(db_conn, install_ods_tables, schema_path):
    ensure_schema(db_conn, schema_path)
    db_conn.execute(
        """
        INSERT INTO public.stories
            (id, title, score, descendants, created_at, loaded_at)
        VALUES (1, 'first', 1, 0, '2026-09-24 07:00:00', '2026-09-24 08:00:00')
        """
    )
    windows = capture_source_windows(db_conn)
    db_conn.execute(
        """
        INSERT INTO public.stories
            (id, title, score, descendants, created_at, loaded_at)
        VALUES (2, 'late', 1, 0, '2026-09-24 07:01:00', '2026-09-24 08:01:00')
        """
    )
    rows = select_source_rows(db_conn, windows["stories"])
    assert [row["id"] for row in rows] == [1]
```

Добавить тесты ISO round-trip aware timestamps и границы `(lower_bound, upper_bound]`.

- [ ] **Step 3: Запустить тесты и увидеть import failures**

Run: `cd lab_2 && TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_cleaning.py tests/test_incremental_windows.py -v`

Expected: FAIL с `ModuleNotFoundError: No module named 'lib.cleaning'`.

- [ ] **Step 4: Реализовать immutable models и очистку**

`models.py`:

```python
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SourceWindow:
    source_name: str
    lower_bound: datetime | None
    upper_bound: datetime | None
```

`cleaning.py` должен использовать только stdlib `html.unescape`, `html.parser.HTMLParser`, `re` и `datetime.UTC`: HTML удаляется парсером, затем `\s+` заменяется одним пробелом. `clean_required_text` вызывает optional-вариант без HTML и бросает точный `ValueError("required text is blank")`.

- [ ] **Step 5: Реализовать schema bootstrap и capture windows**

В `incremental.py`:

```python
SOURCE_TABLES = {
    "stories": "public.stories",
    "users": "public.users",
    "comments": "public.comments",
}


def source_changed(window: SourceWindow) -> bool:
    return window.upper_bound is not None and (
        window.lower_bound is None or window.upper_bound > window.lower_bound
    )
```

`capture_source_windows` обязан:

1. прочитать три watermarks из `dds.pipeline_state`;
2. проверить таблицу через `to_regclass`, не подставляя вход пользователя в SQL;
3. для существующей таблицы получить `MAX(loaded_at) AT TIME ZONE 'UTC'`;
4. вернуть окно с фиксированным cutoff.

`select_source_rows` использует whitelisted table и точный список столбцов из констант `SOURCE_TABLES`/`SOURCE_COLUMNS`, преобразует cursor tuples в dictionaries и применяет предикат:

```sql
WHERE (%s IS NULL OR loaded_at AT TIME ZONE 'UTC' > %s)
  AND loaded_at AT TIME ZONE 'UTC' <= %s
ORDER BY loaded_at, id
```

Если `upper_bound IS NULL`, функция возвращает пустой list без SQL к отсутствующей таблице.

- [ ] **Step 6: Добавить Airflow ignore rule**

`dags/.airflowignore`:

```text
lib/
```

- [ ] **Step 7: Запустить focused tests и lint**

Run:

```bash
cd lab_2
TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_cleaning.py tests/test_incremental_windows.py -v
.venv/bin/ruff check dags/lib tests/test_cleaning.py tests/test_incremental_windows.py
```

Expected: все tests PASS, Ruff выводит `All checks passed!`.

- [ ] **Step 8: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_2/dags/.airflowignore lab_2/dags/lib lab_2/tests/test_cleaning.py lab_2/tests/test_incremental_windows.py
git commit -m "feat: capture fixed ods source windows"
```

## Task 5: Загрузить пользователей, SCD2-профиль и user snapshots

**Files:**
- Modify: `lab_2/dags/lib/incremental.py`
- Create: `lab_2/tests/test_user_loading.py`

**Interfaces:**
- Consumes: `dict[str, SourceWindow]`, cleaning functions, schema из Task 3.
- Produces: `load_dim_users(conn, windows) -> int`, `load_user_profile_history(conn, users_window) -> int`, `load_user_snapshots(conn, users_window) -> int`.

- [ ] **Step 1: Написать тест dim_user из трёх источников**

В начале test module определить fixtures, которые вызывают `ensure_schema`, используют `install_ods_tables` и вставляют ODS-строки параметризованными `INSERT`. Fixture `seed_window` строит текущие окна через `capture_source_windows` и возвращает named tuple с полями `conn` и `windows`. Fixture `user_history` возвращает `conn`, `first_window`, `second_window`, причём два окна создаются явно как `SourceWindow("users", None, first_at)` и `SourceWindow("users", first_at, second_at)`; между ними `public.users` обновляется через `ON CONFLICT (id) DO UPDATE`. Так Task 5 не зависит от будущей функции watermark из Task 6.

```python
def test_dim_user_collects_users_story_and_comment_authors(seed_window):
    counts = load_dim_users(seed_window.conn, seed_window.windows)
    users = seed_window.conn.execute(
        "SELECT user_id FROM dds.dim_user ORDER BY user_id"
    ).fetchall()
    assert counts == 3
    assert users == [("comment_author",), ("story_author",), ("user_row",)]
```

В fixture одна строка users, одна story и один comment имеют разных авторов; duplicate username между источниками добавить отдельным assertion, что создаётся одна dimension row.

- [ ] **Step 2: Написать SCD2-тесты, включая NULL edge cases**

Сценарий:

1. `about='<p>Hello</p>'`, `loaded_at=08:00` → одна открытая версия `Hello`;
2. повтор окна → по-прежнему одна версия;
3. `about=' <p>Hello&nbsp;</p> '`, `loaded_at=16:00` → очистка не изменилась, версия не добавляется;
4. `about=NULL`, `loaded_at=00:00` следующего дня → старая закрыта в 00:00, открыта версия с `about IS NULL`;
5. пустой HTML в следующем окне также очищается в NULL и не создаёт третью версию;
6. NULL → `New text` создаёт новую версию.

Проверить `valid_to == next.valid_from`, ровно одну открытую версию и отсутствие пересечения.

- [ ] **Step 3: Написать snapshot/delta/idempotency tests**

```python
def test_user_snapshot_stores_growth_and_is_idempotent(user_history):
    load_user_snapshots(user_history.conn, user_history.first_window)
    load_user_snapshots(user_history.conn, user_history.second_window)
    load_user_snapshots(user_history.conn, user_history.second_window)
    rows = user_history.conn.execute(
        """
        SELECT karma, karma_delta, submitted_count, submitted_count_delta
        FROM dds.fact_user_snapshot
        ORDER BY source_loaded_at
        """
    ).fetchall()
    assert rows == [(250, None, 10, None), (320, 70, 14, 4)]
```

Этот тест специально сохраняет пользователя с первой karma ниже 300: DDS не фильтрует его.

- [ ] **Step 4: Запустить focused tests и увидеть отсутствующие функции**

Run: `cd lab_2 && TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_user_loading.py -v`

Expected: FAIL с import error для `load_dim_users`.

- [ ] **Step 5: Реализовать `load_dim_users` одной транзакцией**

Для каждого изменившегося окна выбрать только `id/author`, `created_at`, `loaded_at`, нормализовать username через `strip`, удалить blank/duplicates в Python и выполнить:

```sql
INSERT INTO dds.dim_user (user_id, created_at)
VALUES (%s, %s)
ON CONFLICT (user_id) DO UPDATE
SET created_at = COALESCE(dds.dim_user.created_at, EXCLUDED.created_at)
```

Автор, известный только по story/comment, получает `created_at=NULL`; поздняя users-row дополняет его.

- [ ] **Step 6: Реализовать SCD2 с NULL-safe comparison**

На пользователя блокировать открытую строку `FOR UPDATE`, сравнивать очищенные значения через Python equality или SQL `IS DISTINCT FROM`. Если значение изменилось:

```sql
UPDATE dds.dim_user_profile_history
SET valid_to = %s
WHERE user_sk = %s AND valid_to IS NULL;

INSERT INTO dds.dim_user_profile_history (user_sk, about, valid_from)
VALUES (%s, %s, %s)
ON CONFLICT (user_sk, valid_from) DO NOTHING;
```

Все строки окна обрабатывать по `(loaded_at, id)`. Commit выполнять один раз после полного окна; exception делает rollback.

- [ ] **Step 7: Реализовать user snapshots и deltas**

Предыдущий снимок выбирать с `source_loaded_at < current_loaded_at ORDER BY source_loaded_at DESC LIMIT 1`. Insert:

```sql
INSERT INTO dds.fact_user_snapshot (
    user_sk, source_loaded_at, karma, karma_delta,
    submitted_count, submitted_count_delta
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (user_sk, source_loaded_at) DO NOTHING
```

Проверять `submitted_count >= 0` до insert; отрицательная karma допустима контрактом Hacker News и не фильтруется. Некорректная ODS-строка должна завершить task ошибкой, а не быть silently skipped.

- [ ] **Step 8: Запустить user tests и полный локальный suite**

Run:

```bash
cd lab_2
TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_user_loading.py -v
TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest -q
```

Expected: focused tests и весь текущий suite PASS.

- [ ] **Step 9: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_2/dags/lib/incremental.py lab_2/tests/test_user_loading.py
git commit -m "feat: load user history into dds"
```

## Task 6: Загрузить stories, first-level comments, story snapshots и watermarks

**Files:**
- Modify: `lab_2/dags/lib/incremental.py`
- Create: `lab_2/tests/test_story_loading.py`

**Interfaces:**
- Consumes: `SourceWindow`, `clean_optional_text`, `clean_required_text`, уже заполненный `dds.dim_user`.
- Produces: `load_dim_stories(conn, stories_window) -> int`, `load_comments(conn, comments_window) -> int`, `load_story_snapshots(conn, stories_window) -> int`, `update_pipeline_state(conn, windows) -> int`.

- [ ] **Step 1: Написать test story upsert и сохранения natural key**

В начале test module определить fixtures `story_window`, `comment_window`, `source_windows`: каждая вызывает `ensure_schema`, использует `install_ods_tables`, вставляет parameterized ODS rows, вызывает `load_dim_users` перед story loader и возвращает named tuple только с полями, которые используются в тестах (`conn`, `window`/`windows`). Для второго окна fixture сначала вызывает `update_pipeline_state`, затем обновляет ODS-row с более поздним `loaded_at` и фиксирует новый cutoff.

```python
def test_story_dimension_uses_hn_id_and_user_surrogate(story_window):
    assert load_dim_stories(story_window.conn, story_window.window) == 1
    row = story_window.conn.execute(
        """
        SELECT s.story_id, u.user_id, s.title, s.url
        FROM dds.dim_story s
        LEFT JOIN dds.dim_user u ON u.user_sk = s.author_user_sk
        """
    ).fetchone()
    assert row == (101, "alice", "A title", "https://example.test/story")
```

Добавить второе ODS-окно той же story с обновлёнными title/url и проверить: `story_sk` не меняется, строка не дублируется, разрешённые dimension attributes обновляются.

- [ ] **Step 2: Написать comment tests**

```python
def test_comment_parent_resolves_to_story_and_retry_is_idempotent(comment_window):
    load_comments(comment_window.conn, comment_window.window)
    load_comments(comment_window.conn, comment_window.window)
    row = comment_window.conn.execute(
        """
        SELECT c.comment_id, s.story_id, u.user_id, c.text, c.bad_word
        FROM dds.fact_comment c
        JOIN dds.dim_story s USING (story_sk)
        LEFT JOIN dds.dim_user u ON u.user_sk = c.author_user_sk
        """
    ).fetchone()
    assert row == (501, 101, "bob", "Hello world", False)
    assert comment_window.conn.execute(
        "SELECT count(*) FROM dds.fact_comment"
    ).fetchone()[0] == 1
```

Для комментария с `parent=999999`, отсутствующим в `dds.dim_story`, ожидать:

```python
with pytest.raises(ValueError, match="comment 502 references unknown story 999999"):
    load_comments(conn, comments_window)
```

После exception проверить rollback всей comment batch и неизменный `pipeline_state.last_processed_at`.

- [ ] **Step 3: Написать snapshot/delta/aggregate tests**

Первое окно story: `score=10`, `descendants=3`, два загруженных first-level comments, один `bad_word=true`. Второе окно: `score=17`, `descendants=5`. Ожидание:

```python
assert rows == [
    (10, None, 3, None, 2, 1),
    (17, 7, 5, 2, 2, 1),
]
```

Дважды обработать второе окно и проверить ровно две snapshot rows. Отдельно проверить порядок: комментарии загружены до `load_story_snapshots`, поэтому агрегаты первого снимка уже равны `(2, 1)`.

- [ ] **Step 4: Написать watermark tests**

```python
def test_watermarks_advance_to_captured_cutoffs_only(db_conn, source_windows):
    update_pipeline_state(db_conn, source_windows)
    actual = dict(db_conn.execute(
        "SELECT source_name, last_processed_at FROM dds.pipeline_state"
    ).fetchall())
    assert actual == {
        name: window.upper_bound for name, window in source_windows.items()
    }
```

Добавить assertions:

- окно без изменений не переписывает `updated_at`;
- повторный вызов с теми же cutoffs не сдвигает watermark назад и возвращает `0` updated sources;
- cutoff, зафиксированный до поздней вставки, остаётся прежним;
- если `load_comments` упал, `update_pipeline_state` не вызывался и watermark не изменился.

- [ ] **Step 5: Запустить tests и увидеть отсутствующие loaders**

Run: `cd lab_2 && TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_story_loading.py -v`

Expected: FAIL с import error для `load_dim_stories`.

- [ ] **Step 6: Реализовать story dimension upsert**

Для changed story rows найти `author_user_sk` по очищенному username; blank author хранить как NULL. Обязательный upsert:

```sql
INSERT INTO dds.dim_story (
    story_id, author_user_sk, title, url, created_at
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (story_id) DO UPDATE SET
    author_user_sk = EXCLUDED.author_user_sk,
    title = EXCLUDED.title,
    url = EXCLUDED.url
```

`story_id`, `created_at` и `story_sk` при конфликте не изменять. Blank title завершает batch точным `ValueError("story <id> has blank title")`.

- [ ] **Step 7: Реализовать comment load с явным parent resolution**

Для каждой ODS-row:

1. найти `story_sk` по `dim_story.story_id = comments.parent`;
2. при отсутствии story бросить `ValueError` до commit;
3. найти nullable `author_user_sk`;
4. очистить `text` с `strip_html=True`;
5. выполнить insert с `ON CONFLICT (comment_id) DO NOTHING`.

Не создавать comment snapshots: grain остаётся одной строкой на `comment_id`.

- [ ] **Step 8: Реализовать story snapshots после comments**

Предыдущий снимок выбирать строго раньше текущего `source_loaded_at`. Агрегаты считать из всего уже загруженного `dds.fact_comment` для этой story:

```sql
SELECT count(*), count(*) FILTER (WHERE bad_word)
FROM dds.fact_comment
WHERE story_sk = %s
```

Insert защищается:

```sql
ON CONFLICT (story_sk, source_loaded_at) DO NOTHING
```

Отрицательные `score` или `descendants` завершают batch ошибкой. Не заставлять `loaded_comments_count <= descendants` на уровне CHECK: внешние HN API-вызовы ЛР-1 неатомарны, эту гонку оценивает DQ как warning.

- [ ] **Step 9: Реализовать final watermark update**

Для каждого changed window:

```sql
UPDATE dds.pipeline_state
SET last_processed_at = %s, updated_at = now()
WHERE source_name = %s
  AND (last_processed_at IS NULL OR last_processed_at < %s)
```

Commit только после обновления всех changed sources. Эта функция не должна вызываться из loaders: порядок задаёт DAG, и failed loader оставляет все watermarks прежними.

- [ ] **Step 10: Запустить story tests, suite и lint**

Run:

```bash
cd lab_2
TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_story_loading.py -v
TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest -q
.venv/bin/ruff check dags/lib tests
```

Expected: все tests PASS, Ruff выводит `All checks passed!`.

- [ ] **Step 11: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_2/dags/lib/incremental.py lab_2/tests/test_story_loading.py
git commit -m "feat: load stories and comments into dds"
```

## Task 7: Создать минимальный автономный Airflow runtime

**Files:**
- Create: `lab_2/Dockerfile`
- Create: `lab_2/docker-compose.yaml`
- Create: `lab_2/.env.example`

**Interfaces:**
- Consumes: external Docker network `lab_1_default`, running host `postgres-ods:5432`, `lab_2/requirements.txt`.
- Produces: services `postgres-metadata`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `airflow-cli`; Airflow connection id `postgres_ods`; UI `http://localhost:8083`.

- [ ] **Step 1: Создать image с pinned providers**

`Dockerfile`:

```dockerfile
FROM apache/airflow:3.3.1

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

- [ ] **Step 2: Описать environment contract без секретов в tracked compose**

`.env.example`:

```dotenv
AIRFLOW_UID=50000
AIRFLOW_ADMIN_USERNAME=airflow
AIRFLOW_ADMIN_PASSWORD=replace_me
AIRFLOW_METADATA_PASSWORD=replace_me
ODS_USER=ods
ODS_PASSWORD=replace_me
ODS_DATABASE=ods
```

Реальный `.env` создаётся пользователем локально и уже исключён из Git. В compose URI собираются только из `${...}` variables; значение ODS password `example` из ЛР-1 не записывать в tracked `docker-compose.yaml`.

- [ ] **Step 3: Описать общий Airflow anchor**

Ключевая конфигурация:

```yaml
x-airflow-common: &airflow-common
  build: .
  env_file: .env
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${AIRFLOW_METADATA_PASSWORD}@postgres-metadata/airflow
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/
    AIRFLOW_CONN_POSTGRES_ODS: postgresql://${ODS_USER}:${ODS_PASSWORD}@postgres-ods:5432/${ODS_DATABASE}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./sql:/opt/airflow/sql:ro
    - ./logs:/opt/airflow/logs
  user: "${AIRFLOW_UID:-50000}:0"
  networks:
    - default
    - lab1
```

Metadata DB использует отдельный named volume `airflow-metadata-data`; ODS volume ЛР-1 не монтировать.

- [ ] **Step 4: Добавить минимальные Airflow 3 services**

Определить:

- `postgres-metadata` на `postgres:16` с healthcheck;
- `airflow-init` с `_AIRFLOW_DB_MIGRATE=true`, `_AIRFLOW_WWW_USER_CREATE=true` и admin variables из `.env`;
- `airflow-apiserver` с command `api-server` и port `8083:8080`;
- `airflow-scheduler` с command `scheduler`;
- `airflow-dag-processor` с command `dag-processor`;
- `airflow-triggerer` с command `triggerer`;
- `airflow-cli` с profile `debug` и command `bash`.

Не добавлять Redis, Celery worker, Flower, MongoDB, MinIO, Adminer или второй `postgres-ods`.

External network:

```yaml
networks:
  lab1:
    external: true
    name: lab_1_default
```

- [ ] **Step 5: Проверить итоговый Compose config до запуска**

Run:

```bash
cd lab_2
cp .env.example .env
docker compose config --quiet
docker compose config --services
```

Expected services exactly: `postgres-metadata`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `airflow-cli`. В списке нет `redis`, `worker` и `postgres-ods`.

- [ ] **Step 6: Проверить понятную ошибку при не запущенной ЛР-1**

Run: `docker network inspect lab_1_default`

Expected when Lab 1 is running: exit `0`, network содержит container сервиса `postgres-ods`. Если network отсутствует, README Task 10 укажет сначала выполнить `cd ../lab_1 && docker compose up -d postgres-ods`.

- [ ] **Step 7: Собрать и инициализировать runtime**

Run:

```bash
cd lab_2
docker compose build
docker compose up airflow-init
docker compose up -d postgres-metadata airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
docker compose ps
```

Expected: init exits `0`, остальные пять services имеют state `running`/`healthy`, UI отвечает на `http://localhost:8083`.

- [ ] **Step 8: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_2/Dockerfile lab_2/docker-compose.yaml lab_2/.env.example
git commit -m "build: add standalone lab2 airflow stack"
```

## Task 8: Собрать основной incremental Airflow DAG

**Files:**
- Create: `lab_2/dags/dds_incremental_etl.py`
- Create: `lab_2/tests/test_dag_structure.py`

**Interfaces:**
- Consumes: all incremental functions from Tasks 4–6, Airflow connection `postgres_ods`, schema path `/opt/airflow/sql/create_dds_schema.sql`.
- Produces: DAG id `dds_incremental_etl`; XCom payload `dict[str, dict[str, str | None]]`; downstream trigger for `dds_data_quality`.

- [ ] **Step 1: Написать статический DAG contract test**

Тест читает source через `ast.parse` и проверяет обязательные literals/imports:

```python
def test_incremental_dag_contract():
    source = DAG_PATH.read_text()
    ast.parse(source)
    assert 'dag_id="dds_incremental_etl"' in source
    assert 'schedule="0 */8 * * *"' in source
    assert 'tz="Europe/Moscow"' in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source
    assert 'trigger_dag_id="dds_data_quality"' in source
    assert "wait_for_completion=True" in source
    assert "reset_dag_run=True" in source
```

Проверить наличие task ids в порядке исходного текста:

```python
EXPECTED_ORDER = [
    "ensure_dds_schema",
    "check_ods_changes",
    "load_dim_user",
    "load_user_profile_history",
    "load_fact_user_snapshot",
    "load_dim_story",
    "load_fact_comment",
    "load_fact_story_snapshot",
    "update_pipeline_state",
    "trigger_data_quality",
]
assert [source.index(name) for name in EXPECTED_ORDER] == sorted(
    source.index(name) for name in EXPECTED_ORDER
)
```

- [ ] **Step 2: Запустить test и увидеть missing DAG file**

Run: `cd lab_2 && .venv/bin/pytest tests/test_dag_structure.py -v`

Expected: FAIL с `FileNotFoundError` для `dags/dds_incremental_etl.py`.

- [ ] **Step 3: Создать DAG только через публичные Airflow 3 imports**

Начало файла:

```python
from pathlib import Path

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

SCHEMA_PATH = Path("/opt/airflow/sql/create_dds_schema.sql")


@dag(
    dag_id="dds_incremental_etl",
    schedule="0 */8 * * *",
    start_date=pendulum.datetime(2026, 9, 24, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    tags=["hacker-news", "dds", "lab2"],
)
def dds_incremental_etl():
```

Каждая DB-task получает собственное `PostgresHook(postgres_conn_id="postgres_ods").get_conn()`, вызывает одну pure loader function и закрывает connection через `try/finally`.

- [ ] **Step 4: Реализовать bootstrap, capture и branch без row XCom**

`ensure_dds_schema` сначала выполняет idempotent schema SQL. После него `check_ods_changes` фиксирует cutoffs и сериализует только три `SourceWindow`. Branch:

```python
@task.branch(task_id="choose_load_path")
def choose_load_path(payload: dict[str, dict[str, str | None]]) -> str:
    windows = {name: window_from_dict(value) for name, value in payload.items()}
    if any(source_changed(window) for window in windows.values()):
        return "load_dim_user"
    return "no_changes"
```

`no_changes` — `EmptyOperator`; эта ветка не запускает DQ, потому что DDS не изменился.

- [ ] **Step 5: Реализовать строгий load order**

Dependency graph должен быть ровно таким:

```text
ensure_dds_schema
  -> check_ods_changes
  -> choose_load_path
       -> no_changes -> finish
       -> load_dim_user
          -> load_user_profile_history
          -> load_fact_user_snapshot
          -> load_dim_story
          -> load_fact_comment
          -> load_fact_story_snapshot
          -> update_pipeline_state
          -> trigger_data_quality
          -> finish
```

`finish` использует trigger rule `none_failed_min_one_success`. Comments предшествуют story snapshots, а `update_pipeline_state` — последняя DB-write перед DQ.

- [ ] **Step 6: Настроить отдельный DQ trigger**

```python
trigger_data_quality = TriggerDagRunOperator(
    task_id="trigger_data_quality",
    trigger_dag_id="dds_data_quality",
    conf={"source_run_id": "{{ run_id }}"},
    wait_for_completion=True,
    reset_dag_run=True,
    poke_interval=10,
)
```

Если DQ failed, waiting operator также failed; watermark уже отражает успешно записанный DDS и повторная ODS-загрузка не нужна.

- [ ] **Step 7: Запустить static test и реальный Airflow import check**

Run:

```bash
cd lab_2
.venv/bin/pytest tests/test_dag_structure.py -v
docker compose run --rm airflow-cli airflow dags list-import-errors
docker compose run --rm airflow-cli airflow dags list | grep dds_incremental_etl
```

Expected: pytest PASS; `list-import-errors` показывает `No data found`; DAG встречается ровно один раз.

- [ ] **Step 8: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_2/dags/dds_incremental_etl.py lab_2/tests/test_dag_structure.py
git commit -m "feat: orchestrate incremental dds load"
```

## Task 9: Реализовать Data Quality engine и отдельный DAG

**Files:**
- Modify: `lab_2/dags/lib/models.py`
- Create: `lab_2/dags/lib/quality.py`
- Create: `lab_2/dags/dds_data_quality.py`
- Modify: `lab_2/tests/test_dag_structure.py`
- Create: `lab_2/tests/test_quality.py`

**Interfaces:**
- Consumes: populated DDS, ODS rows up to `dds.pipeline_state.last_processed_at`, trigger conf `source_run_id`.
- Produces: `QualityResult(check_name: str, status: bool, actual_value: str, expected_rule: str, details: str | None, blocking: bool)`, `DataQualityError(RuntimeError)`, `run_quality_checks(conn) -> list[QualityResult]`, `persist_quality_results(conn, run_id, results) -> None`, `assert_blocking_checks(results) -> None`; DAG id `dds_data_quality`.

- [ ] **Step 1: Написать quality model и clean-dataset tests**

Fixture `loaded_dds` в `test_quality.py` создаёт schema и ODS tables, вставляет одного пользователя, одну story и один прямой comment, последовательно вызывает loaders Tasks 5–6, затем `update_pipeline_state`; возвращает named tuple с единственным полем `conn`. Так clean и corrupted scenarios проверяют тот же публичный API, который вызывает DAG.

```python
def test_clean_dataset_passes_all_blocking_checks(loaded_dds):
    results = run_quality_checks(loaded_dds.conn)
    assert results
    assert all(result.status for result in results if result.blocking)
    assert_blocking_checks(results) is None
```

Проверить точный набор check names:

```python
EXPECTED_CHECKS = {
    "completeness.required_values",
    "uniqueness.natural_and_snapshot_keys",
    "referential_integrity.orphan_facts",
    "validity.non_negative_measures",
    "validity.timestamps_not_in_future",
    "scd2.single_open_version",
    "scd2.non_overlapping_intervals",
    "reconciliation.ods_users_to_dds",
    "reconciliation.ods_stories_to_dds",
    "reconciliation.ods_comments_to_dds",
    "history.snapshot_key_uniqueness",
    "anomaly.comment_counts",
}
```

- [ ] **Step 2: Написать failure и warning tests**

Так как PK/FK/CHECK не позволят обычным insert создать часть аномалий, tests используют отдельные транзакции и временно `ALTER TABLE ... DISABLE TRIGGER ALL` только для fixture, затем обязательно возвращают `ENABLE TRIGGER ALL`.

Проверить:

- orphan `fact_comment.story_sk` даёт `status=False`, `blocking=True`, и `assert_blocking_checks` бросает `DataQualityError`;
- пересекающиеся SCD2 intervals дают blocking failure;
- ODS comment до watermark без DDS comment даёт reconciliation failure;
- `loaded_comments_count > descendants` даёт `anomaly.comment_counts` со `status=False`, `blocking=False`, а итоговый assert не бросает;
- все результаты, включая warning, сохраняются до exception.

- [ ] **Step 3: Запустить tests и увидеть missing module**

Run: `cd lab_2 && TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_quality.py -v`

Expected: FAIL с `ModuleNotFoundError: No module named 'lib.quality'`.

- [ ] **Step 4: Реализовать `QualityResult` и SQL checks**

`models.py`:

```python
@dataclass(frozen=True, slots=True)
class QualityResult:
    check_name: str
    status: bool
    actual_value: str
    expected_rule: str
    details: str | None = None
    blocking: bool = True
```

В `quality.py` каждый check выполняет один агрегирующий SQL, где `actual_value` — count нарушений, а `status = count == 0`. Reconciliation queries применяют соответствующий watermark и вызываются только если `to_regclass(source_table)` не NULL.

SCD overlap query:

```sql
SELECT count(*)
FROM dds.dim_user_profile_history left_version
JOIN dds.dim_user_profile_history right_version
  ON left_version.user_sk = right_version.user_sk
 AND left_version.user_profile_sk < right_version.user_profile_sk
 AND left_version.valid_from < COALESCE(right_version.valid_to, 'infinity')
 AND right_version.valid_from < COALESCE(left_version.valid_to, 'infinity')
```

- [ ] **Step 5: Сохранить все результаты до определения общего failure**

Upsert:

```sql
INSERT INTO dds.data_quality_results (
    run_id, check_name, status, actual_value, expected_rule, details
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, check_name) DO UPDATE SET
    status = EXCLUDED.status,
    actual_value = EXCLUDED.actual_value,
    expected_rule = EXCLUDED.expected_rule,
    details = EXCLUDED.details,
    checked_at = now()
```

Commit выполняется внутри `persist_quality_results`. Затем `assert_blocking_checks` реализуется точно так:

```python
failed = sorted(
    result.check_name for result in results if not result.status and result.blocking
)
if failed:
    raise DataQualityError(
        f"blocking data quality checks failed: {', '.join(failed)}"
    )
```

- [ ] **Step 6: Создать отдельный DQ DAG**

```python
import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException


@dag(
    dag_id="dds_data_quality",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 24, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    tags=["hacker-news", "data-quality", "lab2"],
)
def dds_data_quality():
```

Одна task `run_and_record_quality_checks`:

1. берёт `source_run_id` из `get_current_context()["dag_run"].conf`, fallback — текущий DQ `run_id`;
2. запускает все checks;
3. сохраняет все results;
4. преобразует `DataQualityError` в `AirflowException` после commit.

Так один failed check не мешает записать результаты остальных групп.

- [ ] **Step 7: Расширить DAG contract tests**

Проверить `dag_id="dds_data_quality"`, `schedule=None`, `catchup=False`, import `AirflowException`, task id `run_and_record_quality_checks` и отсутствие schedule `0 */8 * * *` в DQ source.

- [ ] **Step 8: Запустить quality tests и Airflow import check**

Run:

```bash
cd lab_2
TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest tests/test_quality.py tests/test_dag_structure.py -v
docker compose run --rm airflow-cli airflow dags list-import-errors
docker compose run --rm airflow-cli airflow dags list | grep dds_data_quality
```

Expected: tests PASS, import errors отсутствуют, DQ DAG встречается ровно один раз.

- [ ] **Step 9: Зафиксировать изменение, если репозиторий существует**

```bash
git add lab_2/dags/lib/models.py lab_2/dags/lib/quality.py lab_2/dags/dds_data_quality.py lab_2/tests/test_quality.py lab_2/tests/test_dag_structure.py
git commit -m "feat: add persistent dds quality checks"
```

## Task 10: Добавить аналитические запросы, документацию и end-to-end verification

**Files:**
- Create: `lab_2/sql/analytics_examples.sql`
- Create: `lab_2/README.md`

**Interfaces:**
- Consumes: running Lab 1 `postgres-ods`, completed DDS and DQ DAGs.
- Produces: воспроизводимая демонстрация лабораторной и четыре аналитических запроса без дополнительных mart/word tables.

- [ ] **Step 1: Написать запрос роста рейтинга публикации**

```sql
SELECT
    s.story_id,
    s.title,
    ss.source_loaded_at,
    ss.score,
    ss.score_delta,
    ss.descendants,
    ss.descendants_delta,
    ss.loaded_comments_count
FROM dds.fact_story_snapshot ss
JOIN dds.dim_story s USING (story_sk)
WHERE s.story_id = :story_id
ORDER BY ss.source_loaded_at;
```

- [ ] **Step 2: Написать запрос роста автора с аналитическим порогом 300**

```sql
WITH latest AS (
    SELECT DISTINCT ON (user_sk)
        user_sk, source_loaded_at, karma, karma_delta,
        submitted_count, submitted_count_delta
    FROM dds.fact_user_snapshot
    ORDER BY user_sk, source_loaded_at DESC
)
SELECT
    u.user_id,
    latest.source_loaded_at,
    latest.karma,
    latest.karma_delta,
    latest.submitted_count,
    latest.submitted_count_delta
FROM latest
JOIN dds.dim_user u USING (user_sk)
WHERE latest.karma >= 300
ORDER BY latest.karma_delta DESC NULLS LAST, latest.karma DESC;
```

Порог `300` не должен встречаться ни в loader Python, ни в schema SQL.

- [ ] **Step 3: Добавить запросы популярности и истории профиля**

Запрос популярности использует последний snapshot каждой story и сортирует по `score`, `descendants`, `loaded_comments_count`. Запрос SCD2 возвращает `user_id`, `about`, `valid_from`, `valid_to` по одному `:user_id`. Ни один запрос не создаёт `dim_word`, `bridge_story_word`, materialized view или mart.

- [ ] **Step 4: Написать README как сценарий защиты**

README обязан объяснить:

1. lineage `Hacker News → Lab 1 ODS → Lab 2 DDS → DQ`;
2. почему это снежинка/3НФ и чем facts отличаются от dimensions;
3. почему `user_id` — HN username, а `user_sk` — surrogate key;
4. зачем SCD2 только для `about`, а karma/score — snapshots;
5. алгоритм fixed cutoff + `(watermark, cutoff]` + watermark-last;
6. почему комментарий хранится один раз и `parent` превращается в `story_sk`;
7. почему `descendants` и loaded first-level comment count различаются;
8. почему 8-часовой poll не восстанавливает перезаписанные часовые ODS-состояния;
9. DQ groups, blocking failures и non-blocking source-race warning;
10. точные команды запуска, проверки и остановки.

Команды первого запуска:

```bash
cd lab_1
docker compose up -d postgres-ods

cd ../lab_2
cp .env.example .env
# В .env установить ODS_PASSWORD, совпадающий с lab_1.
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose run --rm airflow-cli airflow dags trigger dds_incremental_etl
```

- [ ] **Step 5: Выполнить статическую финальную проверку**

Run:

```bash
cd lab_2
.venv/bin/ruff check dags tests
.venv/bin/pytest tests/test_lab1_user_filter.py tests/test_cleaning.py tests/test_dag_structure.py -v
docker compose config --quiet
docker compose run --rm airflow-cli airflow dags list-import-errors
```

Expected: Ruff clean, pytest PASS, compose valid, Airflow import errors отсутствуют.

- [ ] **Step 6: Выполнить полный integration suite на чистой test DB**

Run:

```bash
cd lab_2
docker compose -f docker-compose.test.yaml down -v
docker compose -f docker-compose.test.yaml up -d --wait
TEST_DATABASE_DSN=postgresql://ods_test:ods_test@localhost:55432/ods_test .venv/bin/pytest -q
```

Expected: весь suite PASS. Повторить ту же команду pytest без очистки; второй запуск также PASS и не обнаруживает leak между tests.

- [ ] **Step 7: Выполнить end-to-end smoke against Lab 1 ODS**

Run:

```bash
cd lab_2
docker compose up -d
docker compose run --rm airflow-cli airflow dags test dds_incremental_etl 2026-09-24T00:00:00+03:00
docker compose run --rm airflow-cli airflow dags list-runs -d dds_data_quality --no-backfill
```

Затем через `psql` внутри `postgres-ods` проверить:

```sql
SELECT source_name, last_processed_at FROM dds.pipeline_state ORDER BY source_name;
SELECT count(*) FROM dds.dim_user;
SELECT count(*) FROM dds.dim_story;
SELECT count(*) FROM dds.fact_comment;
SELECT count(*) FROM dds.fact_user_snapshot;
SELECT count(*) FROM dds.fact_story_snapshot;
SELECT status, count(*) FROM dds.data_quality_results GROUP BY status;
```

Expected: watermarks соответствуют captured ODS maxima; dimensions/facts содержат данные при непустой ODS; последний DQ run success; blocking false results отсутствуют.

- [ ] **Step 8: Проверить no-change и second-snapshot сценарии**

1. Повторно выполнить `airflow dags test` без запуска ЛР-1: branch `no_changes`, row counts не меняются, DQ не запускается.
2. Запустить ODS DAG-и ЛР-1 или вставить контролируемое следующее `loaded_at`, затем повторить ЛР-2: появляются новые snapshot rows и корректные deltas, dimensions/comments не дублируются.
3. Искусственно создать одну DQ-аномалию в test DB: DQ run failed и записал false result; после rollback/исправления следующий DQ run success.

- [ ] **Step 9: Зафиксировать документацию, если репозиторий существует**

```bash
git add lab_2/sql/analytics_examples.sql lab_2/README.md
git commit -m "docs: add lab2 runbook and analysis queries"
```

- [ ] **Step 10: Подготовить итоговый отчёт реализации**

В отчёте перечислить: изменённые файлы, команды с фактическими exit codes, число passed tests, результат `list-import-errors`, состояние E2E DAG, известное ограничение 8-часовых наблюдений и отсутствие word-analysis tables. Не утверждать успех для команды, которая не запускалась.
