# Ручная настройка OpenMetadata

## 1. Запуск

Из корня проекта выполните через терминал:

```bash
docker compose up -d
docker compose --profile openmetadata up -d
docker compose ps
```

Интерфейс OpenMetadata: <http://localhost:8585>. Встроенный ingestion Airflow:
<http://localhost:8083>.

## 2. PostgreSQL service

В `Settings -> Services -> Databases` создайте PostgreSQL service с именем
`tsd_postgres`:

```text
hostPort: postgres-ods:5432
username: значение ODS_USER из .env
password: значение ODS_PASSWORD из .env
database: ods
```

Настройте ingestion для схем `public`, `dds`, `marts`; служебную
`marts_staging` можно исключить. После запуска должны появиться FQN:

```text
tsd_postgres.ods.public.stories
tsd_postgres.ods.public.users
tsd_postgres.ods.public.comments
tsd_postgres.ods.dds.dim_user
tsd_postgres.ods.dds.dim_user_profile_history
tsd_postgres.ods.dds.fact_user_snapshot
tsd_postgres.ods.dds.dim_story
tsd_postgres.ods.dds.fact_story_snapshot
tsd_postgres.ods.dds.fact_comment
tsd_postgres.ods.marts.story_dynamics
tsd_postgres.ods.marts.current_story_ranking
tsd_postgres.ods.marts.author_dynamics
tsd_postgres.ods.marts.topic_performance
tsd_postgres.ods.marts.daily_activity
```

## 3. Airflow service

В `Settings -> Services -> Pipelines` создайте Airflow service, указав
внутренний endpoint общего Airflow:

```text
service name: tsd_airflow
hostPort: http://airflow-apiserver:8080
connection: REST API
API version: v2
authentication: Basic Auth
username/password: AIRFLOW_ADMIN_USERNAME / AIRFLOW_ADMIN_PASSWORD
```

Коннектор OpenMetadata 1.12.6 сам обменивает Basic Auth на
обновляемый JWT через Airflow 3 `/auth/token`. Статический
Access Token для этого подключения не используйте: он истечёт.

Запустите metadata ingestion. DAG содержат `inlets/outlets` с теми же FQN,
поэтому после ingestion проверьте связи `public -> dds -> marts` на вкладке
Lineage. Если конкретная версия коннектора не интерпретирует упрощённые
Airflow assets, добавьте эти table-level edges в UI и зафиксируйте это как
connector compatibility fallback, не меняя прикладные данные.

В связке OpenMetadata 1.12.6 + Airflow 3.3.1 этот fallback
потребовался: DAG и задачи импортируются, но упрощённые
`inlets/outlets` не превращаются в table-level edges. Ручные связи
помечаются `PipelineLineage` и привязываются к
`tsd_airflow.dds_incremental_etl` и `tsd_airflow.marts_etl`.

## 4. Superset service

После ручного создания dashboard в Superset добавьте BI service Superset:

```text
service name: tsd_superset
hostPort: http://superset:8088
username/password: SUPERSET_ADMIN_USERNAME / SUPERSET_ADMIN_PASSWORD
```

Запустите metadata ingestion и убедитесь, что каталог видит datasets, charts
и dashboard `Hacker News Analytics`.

Коннектор Superset определяет charts dashboard по `position_json`.
Если в каталоге появился только dashboard, откройте его в Superset,
сохраните layout и повторите ingestion. На первом полном проходе
создаются charts и data models; повторный проход безопасен и
идемпотентен.

## 5. Внешний Hacker News API

Создайте REST API service `hacker_news_api`, collection `v0` и endpoint
`hacker_news_api.v0.source` с отображаемым именем `Hacker News API`.
Базовый URL:

```text
https://hacker-news.firebaseio.com/v0
```

В редакторе Lineage вручную добавьте три исходящие связи:

```text
Hacker News API -> tsd_postgres.ods.public.stories
Hacker News API -> tsd_postgres.ods.public.users
Hacker News API -> tsd_postgres.ods.public.comments
```

Эта операция выполняется вручную один раз; bootstrap-скрипт намеренно не
создаётся.

## 6. Приёмочная проверка

На странице Lineage пройдите путь от API до dashboard. Конкретный
проверочный маршрут:

```text
Hacker News API
  -> tsd_postgres.ods.public.stories
  -> tsd_postgres.ods.dds.dim_story
  -> tsd_postgres.ods.marts.story_dynamics
  -> tsd_superset.model.1
  -> Hacker News Analytics
```

Для одного mart проверьте upstream DDS-таблицы, downstream dataset/chart и
Impact Analysis. Пустой участок графа означает либо незапущенный ingestion,
либо несовпадение FQN/имени service.

