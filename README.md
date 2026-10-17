# Аналитическая система Hacker News

Учебный проект по дисциплине «Технологии хранения больших данных».
Система получает данные из Hacker News API, сохраняет их в ODS,
преобразует в нормализованный DDS, строит аналитические витрины
и показывает результаты в Apache Superset. Apache OpenMetadata
используется для каталога данных и Data Lineage.

```text
Hacker News API
      ↓
ЛР1: ODS — PostgreSQL, MongoDB, MinIO
      ↓
ЛР2: DDS — нормализация, история и Data Quality
      ↓
ЛР3: Data Marts — Superset и OpenMetadata
```

## Структура проекта

```text
.
├── docker-compose.yaml   # общее Docker-окружение
├── .env.example          # пример переменных окружения
├── lab_1/                # получение данных и загрузка в ODS
├── lab_2/                # DDS, история и Data Quality
└── lab_3/                # витрины, Superset и OpenMetadata
```

- `lab_1` содержит DAG загрузки публикаций, пользователей и
  комментариев в PostgreSQL, MongoDB и MinIO.
- [`lab_2`](lab_2/README.md) содержит инкрементальный процесс ODS → DDS,
  исторические снимки, SCD2 и проверки качества.
- [`lab_3`](lab_3/README.md) содержит пять аналитических витрин,
  классификатор тем, дашборд Superset и Data Lineage OpenMetadata.

## Быстрый запуск

Все команды выполняются из корня проекта через терминал.

1. Создайте файл окружения и заполните пустые пароли и ключи:

   ```bash
   cp .env.example .env
   ```

2. Проверьте конфигурацию и запустите все сервисы:

   ```bash
   docker compose --profile lab3 config --quiet
   docker compose --profile lab3 up -d --build
   docker compose --profile lab3 ps
   ```

3. Запустите получение данных из Hacker News:

   ```bash
   docker compose exec airflow-scheduler airflow dags trigger stories_etl
   ```

   `stories_etl` передаст тот же набор публикаций в `users_etl` и
   `comments_etl`.

4. После завершения DAG первой лабораторной запустите DDS:

   ```bash
   docker compose exec airflow-scheduler \
     airflow dags trigger dds_incremental_etl
   ```

   При наличии новых данных процесс последовательно запустит
   `dds_data_quality`, затем `marts_etl`.

## Веб-интерфейсы

| Компонент | Адрес |
|---|---|
| Airflow | <http://localhost:8080> |
| Adminer | <http://localhost:8082> |
| OpenMetadata ingestion Airflow | <http://localhost:8083> |
| Superset | <http://localhost:8088> |
| OpenMetadata | <http://localhost:8585> |
| MinIO Console | <http://localhost:9001> |

Логины и пароли задаются в `.env`.

## Остановка

Остановить окружение с сохранением данных:

```bash
docker compose --profile lab3 down
```

Команда `docker compose down -v` удаляет volumes вместе с ODS, DDS,
витринами и служебными базами. Используйте её только для полного
сброса проекта.

## Документация

- [Отчёт по лабораторной работе №2](lab_2/README.md)
- [Отчёт по лабораторной работе №3](lab_3/README.md)

