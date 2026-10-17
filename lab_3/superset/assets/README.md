# Superset dashboard bundle

Dashboard создаётся вручную после первого успешного запуска `marts_etl`.
Не добавляйте вымышленный ZIP: каталог предназначен только для настоящего
экспорта из работающего Superset.

Файл `hacker_news_analytics.zip` в этом каталоге — настоящий экспорт
проверенного dashboard `Hacker News Analytics`. Bundle содержит шесть charts,
пять datasets схемы `marts`, подключение `TSD ODS` и четыре native filters.

## Создание

1. Откройте <http://localhost:8088>.
2. Добавьте PostgreSQL database с host `postgres-ods`, port `5432`, database
   `ods` и учётными данными `ODS_USER/ODS_PASSWORD`.
3. Зарегистрируйте datasets схемы `marts`: `story_dynamics`,
   `current_story_ranking`, `author_dynamics`, `topic_performance`,
   `daily_activity`.
4. Создайте dashboard `Hacker News Analytics`.
5. Добавьте шесть charts:
   - line chart `score` по `snapshot_at` для публикаций;
   - line chart `descendants` по `snapshot_at`;
   - horizontal bar текущего `score` по заголовку;
   - line chart `karma` по `snapshot_at` с chart filter `karma >= 300`;
   - treemap/bar по темам и `average_score`/`stories_count`;
   - combined time series дневных публикаций и комментариев.
6. Добавьте native filters: период, `topic`, `author_id`/`user_id`,
   `story_id`/`title`.
7. Проверьте, что фильтры воздействуют только на совместимые datasets.

В текущем экспорте рейтинг публикаций использует горизонтальные столбцы,
график кармы — логарифмическую шкалу и фильтр `karma >= 300`, а комментарии
в combined chart вынесены на вторую ось.

## Экспорт

В Superset выберите dashboard, затем `Export`. Сохраните полученный bundle в
этом каталоге под именем:

```text
hacker_news_analytics.zip
```

Bundle должен появиться только после ручной проверки всех charts. Для
импорта используйте `Settings -> Import Dashboards` и выберите тот же ZIP.
