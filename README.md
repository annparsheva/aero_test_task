# aero_test_task
## Задание
https://taniadi.notion.site/Aero-DE-c7b2b267caa14570935d7cba30218865
## Результирующая таблица
```
CREATE TABLE raw_nhl_teams_stats 
(
    teamId UInt32, 
    teamName Nullable(String),
    teamLink Nullable(String),
    regularSeasonStatRankings Map(String, String),
    statsSingleSeason Map(String, Float64),
    gameType Map(String, String),
    dateTime DateTime
)
ENGINE MergeTree ORDER BY dateTime
```
## Пример запроса
```
SELECT
    teamName,
    regularSeasonStatRankings['stat.wins']
FROM default.raw_nhl_teams_stats
ORDER BY dateTime ASC
LIMIT 1 BY teamId

Query id: 09bfca47-0880-41da-9723-2110eadf87d6

┌─teamName───────────┬─arrayElement(regularSeasonStatRankings, 'stat.wins')─┐
│ Colorado Avalanche │ 5th                                                  │
└────────────────────┴──────────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.015 sec.
```