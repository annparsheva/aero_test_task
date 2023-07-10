# aero_test_task
## Задание
Реализовать логику ELT процесса для выгрузки данных из
`https://statsapi.web.nhl.com/api/v1/teams/21/stats`

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
    regularSeasonStatRankings['stat.wins'] AS wins_rank
FROM default.raw_nhl_teams_stats
ORDER BY dateTime ASC
LIMIT 1 BY teamId

Query id: 4ff1795f-6599-4ad9-bad4-2ac77c38aa5c

┌─teamName───────────┬─wins_rank─┐
│ Colorado Avalanche │ 5th       │
└────────────────────┴───────────┘

1 row in set. Elapsed: 0.021 sec. 
```