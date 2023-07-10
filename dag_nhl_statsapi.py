import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from airflow_clickhouse_plugin.operators.clickhouse_operator import (
    ClickHouseHook
)

def filter_stat(dict_col):
    return {k: v for k, v in dict_col.items() if k.startswith('stat.')}

def to_float_stat(dict_col):
    return {k: round(float(v),4) for k, v in dict_col.items()}

def to_str_stat(dict_col):
    return {k: str(v) for k, v in dict_col.items()}


with DAG(
    dag_id="dag_nhl_statsapi",
    schedule="* */12 * * *",
    start_date=pendulum.datetime(2023, 7, 9),
    catchup=False,
) as dag:
    @task()
    def extract_team_stats(team_id,result_table):
        """Load data from statsapi.web.nhl.com and load to ClickHouse"""
        import logging
        import requests
        import pandas as pd
        from datetime import datetime

        logger = logging.getLogger("airflow.task")

        statsapi_nhl_endpoint = Variable.get("statsapi_nhl_endpoint", deserialize_json=True)
        url = f"{statsapi_nhl_endpoint}teams/{team_id}/stats"

        response = requests.get(url)

        if response.status_code == 200:
            data = pd.DataFrame(response.json()['stats'])
            if data["splits"].str.len().sum() > 0:
                data["split"] = pd.json_normalize(data["splits"])

                data["teamId"] = data["split"].apply(lambda x: x.get("team.id"))
                data["teamName"] = data["split"].apply(lambda x: x.get("team.name"))
                data["teamLink"] = data["split"].apply(lambda x: x.get("team.link"))

                data["displayName"] = data["type"].apply(lambda x: x.get("displayName"))
                data["gameType"] = data["type"].apply(lambda x: x.get("gameType"))

                data["split_stat"] = data["split"].apply(lambda x: filter_stat(x))

                data_pivoted = pd.pivot_table(
                    data[["teamId","teamName","teamLink","displayName","split_stat","gameType"]],
                    index=["teamId","teamName","teamLink"],
                    columns="displayName",
                    values="split_stat",
                    aggfunc='first'
                ).reset_index()

                data_pivoted["gameType"] = data["gameType"].dropna().apply(lambda x: to_str_stat(x))
                data_pivoted["statsSingleSeason"] = data_pivoted["statsSingleSeason"].apply(lambda x: to_float_stat(x))

                data_clean = data_pivoted[[
                    "teamId",
                    "teamName",
                    "teamLink",
                    "regularSeasonStatRankings",
                    "statsSingleSeason",
                    "gameType"
                ]]

                data_clean["dateTime"] = datetime.utcnow().replace(microsecond=0)

                ch_hook = ClickHouseHook(clickhouse_conn_id="clickhouse")

                ret = ch_hook.run(
                    sql=f"INSERT INTO {result_table} VALUES;",
                    parameters = data_clean.to_dict('records'),   
                )
                return {
                    "team_id" : team_id,
                    "upload_date_time" : data_clean["dateTime"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
                }
            else:
                logger.warning(f"Empy stats for team {team_id}")
        else:
            raise ValueError("API connection error.")
    
    extract_team_stats(21,"default.raw_nhl_teams_stats")