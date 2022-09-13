from dagster import repository, with_resources
from dagster_dbt import dbt_cli_resource
from project.dbt_config import DBT_PROJECT_PATH
from project.resources import postgres_resource
from project.week_4 import (
    get_s3_data_docker,
    process_data_docker,
    put_redis_data_docker,
)
from project.week_4_challenge import create_dbt_table, insert_dbt_data


@repository
def repo():
    return [get_s3_data_docker, process_data_docker, put_redis_data_docker]


@repository
def assets_dbt():
    pass
