from dagster import repository
from workspaces.project.week_4 import (
    get_s3_data_docker,
    process_data_docker,
    put_redis_data_docker,
)


@repository
def repo():
    return [get_s3_data_docker, process_data_docker, put_redis_data_docker]
