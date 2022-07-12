from dagster import repository

# from dagster_ucr.week_2 import docker_week_2_pipeline, local_week_2_pipeline
from dagster_ucr.content import etl_docker, etl_local
from dagster_ucr.hello_world import job


@repository
def repo():
    return [job, etl_docker, etl_local]
    # return [job, etl_docker, etl_local, docker_week_2_pipeline, local_week_2_pipeline]
