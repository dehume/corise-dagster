from dagster import repository
from dagster_ucr.content.etl import etl_docker, etl_local
from dagster_ucr.content.hello import job
from dagster_ucr.project.week_2 import docker_week_2_pipeline, local_week_2_pipeline


@repository
def repo():
    return [job, etl_docker, etl_local, docker_week_2_pipeline, local_week_2_pipeline]


@repository
def local_repo():
    return [etl_local, local_week_2_pipeline]


@repository
def prod_repo():
    return [etl_docker, docker_week_2_pipeline]
