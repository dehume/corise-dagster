from dagster import repository
from workspaces.content.etl import etl_docker, etl_local
from workspaces.content.hello import job


@repository
def repo():
    return [etl_local, etl_docker, job]
