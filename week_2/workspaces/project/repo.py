from dagster import repository
from workspaces.project.week_2 import week_2_pipeline_docker, week_2_pipeline_local


@repository
def repo():
    return [week_2_pipeline_docker, week_2_pipeline_local]
