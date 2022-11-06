from dagster import repository
from workspaces.project.week_3 import (
    week_3_pipeline_docker,
    week_3_pipeline_local,
    week_3_schedule_docker,
    week_3_schedule_local,
    week_3_sensor_docker,
)


@repository
def repo():
    return [
        week_3_pipeline_docker,
        week_3_schedule_docker,
        week_3_sensor_docker,
        week_3_pipeline_local,
        week_3_schedule_local,
    ]
