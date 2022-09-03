from dagster import repository
from project.week_3 import (
    docker_week_3_pipeline,
    docker_week_3_schedule,
    local_week_3_pipeline,
    local_week_3_schedule,
    docker_week_3_sensor
)

@repository
def repo():
    return [
        docker_week_3_pipeline,
        local_week_3_pipeline,
        local_week_3_schedule,
        docker_week_3_schedule,
        docker_week_3_sensor
    ]
