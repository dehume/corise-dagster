from dagster import Definitions
from workspaces.project.week_3 import (
    machine_learning_job_docker,
    machine_learning_job_local,
    machine_learning_schedule_docker,
    machine_learning_schedule_local,
    machine_learning_sensor_docker,
)

definition = Definitions(
    schedules=[machine_learning_schedule_local, machine_learning_schedule_docker],
    sensors=[machine_learning_sensor_docker],
    jobs=[machine_learning_job_docker, machine_learning_job_local],
)
