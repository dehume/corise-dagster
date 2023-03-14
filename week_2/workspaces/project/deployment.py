from dagster import Definitions
from workspaces.project.week_2 import (
    machine_learning_job_docker,
    machine_learning_job_local,
)

definition = Definitions(jobs=[machine_learning_job_docker, machine_learning_job_local])
