from dagster import Definitions
from workspaces.challenge.week_2_challenge import dbt_job_docker

definition = Definitions(jobs=[dbt_job_docker])
