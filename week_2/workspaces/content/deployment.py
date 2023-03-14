from dagster import Definitions
from workspaces.content.etl import etl_docker, etl_local
from workspaces.content.hello import job
from workspaces.content.jupyter import (
    week_2_job_jupyter_docker,
    week_2_job_jupyter_local,
)

definition = Definitions(
    jobs=[
        etl_local,
        etl_docker,
        job,
        week_2_job_jupyter_local,
        week_2_job_jupyter_docker,
    ],
)
