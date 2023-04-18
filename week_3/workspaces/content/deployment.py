from dagster import Definitions
from workspaces.content.assets import asset_job
from workspaces.content.etl import etl_docker, etl_local, etl_local_partitioned_schedule
from workspaces.content.io_retry import job_local_io_manager, job_local_io_manager_retry
from workspaces.content.logging import logging_dev_job, logging_prod_job

definition = Definitions(
    schedules=[etl_local_partitioned_schedule],
    jobs=[
        job_local_io_manager,
        job_local_io_manager_retry,
        etl_docker,
        etl_local,
        asset_job,
        logging_dev_job,
        logging_prod_job,
    ],
)