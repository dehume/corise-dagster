from dagster import Definitions
from workspaces.content.etl import etl_docker, etl_local, etl_local_partitioned_schedule
from workspaces.content.io_retry import job_local_io_manager, job_local_io_manager_retry

definition = Definitions(
    schedules=[etl_local_partitioned_schedule],
    jobs=[job_local_io_manager, job_local_io_manager_retry, etl_docker, etl_local],
)
