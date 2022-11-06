from dagster import repository
from workspaces.content.etl import etl_docker, etl_local, etl_local_partitioned_schedule
from workspaces.content.io_retry import job_local_io_manager, job_local_io_manager_retry


@repository
def repo():
    return [job_local_io_manager, job_local_io_manager_retry, etl_docker, etl_local, etl_local_partitioned_schedule]
