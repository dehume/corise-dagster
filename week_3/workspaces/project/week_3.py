from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op
def get_s3_data():
    # You can reuse the logic from the previous week
    pass


@op
def process_data():
    # You can reuse the logic from the previous week
    pass


@op
def put_redis_data():
    # You can reuse the logic from the previous week
    pass


@op
def put_s3_data():
    # You can reuse the logic from the previous week
    pass


@graph
def week_3_pipeline():
    # You can reuse the logic from the previous week
    pass


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


def docker_config():
    pass


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
)


week_3_schedule_local = None


@schedule
def week_3_schedule_docker():
    pass


@sensor
def week_3_sensor_docker():
    pass
