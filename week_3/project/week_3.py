from typing import List

from dagster import (
    Nothing,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    SkipReason,
    graph,
    op,
    sensor,
)

from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op
def get_s3_data(context):
    # Use your ops from week 2
    pass


@op
def process_data():
    # Use your ops from week 2
    pass


@op
def put_redis_data(context, aggregation: Aggregation) -> Nothing:
    # Use your ops from week 2
    pass


@graph
def week_3_pipeline():
    # Use your graph from week 2
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
                "endpoint_url": "http://host.docker.internal:4566",
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


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = None # Add your schedule

docker_week_3_schedule = None # Add your schedule


@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=60)
def docker_week_3_sensor(context):
    new_s3_keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://host.docker.internal:4566")
    if not new_s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for s3_key in new_s3_keys:
        yield RunRequest(
            run_key=s3_key,
            run_config={
                "resources": {
                    "s3": {
                        "config": {
                            "bucket": "dagster",
                            "access_key": "test",
                            "secret_key": "test",
                            "endpoint_url": "http://host.docker.internal:4566",
                        }
                    },
                    "redis": {
                        "config": {
                            "host": "redis",
                            "port": 6379,
                        }
                    },
                },
                "ops": {"get_s3_data": {"config": {"s3_key": s3_key}}},
            },
        )
