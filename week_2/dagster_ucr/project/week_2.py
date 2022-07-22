from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op

from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource

S3_KEY = "prefix/stock.csv"


@op
def get_s3_data():
    pass


@op
def process_data(context, stocks):
    """
    Use your op from week 1
    """


@op
def put_redis_data():
    pass


@graph
def week_2_pipeline():
    """
    Use your job from week 1
    """


local = {
    "ops": {"process_data": {"config": {"month": 9}}},
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
    "ops": {"process_data": {"config": {"month": 9}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
