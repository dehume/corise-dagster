from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op
def get_s3_data():
    pass


@op
def process_data():
    pass


@op
def put_redis_data():
    pass


@graph
def week_2_pipeline():
    pass


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
)
