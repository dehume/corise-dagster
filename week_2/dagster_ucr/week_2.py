from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op

from dagster_ucr.resources import redis_resource, s3_resource
from dagster_ucr.types import Aggregation, Stock


@op
def get_s3_data(context):
    pass


@op(
    config_schema={"month": int},
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Process data",
)
def process_data(context, stocks):
    # Use your op from week 1
    pass


@op()
def put_redis_data(context, aggregation: Aggregation) -> Nothing:
    pass


@graph
def week_2_pipeline():
    stock_data = get_s3_data()
    agg_data = process_data(stock_data)
    put_redis_data(agg_data)


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
                "endpoint_url": "http://localhost:4566",
            }
        },
        "redis": {
            "config": {
                "host": "cache",
                "port": 6379,
            }
        },
    },
    "ops": {"process_data": {"config": {"month": 9}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="loacl_week_2_pipeline",
    config=local,
    resource_defs={"s3": ResourceDefinition.mock_resource(), "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
