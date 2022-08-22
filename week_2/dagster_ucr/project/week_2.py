from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op
def get_s3_data():
    pass


@op(
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Take a list of Stonks and return the phattest one.",
)
def process_data(stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key=lambda x: x.high, reverse=True)
    top = sorted_stocks[0]
    agg = Aggregation(date=top.date, high=top.high)
    return agg


@op
def put_redis_data():
    pass


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    processed = process_data(stocks)
    put_redis_data(processed)


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
