from typing import List
import logging
from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    required_resource_keys={'s3'},
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file")
def get_s3_data(context):
    stocks = []
    for record in context.resources.s3.get_data(context.op_config["s3_key"]):
        stocks.append(Stock.from_list(record))
    return stocks


@op
def process_data(raw_stocks: List[Stock]) -> Aggregation:
    highest_stock = max(raw_stocks, key=lambda x: x.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op(
    required_resource_keys={'redis'},
    ins={"aggregation": In(dagster_type=Aggregation)},
    description="Put Aggregation data into Redis",
    tags={"kind": "redis"}
)
def put_redis_data(context, aggregation: Aggregation):
    context.resources.redis.put_data(aggregation.date, aggregation.high)

@graph
def week_2_pipeline():
    put_redis_data(process_data(get_s3_data()))


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
