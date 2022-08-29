from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from week_2.dagster_ucr.project.types import Aggregation, Stock
from week_2.dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str}
)
def get_s3_data(context):
    iterator = context.resources.s3.get_data(key_name=context.op_config["s3_key"])
    stocks = [Stock.from_list(stk) for stk in iterator]
    return stocks


@op
def process_data(stocks: List[Stock]) -> Aggregation:
    most_expensive_stock = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=most_expensive_stock.date, high=most_expensive_stock.high)


@op(required_resource_keys={"redis"})
def put_redis_data(context, aggregation: Aggregation):
    context.resources.redis.put_data(aggregation.date, aggregation.high)


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    agg = process_data(stocks)
    put_redis_data(agg)


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
