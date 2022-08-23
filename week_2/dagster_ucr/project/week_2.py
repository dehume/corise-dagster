from typing import List

from dagster import ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    tags={"kind": "s3"},
)
def get_s3_data(context) -> List[Stock]:
    s3 = context.resources.s3
    return [
        Stock.from_list(data)
        for data in s3.get_data(context.op_config["s3_key"])
    ]


@op
def process_data(stocks: List[Stock]) -> Aggregation:
    # Use your op from week 1
    highest = max(stocks, key=lambda stock: stock.high)
    return Aggregation(
        date=highest.date,
        high=highest.high,
    )


@op(
    required_resource_keys={"redis"},
    tags={"kind": "redis"}
)
def put_redis_data(context, agg: Aggregation):
    redis = context.resources.redis
    redis.put_data(agg.date, agg.high)
    context.log.info(f"put key: ({agg.date}, val: {agg.high}) to redis")


@graph
def week_2_pipeline():
    # Use your graph from week 1
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
