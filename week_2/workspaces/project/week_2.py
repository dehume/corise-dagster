from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context) -> List[Stock]:
    files = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(file) for file in files]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key=lambda stock: stock.high, reverse=True)
    return Aggregation(date = sorted_stocks[0].date, high = sorted_stocks[0].high)


@op(
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, agg: Aggregation):
    context.resources.redis.put_data(agg.date, agg.high)


@op(
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def put_s3_data(context, agg: Aggregation):
    context.resources.s3.put_data("stocks_bucket", agg.date, agg)


@graph
def week_2_pipeline():
    files = get_s3_data()
    stocks = process_data(files)
    put_redis_data(stocks)
    put_s3_data(stocks)


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
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    },
)
