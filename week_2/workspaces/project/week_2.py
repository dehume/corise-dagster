from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"}
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_key)]

@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    # get the stock with the highest number of stock.high
    highest_stock = max(stocks, key=lambda stock:stock.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op (
    required_resource_keys={"redis"}
)
def put_redis_data(context, stock_aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=str(stock_aggregation.date), value=str(stock_aggregation.high))

@op (
    required_resource_keys={"s3"}
)
def put_s3_data(context, stock_aggregation: Aggregation) -> Nothing:
    context.resources.s3.put_data(key=stock_aggregation.date, data=stock_aggregation)

@graph
def week_2_pipeline():
    high_stocks = process_data(get_s3_data())
    put_redis_data(high_stocks)
    put_s3_data(high_stocks)

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
        "redis": ResourceDefinition.mock_resource()
    }
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    }
)
