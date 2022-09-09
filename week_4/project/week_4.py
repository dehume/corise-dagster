from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    description="Upload highest stock to Redis",
    op_tags={"kind": "s3"},
)
def get_s3_data(context):
    stocks = []
    for stock in context.resources.s3.get_data(context.op_config["s3_key"]):
        stocks.append(Stock.from_list(stock))
    return stocks


@asset(
    op_tags={"kind": "python"},
    description="Return the stock with the highest value"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    highest_stock = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@asset(
    required_resource_keys={"redis"},
    op_tags={"kind": "Redis"},
    description="Return the stock with the highest value"
)
def put_redis_data(context, max_stock: Aggregation):
    context.resources.redis.put_data(str(max_stock.date), str(max_stock.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    },
    resource_config_by_key={
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
    }
)