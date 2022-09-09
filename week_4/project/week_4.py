from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    group_name="corise",
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    stocks = []
    for stock in context.resources.s3.get_data(context.op_config["s3_key"]):
        stocks.append(Stock.from_list(stock))
    return stocks


@asset(
    group_name="corise",
    description="Get an aggregation of the highest stock",
)
def process_data(stocks: List[Stock]) -> Aggregation:
    """
    Returns the aggregated Stock with the greatest high value
    from a list of Stocks
    """
    max_stock = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@asset(
    group_name="corise",
    required_resource_keys={"redis"},
)
def put_redis_data(context, max_stock: Aggregation):
    context.resources.redis.put_data(str(max_stock.date), str(max_stock.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": { "config": {
            "host": "redis",
            "port": 6379
            },
        }
    },
)