from typing import List
import heapq
from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    op_tags={"kind":"s3"},
    group_name="corise"
)
def get_s3_data(context):
    stocks = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(stock) for stock in stocks]


@asset(
    group_name="corise"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    stock_high_list = [stock.high for stock in stocks]
    highest_stock = float(heapq.nlargest(1,stock_high_list)[0])
    highest_date = stocks[stock_high_list.index(highest_stock)].date
    return Aggregation(date=highest_date,high=highest_stock)


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
    description="Store highest_stock in Redis",
)
def put_redis_data(context, highest_stock: Aggregation):
    context.resources.redis.put_data(str(highest_stock.date), str(highest_stock.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data,process_data,put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
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
    }     
)
