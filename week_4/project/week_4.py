from typing import List

from dagster import Nothing, asset, with_resources, OpExecutionContext, get_dagster_logger
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    group_name="corise",
    description="Get a list of stocks from an S3 file"
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    stocks = context.resources.s3.get_data(context.op_config["s3_key"])
    stocklist = [Stock.from_list(stock) for stock in stocks]
    return stocklist


@asset(
    description="Determine the stock with the highest value",
    group_name="corise",
)
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    # Find the stock with the highest value
    max_stock = max(get_s3_data, key=lambda x: x.high)
    # Log the output
    logger = get_dagster_logger()
    logger.info(f"Higest stock is: {max_stock}")
    agg = Aggregation(date=max_stock.date, high=max_stock.high)
    return agg


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
    op_tags={"kind": "redis"},
    description="Write to Redis"
)
def put_redis_data(context: OpExecutionContext, process_data: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=str(process_data.date), value=str(process_data.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566"
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379
            }
        },
    },
)
