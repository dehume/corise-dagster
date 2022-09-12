from typing import List
from datetime import datetime as dt
from dagster import Nothing, asset, with_resources,get_dagster_logger,AssetIn
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock

logger = get_dagster_logger()

@asset(required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    op_tags={"kind": "s3"},
    group_name="corise",
    description="Get a list of stocks from an S3 file"
)
def get_s3_data(context):
    s3 = context.resources.s3
    stocks = s3.get_data(context.op_config["s3_key"])

    return [Stock.from_list(stock) for stock in stocks]

@asset(
    group_name="corise",
    description="Find the stock with the highest value."
)
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    highest_stock = max(get_s3_data, key=lambda stock: stock.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@asset(required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    group_name="corise",
    description="Store the highest value stock."
)
def put_redis_data(context,process_data: Aggregation):
    agg_date = dt.strftime(process_data.date, '%m/%d/%Y')
    agg_high = str(process_data.high)
    context.resources.redis.put_data(agg_date, agg_high)
    logger.info(f"Date: {agg_date} with daily high of ${agg_high} stored on Redis")



get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
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
        },
)