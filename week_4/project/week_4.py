from typing import List

from dagster import Nothing, Out, In, asset, with_resources, get_dagster_logger
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    group_name="corise",
    description="Accesses S3 to pull a list of Stocks",
)
def get_s3_data(context):
    output = list()
    
    for row in context.resources.s3.get_data(key_name=context.op_config["s3_key"]): 
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    group_name="corise",
    description="Takes list of stocks and outputs one with highest 'high' value",
)
def process_data(get_s3_data):
    stock_choice = max(get_s3_data, key=lambda x: x.high)
    logger = get_dagster_logger()
    logger.info(f'Picked top stock: {stock_choice}')
    return Aggregation(date=stock_choice.date, high=stock_choice.high)


@asset(
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    group_name="corise",
    description="Upload aggregations to Redis",
)
def put_redis_data(context, process_data) -> Nothing:
    logger = get_dagster_logger()
    logger.info(f'Writing the follow record to Redis: {process_data}')
    context.resources.redis.put_data(name=str(process_data.date), value=str(process_data.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions= [get_s3_data, process_data, put_redis_data],
    resource_defs= {"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key= {
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
