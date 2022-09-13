from typing import List
from datetime import datetime

from dagster import asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
    group_name="corise",
)
def get_s3_data(context):
    output = list()
    s3_key = context.op_config["s3_key"]
    for row in context.resources.s3.get_data(s3_key):
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    description="Get the date and high of the highest high",
    group_name="corise",
)
def process_data(get_s3_data):
    highest: float = 0 # stocks cannot be lower than 0
    date_of_highest: datetime = None 
    for stock in get_s3_data:
      if stock.high > highest:
        highest = stock.high
        date_of_highest = stock.date
    return Aggregation(date=date_of_highest, high=highest)

@asset(
    required_resource_keys={"redis"},
    description="Upload an aggregation to Redit",
    group_name="corise",
)
def put_redis_data(context, process_data):
    name = process_data.date.strftime("%Y-%m-%d")
    value = str(process_data.high)
    context.resources.redis.put_data(name, value)

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
