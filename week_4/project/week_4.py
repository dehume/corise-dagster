from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    description="Get a list of stocks from an S3 file",
    group_name="corise"
)
def get_s3_data(context):
    output = list()
    key = context.op_config["s3_key"]
    data = context.resources.s3.get_data(key)

    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output 


@asset(
    description="Filter for largest daily high stock value",
    group_name="corise"
)
def process_data(get_s3_data):
    #get max of daily highest stock value
    stock_high = max(get_s3_data, key = lambda x: x.high)
    return Aggregation(date = stock_high.date, high = stock_high.high)


@asset(
     required_resource_keys={"redis"},
     group_name="corise"
      )
def put_redis_data(context, process_data):
    context.resources.redis.put_data(
        name = str(process_data.date), 
        value = str(process_data.high)
        )


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource,
                    "redis": redis_resource},
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
        }
    }
)
