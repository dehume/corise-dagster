from typing import List
from datetime import datetime

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    s3_key = context.op_config["s3_key"]
    for row in context.resources.s3.get_data(s3_key):
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Get the date and high of the highest high",
)
def process_data(stock_list):
    highest: float = 0 # stocks cannot be lower than 0
    date_of_highest: datetime = None 
    for stock in stock_list:
      if stock.high > highest:
        highest = stock.high
        date_of_highest = stock.date
    return Aggregation(date=date_of_highest, high=highest)

@op(
    required_resource_keys={"redis"},
    description="Upload an aggregation to Redit",
)
def put_redis_data(context, agg):
    name = agg.date.strftime("%Y-%m-%d")
    value = str(agg.high)
    context.resources.redis.put_data(name, value)


@graph
def week_2_pipeline():
    put_redis_data(process_data(get_s3_data()))


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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
