from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context):
    output = list()
    #key = context.resources.op_config["s3_key"]
    stocks = context.resources.s3.get_data("key_name")
    for row in stocks:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(out={"aggregation": Out(dagster_type=Aggregation, description= "return class with values")})
def process_data(stocks):
    high_val = 0
    date = datetime
    
    for stock in stocks:
        if stock.high > high_val:
            high_val = stock.high
            date = stock.date
    
    return Aggregation(date=date, high=high_val)



@op(out={"aggregation": Out(dagster_type=Aggregation, description= "return class with values")})
def process_data(stocks):
    high_val = 0
    date = datetime
    
    for stock in stocks:
        if stock.high > high_val:
            high_val = stock.high
            date = stock.date
    
    return Aggregation(date=date, high=high_val)

@op(
    #config_schema={"host": str, "port": int},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(aggregation.date, aggregation.high)


@graph
def week_2_pipeline():
    put_redis_data(process_data(get_s3_data()))
   


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


def docker_config():
    pass


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
)


local_week_3_schedule = None  # Add your schedule

docker_week_3_schedule = None  # Add your schedule


@sensor
def docker_week_3_sensor():
    pass
