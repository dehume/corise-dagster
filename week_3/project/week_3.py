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
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock],
    description="Upload highest stock to Redis")},
    tags={"kind": "s3"},
)
def get_s3_data(context):
    output = list()
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"highest_stock": Out(dagster_type=Aggregation)},
    tags={"kind": "python"},
    description="Return the stock with the highest value"
)
def process_data(stocks):
    highest_stock = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op(
    ins={"highest_stock": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    required_resource_keys={"redis"},
    tags={"kind": "Redis"},
    description="Return the stock with the highest value"
)
def put_redis_data(context, highest_stock):
    context.resources.redis.put_data(
        name=f"{highest_stock.date}:%m/%d/%Y",
        value=str(highest_stock.high)
    )


@graph
def week_3_pipeline():
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
