import datetime
from operator import attrgetter
from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock],
    description="List of Stocks")},
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
    description="Given a list of stocks, return an Aggregation with the highest value"
)
def process_data(stocks):
    # context.log.info(f"Looping through {len(stocks)} stocks")
    highest_stock = max(stocks, key=attrgetter("high"))
    aggregation = Aggregation(date=highest_stock.date, high=highest_stock.high)
    # context.log.info(f"Highest value: {aggregation}")
    return aggregation


@op(
    required_resource_keys={"redis"},
    ins={"highest_stock": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    description="Upload aggregations to Redis",
    tags={"kind": "redis"},
)
def put_redis_data(context, highest_stock):
    context.resources.redis.put_data(
        name=f"{highest_stock.date}:%m/%d/%Y",
        value=str(highest_stock.high)
    )


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
