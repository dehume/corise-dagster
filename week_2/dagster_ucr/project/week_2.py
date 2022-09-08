# pyright: reportMissingImports=false
from typing import List
from operator import attrgetter

from dagster import In, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Getting a list of stocks from an S3 file",
)
def get_s3_data(context):
    key_name = context.op_config["s3_key"]
    output = list()
    for record in context.resources.s3.get_data(key_name):
        stock = Stock.from_list(record)
        output.append(stock)
    return output


@op(
    ins={"stock_list": In(dagster_type=List[Stock])},
    out={"agg": Out(dagster_type=Aggregation)},
    description="Find the highest high value from a list of stocks"
    )
def process_data(stock_list) -> Aggregation:
    high_stock = max(stock_list, key=attrgetter('high'))
    agg = Aggregation(date=high_stock.date, high=high_stock.high)
    return agg


@op(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    description="Put Aggregation data into Redis",
    tags={"kind": "redis"}
    )
def put_redis_data(context, aggregation: Aggregation):
    date = str(aggregation.date)
    high = str(aggregation.high)
    context.log.info(f"Putting stock {date} with high of {high} into Redis")
    context.resources.redis.put_data(date, high)


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
