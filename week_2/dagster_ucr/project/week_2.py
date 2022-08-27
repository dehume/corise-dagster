from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource
from dagster import get_dagster_logger
from datetime import datetime as dt

logger = get_dagster_logger()


@op(required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    data = context.resources.s3.get_data(context.op_config["s3_key"])
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Aggregate stock data",
)
def process_data(stocks: List[Stock]) -> Aggregation:
    top_high = sorted(stocks, key=lambda x: x.high, reverse=True)[0]
    return Aggregation(date=top_high.date, high=top_high.high)


@op(required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(Nothing),
    tags={"kind": "redis"},
    description="Upload aggregation to Redis",
)
def put_redis_data(context,aggregation: Aggregation):
    agg_date = dt.strftime(aggregation.date, '%m/%d/%Y')
    agg_high = str(aggregation.high)
    context.resources.redis.put_data(agg_date, agg_high)
    logger.info(f"Date: {agg_date} with daily high of ${agg_high} stored on Redis")


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    put_redis_data(aggregation)

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
