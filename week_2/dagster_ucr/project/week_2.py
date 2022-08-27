from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    out={"the_stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={'s3'},
    tags={"kind": "s3"},
    description="List of Stocks",
)
def get_s3_data(context):
    stocklist = list()
    s3_key = context.op_config["s3_key"]
    for row in context.resources.s3.get_data(s3_key):
        stock = Stock.from_list(row)
        stocklist.append(stock)
    return stocklist


@op(
    description="Return Aggregation from stock list with the greatest `high` value",
    ins={"the_stocks": In(dagster_type=List[Stock])},
    out={"Aggregation": Out(Aggregation)},
)
def process_data(the_stocks: List[Stock]):
    aggregation = max(the_stocks, key=lambda stock: stock.high)
    return Aggregation(date=aggregation.date, high=aggregation.high)


@op(
    description="Upload to Redis",
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(Nothing),
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, aggregation) -> Nothing:
    context.resources.redis.put_data("agg_data", str(aggregation))


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    stock_agg = process_data(stocks)
    put_redis_data(stock_agg)


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
