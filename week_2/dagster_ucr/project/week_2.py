import csv
from operator import attrgetter
from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    description="Get a list of stocks from an S3 file.",
)
def get_s3_data(context):
    s3 = context.resources.s3
    stocks = s3.get_data(context.op_config["s3_key"])

    return [Stock.from_list(stock) for stock in stocks]


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"highest_stock": Out(dagster_type=Aggregation)},
    description="Find the stock with the highest value."
)
def process_data(stocks: List[Stock]):
    find_highest: Stock = max(stocks, key=attrgetter("high"))
    return Aggregation(date=find_highest.date, high=find_highest.high)


@op(
    ins={"highest_stock": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    description="Store the highest value stock."
)
def put_redis_data(context, highest_stock: Stock):
    redis = context.resources.redis
    redis.put_data(str(highest_stock.date), str(highest_stock.high))


@graph
def week_2_pipeline():
    s3_data = get_s3_data()
    stock_to_store = process_data(s3_data)
    put_redis_data(stock_to_store)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "week_2/data/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "week_2/data/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

# docker_week_2_pipeline = week_2_pipeline.to_job(
#     name="docker_week_2_pipeline",
#     config=docker,
#     resource_defs={"s3": s3_resource, "redis": redis_resource},
# )
