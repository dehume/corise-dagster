from typing import List
import heapq
from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock], description="List of Stocks")},
)
def get_s3_data(context):
    stocks = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(stock) for stock in stocks]


@op(
    ins={'stocks': In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Receives list from S3 Data and selects highest stock value"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    stock_high_list = [stock.high for stock in stocks]
    highest_stock = float(heapq.nlargest(1,stock_high_list)[0])
    highest_date = stocks[stock_high_list.index(highest_stock)].date
    return Aggregation(date=highest_date,high=highest_stock)


@op(
    ins={"highest_stock": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    out=Out(Nothing),
    tags={"kind": "redis"},
    description="Store highest_stock in Redis",
)
def put_redis_data(context, highest_stock: Aggregation):
    context.resources.redis.put_data(str(highest_stock.date), str(highest_stock.high))

@graph
def week_2_pipeline():
    # Use your graph from week 1
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
