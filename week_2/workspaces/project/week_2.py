from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    description="Get a list of stocks from an S3 file"
)
def get_s3_data(context) -> List[Stock]:
    return [Stock.from_list(record) for record in context.resources.s3.get_data(context.op_config["s3_key"])]


@op(
    description="Given a list of stocks, return the Aggregation with the greatest high value"
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key=lambda stock: stock.high, reverse=True)
    return Aggregation(date = sorted_stocks[0].date, high = sorted_stocks[0].high)


@op(
    required_resource_keys={"redis"},
    description="Upload the Aggregation to Redis"
)
def put_redis_data(context, stock: Aggregation):
    context.resources.redis.put_data(stock.date, stock.high)


@op(
    required_resource_keys={"s3"},
    description="Upload the Aggregation to S3"
)
def put_s3_data(context, stock: Aggregation):
    context.resources.s3.put_data("stocks/{date}/{high}".format(date = stock.date, high = stock.high), stock)


@graph
def week_2_pipeline():
    s3_data = get_s3_data()
    processed_data = process_data(s3_data)
    put_redis_data(processed_data)
    put_s3_data(processed_data)


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

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource()
    }
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    }
)