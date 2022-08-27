from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context) -> List[Stock]:
    res = []
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        res.append(stock)
    return res


@op(tags={"kind": "business_logic"})
def process_data(stocks: List[Stock]) -> Aggregation:
    highest_val_stock = max(stocks, key=lambda x: x.high)
    return Aggregation(date=highest_val_stock.date, high=highest_val_stock.high)


@op(
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, agg: Aggregation):
    context.resources.redis.put_data(
        name=f"{agg.date}",
        value=agg.high,
    )


@graph
def week_2_pipeline():
    stock_data = get_s3_data()
    processed = process_data(stock_data)
    put_redis_data(processed)


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
