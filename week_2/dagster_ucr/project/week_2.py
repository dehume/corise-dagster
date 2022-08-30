from typing import List
from operator import attrgetter

from dagster import In, Nothing, Out, ResourceDefinition, graph, op, OpExecutionContext
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource
from dagster_ucr.resources import S3
from dagster_ucr.resources import Redis


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    out={"stocks": Out(dagster_type=List[Stock], description="List of Stocks")},
)
def get_s3_data(context: OpExecutionContext):
    output = list()
    s3: S3 = context.resources.s3  # assinged to a variable for better typing support
    data = s3.get_data(key_name=context.op_config["s3_key"])

    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"agg": Out(dagster_type=Aggregation)},
    tags={},
    description="takes the list of stocks and determines the Stock with the greatest `high` value",
)
def process_data(stocks: List[Stock]):
    highest_stock: Stock = max(stocks, key=attrgetter("high"))
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op(
    ins={"agg": In(dagster_type=Aggregation)},
    tags={"kind": "redis"},
    required_resource_keys={"redis"},
)
def put_redis_data(context: OpExecutionContext, agg: Aggregation) -> Nothing:
    redis: Redis = context.resources.redis  # assinged to a variable for better typing support
    redis.put_data(name=agg.date.isoformat(), value=str(agg.high))


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
