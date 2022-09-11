from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op, get_dagster_logger
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context) -> List[Stock]:
    stocks = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(stock) for stock in stocks]


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"agg_max": Out(dagster_type=Aggregation)},
    tags={"kind": "transformation"},
    description="Determine the stock with the highest value"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    # Find the stock with the highest value
    max_stock = max(stocks, key=lambda x: x.high)
    # Log the output
    logger = get_dagster_logger()
    logger.info(f"Higest stock is: {max_stock}")
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    ins={"agg_max": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    out=Out(dagster_type=Nothing),
    tags={"kind": "redis"},
    description="Write to Redis"
)
def put_redis_data(context, agg_max: Aggregation) -> None:
    data = context.resources.redis.put_data(str(agg_max.date), str(agg_max.high))
    context.log.info(f"Write {data} to Redis.")


@graph
def week_2_pipeline():
    s3_data = get_s3_data()
    highest_stock = process_data(s3_data)
    put_redis_data(highest_stock)


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
