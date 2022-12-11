from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": str}, required_resource_keys={"s3"})
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    context.log.info(f"Getting {s3_key} from S3")
    records = context.resources.s3.get_data(s3_key)
    stocks = [Stock.from_list(r) for r in records]
    return stocks


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    max_stock = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(required_resource_keys={"redis"})
def put_redis_data(context, agg: Aggregation):
    context.log.info(f"Putting {agg} into Redis")
    context.resources.redis.put_data(agg.date.isoformat(), f"{agg.high}")


@op(required_resource_keys={"s3"})
def put_s3_data(context, agg: Aggregation):
    s3_key = agg.date.isoformat()
    context.log.info(f"Putting {agg} into S3 at {s3_key}")
    context.resources.s3.put_data(s3_key, agg)


@graph
def week_2_pipeline():
    agg = process_data(get_s3_data())
    put_redis_data(agg)
    put_s3_data(agg)


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
        "redis": ResourceDefinition.mock_resource(),
    },
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
)
