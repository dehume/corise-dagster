from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


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
def process_data(context, the_stocks: List[Stock]):
    aggregation = max(the_stocks, key=lambda stock: stock.high)
    context.log.info(f"Top Stock: {aggregation}")
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


local = {
    "ops": {
        "get_s3_data": {
            "config": {
                "s3_key": "prefix/stock_9.csv"
            }
        }
    },
}

def get_docker_config(key):
    list = {"resources": {"s3": {"config": {"bucket": "dagster","access_key": "test","secret_key": "test","endpoint_url": "http://host.docker.internal:4566",}},"redis": {"config": {"host": "redis","port": 6379,}},},"ops": {"get_s3_data": {"config": {"s3_key": key}}},}
    return list


# @static_partitioned_config(partition_keys=[str(part_numb+1) for part_numb in range(10)])
# def docker_config(partition_key: str):
#     return {
#         frozenset(get_docker_config(f"prefix/stock_{partition_key}.csv").items())
#             }

@static_partitioned_config(partition_keys=[str(part_numb+1) for part_numb in range(10)])
def docker_config(partition_key: str):
    return {
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
    "ops": {
        "get_s3_data": {
            "config": {
                "s3_key": f"prefix/stock_{partition_key}.csv"
            }
        }
    },
}


@graph
def week_3_pipeline():
    stocks = get_s3_data()
    stock_agg = process_data(stocks)
    put_redis_data(stock_agg)


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=get_docker_config("prefix/stock_9.csv"),
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)

local_week_3_schedule =  ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")
docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")

@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):

    new_keys = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://host.docker.internal:4566",
        since_key = None,
        max_keys = 50
    )

    # No new files found
    if not new_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return

    # If new files found
    for key in new_keys:
        yield RunRequest(
            run_key=key,
            run_config=get_docker_config(key)
        )


