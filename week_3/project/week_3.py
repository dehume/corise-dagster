from operator import attrgetter
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
def week_3_pipeline():
    s3_data = get_s3_data()
    stock_to_store = process_data(s3_data)
    put_redis_data(stock_to_store)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "dagster",
                "secret_key": "dagster",
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

bucket, access_key, secrect_key, endpoint_url =  docker["resources"]["s3"]["config"].values()

partition_keys = [str(i) for i in range(1, 11)]

@static_partitioned_config(partition_keys=partition_keys)
def docker_config(partition_key: str):
    return {
        "resources": {**docker["resources"]},
        "ops": {"get_s3_data": {"config": {"s3_key": f'prefix/stock_{partition_key}.csv'}}}
    }



local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },

)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },

    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)

local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")

@sensor(job=week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket=bucket,
        prefix="prefix"
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config= docker_config(new_file)
        )
