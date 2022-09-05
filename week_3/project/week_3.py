from typing import List
import heapq

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
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(partition_keys=[str(num) for num in range(1,11)])
def docker_config(partition_key):
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
        "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_{}.csv".format(partition_key)}}},
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
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")  # Add your schedule

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")  # Add your schedule


@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):
    new_keys = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://host.docker.internal:4566",
        since_key = None
    )
    if not new_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_key in new_keys:
        yield RunRequest(
            run_key=new_key,
            run_config={
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
        "ops": {"get_s3_data": {"config": {"s3_key": new_key}}}}
        )
