# pyright: reportMissingImports=false
from typing import List
from operator import attrgetter

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
    schedule
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Getting a list of stocks from an S3 file",
)
def get_s3_data(context):
    key_name = context.op_config["s3_key"]
    output = list()
    for record in context.resources.s3.get_data(key_name):
        stock = Stock.from_list(record)
        output.append(stock)
    return output


@op(
    ins={"stock_list": In(dagster_type=List[Stock])},
    out={"agg": Out(dagster_type=Aggregation)},
    description="Find the highest high value from a list of stocks"
    )
def process_data(stock_list) -> Aggregation:
    high_stock = max(stock_list, key=attrgetter('high'))
    agg = Aggregation(date=high_stock.date, high=high_stock.high)
    return agg


@op(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    description="Put Aggregation data into Redis",
    tags={"kind": "redis"}
    )
def put_redis_data(context, aggregation: Aggregation):
    date = str(aggregation.date)
    high = str(aggregation.high)
    context.log.info(f"Putting stock {date} with high of {high} into Redis")
    context.resources.redis.put_data(date, high)


@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

def config_setup(config_key: str):
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
        "ops": {"get_s3_data": {"config": {"s3_key": f"{config_key}"}}},
    }
    return docker

partition_keys = [str(i) for i in range(1, 11)]

@static_partitioned_config(partition_keys=partition_keys)
def docker_config(partition_key: str):
    file = f"prefix/stock_{partition_key}.csv"
    partition_config = config_setup(file)
    return partition_config


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

@schedule(
    cron_schedule="0 * * * *",
    job=docker_week_3_pipeline,
    tags={"kind": "schedule"},
    description="Run scheduled to start at the beginning of each hour"
)
def docker_week_3_schedule():
     for partition_key in partition_keys:
        request = docker_week_3_pipeline.run_request_for_partition(partition_key=partition_key, run_key=partition_key)
        yield request

@sensor(
    job=docker_week_3_pipeline, 
    minimum_interval_seconds=30
)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://host.docker.internal:4566"
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config=config_setup(new_file)
     )
