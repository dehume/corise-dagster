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
    schedule,
    DefaultScheduleStatus,
    StaticPartitionsDefinition,
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
    out={"stocks": Out(dagster_type=List[Stock],
                       description="List of Stocks")},
)
def get_s3_data(context):
    # Use your ops from week 2
    output = list()
    for csv_row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(csv_row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"highest_stock": Out(dagster_type=Aggregation)},
    description="Given a list of stocks, return an Aggregation with the highest value"
)
def process_data(stocks):
    # Use your ops from week 2
    highest_stock = max(stocks, key=attrgetter("high"))
    aggregation = Aggregation(date=highest_stock.date, high=highest_stock.high)
    return aggregation


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    description="Given a Aggregation, Upload to Redis"
)
def put_redis_data(context, aggregation):
    # Use your ops from week 2
    context.resources.redis.put_data(aggregation.date, str(aggregation.high))


@graph
def week_3_pipeline():
    # Use your graph from week 2
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(partition_keys=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])
def docker_config(partition: str):
    update_docker_ops = docker
    update_docker_ops["ops"]["get_s3_data"]["config"][
        "s3_key"] = f"prefix/stock_{partition}.csv"
    return update_docker_ops


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
    op_retry_policy=RetryPolicy(
        max_retries=10,
        delay=1,  # 200ms
    ),

)


local_week_3_schedule = ScheduleDefinition(
    job=local_week_3_pipeline, cron_schedule="*/15 * * * *", default_status=DefaultScheduleStatus.RUNNING)  # Add your schedule

docker_week_3_schedule = ScheduleDefinition(
    job=docker_week_3_pipeline, cron_schedule="0 * * * *", default_status=DefaultScheduleStatus.RUNNING)  # Add your schedule


# Struggled to figure out what this job is
@ sensor(job=docker_week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://localstack:4566",
        since_key=".",
        max_keys=1000,
    )
    updated_config = docker_config
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:

        yield RunRequest(
            run_key=new_file,
            # updated_config["ops"]["get_s3_data"]["config"]["s3_key"] = new_file -- this is failing with ParsedConfig not subscriptable issue , Need to figure out how to update the config
            # run_config=updated_config -- this is failing with ParsedConfig not subscriptable issue
            run_config={"resources": {
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
                "ops": {"get_s3_data": {"config": {"s3_key": new_file}}},
            },
        )
