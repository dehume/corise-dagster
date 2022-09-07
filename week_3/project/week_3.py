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
    OpExecutionContext,
    schedule,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource, S3, Redis
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    out={"stocks": Out(dagster_type=List[Stock], description="List of Stocks")},
)
def get_s3_data(context: OpExecutionContext):
    s3: S3 = context.resources.s3  # assinged to a variable for better typing support
    data = s3.get_data(key_name=context.op_config["s3_key"])

    output = [Stock.from_list(row) for row in data]
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
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker_resources = {
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
}


@static_partitioned_config(partition_keys=[str(x) for x in range(1, 11)])
def docker_config(partition_key: str):
    return {
        **docker_resources,
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
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
    op_retry_policy=RetryPolicy(max_retries=10, delay=1, backoff=None, jitter=None),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")


@schedule(cron_schedule="0 * * * *", job=docker_week_3_pipeline)
def docker_week_3_schedule():
    request = docker_week_3_pipeline.run_request_for_partition(partition_key="1", run_key=None)
    yield request


@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):
    new_keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://host.docker.internal:4566")
    if not new_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for s3_key in new_keys:
        yield RunRequest(
            run_key=s3_key,
            run_config={
                **docker_resources,
                "ops": {"get_s3_data": {"config": {"s3_key": s3_key}}},
            },
        )
