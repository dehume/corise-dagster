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
def week_3_pipeline():
    stock_data = get_s3_data()
    processed = process_data(stock_data)
    put_redis_data(processed)


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


@static_partitioned_config(partition_keys=[str(num) for num in range(1, 11)])
def docker_config(partition_key: str):
    update_docker_ops = docker
    update_docker_ops["ops"]["get_s3_data"]["config"][
        "s3_key"] = f"prefix/stock_{partition_key}.csv"
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
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = ScheduleDefinition(
    job=local_week_3_pipeline,
    cron_schedule="*/15 * * * *",
)

docker_week_3_schedule = ScheduleDefinition(
    job=docker_week_3_pipeline,
    cron_schedule="0 * * * *",
)


@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://host.docker.internal:4566",
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return

    for filekey in new_files:
        yield RunRequest(
            run_key=filekey,
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
                "ops": {"get_s3_data": {"config": {"s3_key": filekey}}},
            }
        )