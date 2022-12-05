from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    String,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
    build_schedule_from_partitioned_job
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={
        "s3_key": String
    },
    required_resource_keys={"s3"}
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(item) for item in context.resources.s3.get_data(s3_key)]

@op (
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"agg": Out(dagster_type=Aggregation)}
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highest_stock = max(stocks, key= lambda stock: stock.high)
    return Aggregation(date=highest_stock.date,high=highest_stock.high)


@op(
    required_resource_keys={"redis"}
)
def put_redis_data(context, agg: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=str(agg.date), value=str(agg.high))


@op(
    required_resource_keys={"s3"}
)
def put_s3_data(context, agg: Aggregation) -> Nothing:
    context.resources.s3.put_data(key=agg.date, data=agg)


@graph
def week_3_pipeline():
    stocks = get_s3_data()
    processed = process_data(stocks)
    put_redis_data(processed)
    put_s3_data(processed)


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

@static_partitioned_config(
    partition_keys=[str(i) for i in range(1,11)]
)
def docker_config(partition_key: str):
    partitioned_config = docker.copy()
    partitioned_config["ops"]["get_s3_data"]["config"]["s3_key"] = f"prefix/stock_{partition_key}.csv"
    return partitioned_config

week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config= local,
    resource_defs = {
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource()
    },
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config= docker_config,
    resource_defs = {
        "s3": s3_resource,
        "redis": redis_resource
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)


week_3_schedule_local = ScheduleDefinition(
    job=week_3_pipeline_local,
    cron_schedule="15 * * * *"
)


@schedule
def week_3_schedule_docker():
    return build_schedule_from_partitioned_job(week_3_pipeline_docker)


@sensor(
    job=week_3_pipeline_docker,
    minimum_interval_seconds=30
)
def week_3_sensor_docker(context):
    new_files = get_s3_keys(
        bucket=context.resources.s3.config.bucket
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config= {
                "ops": {
                    "get_s3_data": {
                        "config": {
                            "s3_key": "prefix/stock_9.csv"
                        }
                    }
                }
            }
        )

