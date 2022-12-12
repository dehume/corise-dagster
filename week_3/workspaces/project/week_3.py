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
    schedule,
    sensor,
    static_partitioned_config,
    String
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context) -> List[Stock]:
    files = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(file) for file in files]


@op
def process_data(stocks: List[Stock]) -> Aggregation:
    max_stock = max(stocks, key=lambda s: s.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, agg: Aggregation):
    context.resources.redis.put_data(name=agg.date.strftime(r"%Y-%m-%d"), value=str(agg.high))


@op(
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def put_s3_data(context, agg: Aggregation):
    context.resources.s3.put_data("stocks/{date}/{high}".format(date = agg.date, high = agg.high), agg)


@graph
def week_3_pipeline():
    files = get_s3_data()
    stocks = process_data(files)
    put_redis_data(stocks)
    put_s3_data(stocks)


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
    partition_keys=[str(i) for i in range(1, 11)]
)
def docker_config(part_key):
    conf = docker
    conf['ops']['get_s3_data']['config']['s3_key'] = f"prefix/stock_{part_key}.csv"
    return conf


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)

week_3_schedule_local = ScheduleDefinition(
    job=week_3_pipeline_local,
    cron_schedule="*/15 * * * *"
)


@schedule(
    job=week_3_pipeline_docker,
    cron_schedule="0 * * * *"
)
def week_3_schedule_docker():
    return RunRequest(
        run_config=docker_config
    )


@sensor(job=week_3_pipeline_docker, minimum_interval_seconds=30)
def week_3_sensor_docker(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://localstack:4566",
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        conf = docker
        conf['ops']['get_s3_data']['config']['s3_key'] = f"{new_file}"
        yield RunRequest(
            run_key=new_file,
            run_config=conf
        )
