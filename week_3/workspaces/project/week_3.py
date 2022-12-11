from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.project.sensors import get_s3_keys
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
def week_3_pipeline():
    agg = process_data(get_s3_data())
    put_redis_data(agg)
    put_s3_data(agg)


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

PARTITIONS = [str(i) for i in range(1, 11)]


@static_partitioned_config(partition_keys=PARTITIONS)
def docker_config(partition_key: str):
    cfg = docker.copy()
    cfg["ops"]["get_s3_data"]["config"]["s3_key"] = f"prefix/stock_{partition_key}.csv"
    return cfg


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
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


week_3_schedule_local = ScheduleDefinition(job=week_3_pipeline_local, cron_schedule="*/15 * * * *")


@schedule(job=week_3_pipeline_docker, cron_schedule="0 * * * *")
def week_3_schedule_docker():
    for partition in PARTITIONS:
        yield week_3_pipeline_docker.run_request_for_partition(partition_key=partition, run_key=partition)


# TODO: Not sure best way to store these.
BUCKET = "dagster"
PREFIX = "prefix"
ENDPOINT_URL = "http://localstack:4566"


@sensor(job=week_3_pipeline_docker, minimum_interval_seconds=30)
def week_3_sensor_docker(context: SensorEvaluationContext):
    since_key = context.cursor
    
    new_keys = get_s3_keys(bucket=BUCKET, prefix=PREFIX, endpoint_url=ENDPOINT_URL, since_key=since_key)
    # TODO: How to log in sensor?
    print(f"Found new keys: {new_keys}")

    if not new_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return

    for key in new_keys:
        cfg = docker.copy()
        cfg["ops"]["get_s3_data"]["config"]["s3_key"] = key
        yield RunRequest(run_key=key, run_config=cfg)

    context.update_cursor(new_keys[-1])
