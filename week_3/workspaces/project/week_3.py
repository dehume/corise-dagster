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
    DefaultSensorStatus,
    build_sensor_context,
    SensorEvaluationContext,
    ScheduleEvaluationContext,
    build_schedule_from_partitioned_job
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": str},
    required_resource_keys={"s3"}
)
def get_s3_data(context) -> List[Stock]:
    s3key = context.op_config["s3_key"]
    return [Stock.from_list(st) for st in context.resources.s3.get_data(s3key)]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highestock = max(stocks, key=lambda st: st.high)
    return Aggregation(date=highestock.date, high=highestock.high)


@op(required_resource_keys={"redis"})
def put_redis_data(context, highest: Aggregation) -> Nothing:
    context.resources.redis.put_data(str(highest.date), str(highest.high))


@op(required_resource_keys={"s3"})
def put_s3_data(context, highest: Aggregation) -> Nothing:
    context.resources.s3.put_data(str(highest.date), highest)


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
                "endpoint_url":
                    # "http://localhost:4566/"
                    "http://localstack:4566",
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

PARTITIONS = [str(i) for i in range(1,11)]

@static_partitioned_config(partition_keys=PARTITIONS)
def docker_config(partition_key: str):
    docker_key = docker.copy()
    docker_key['ops'] = {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}}
    return docker_key


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource,
                   "redis": ResourceDefinition.mock_resource()}
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)


week_3_schedule_local = ScheduleDefinition(job=week_3_pipeline_local,
                                           cron_schedule="*/15 * * * *")


@schedule(cron_schedule="0 * * * *",
          job=week_3_pipeline_docker)
def week_3_schedule_docker(context: ScheduleEvaluationContext):
    # following example at https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules
    return RunRequest(
        run_key=context.scheduled_execution_time.strftime("%Y-%m-%d"),
        run_config=docker_config()
    )


@sensor(job=week_3_pipeline_docker,
        minimum_interval_seconds=31,
        )
def week_3_sensor_docker(context: SensorEvaluationContext):
    s3config = docker['resources']['s3']['config']
    new_keys = get_s3_keys(bucket=s3config['bucket'],
                           prefix='prefix',
                           endpoint_url=s3config['endpoint_url'])
                                        #http://localstack:4566')
                            # someone used "http://host.docker.internal:4566"
    key_config = docker.copy()
    if not new_keys:
        yield SkipReason("No new s3 files found in bucket.")

    for key in new_keys:
        key_config["ops"]["get_s3_data"]["config"]["s3_key"] = f"{key}"
        yield RunRequest(
            run_key=key,  # to avoid duplicates
            run_config=key_config
        )
