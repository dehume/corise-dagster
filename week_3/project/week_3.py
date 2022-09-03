from typing import List, Optional

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
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
    tags={"kind": "s3"},
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    output = list()
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Take a list of Stonks and return the phattest one.",
)
def process_data(stocks: List[Stock]) -> Optional[Aggregation]:
    if len(stocks) == 0:
        return None
    sorted_stocks = sorted(stocks, key=lambda x: x.high, reverse=True)
    top = sorted_stocks[0]
    agg = Aggregation(date=top.date, high=top.high)
    return agg


@op(
    required_resource_keys={"redis"},
    ins={"aggregation": Out(dagster_type=Aggregation)},
    description="Upload an Aggregation to Redis where the whole world can see it.",
    tags={"kind": "redis"},
)
def put_redis_data(context, aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))


@graph
def week_3_pipeline():
    stocks = get_s3_data()
    processed = process_data(stocks)
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
    # The assignment says set 1-10, but also says they represent month, so shouldn't
    # the keys be 1-12?
    partition_keys=[str(i) for i in range(1,13)]
)
def docker_config(month):
    config = docker.copy()
    config["ops"] = {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{month}.csv"}}},
    return config


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


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")


@sensor
def docker_week_3_sensor(context, prefix):
    new_files = get_s3_keys(bucket="dagster", prefix=prefix)
    if len(new_files) == 0:
        yield SkipReason("Aint found no new files there bub.")
        return
    # Don't want to change the original, so make a copy
    config = docker_config.copy()
    for new_file in new_files:
        config["ops"]["get_s3_data"]["config"]["s3_key"] = new_file
        yield RunRequest(
            run_key=new_file,
            run_config=config,
        )
