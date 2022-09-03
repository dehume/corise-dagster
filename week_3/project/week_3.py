from datetime import datetime as dt
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
    get_dagster_logger,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock

logger = get_dagster_logger()

@op(required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    data = context.resources.s3.get_data(context.op_config["s3_key"])
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Aggregate stock data",
)
def process_data(stocks: List[Stock]) -> Aggregation:
    top_high = sorted(stocks, key=lambda x: x.high, reverse=True)[0]
    return Aggregation(date=top_high.date, high=top_high.high)


@op(required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(Nothing),
    tags={"kind": "redis"},
    description="Upload aggregation to Redis",
)
def put_redis_data(context,aggregation: Aggregation):
    agg_date = dt.strftime(aggregation.date, '%m/%d/%Y')
    agg_high = str(aggregation.high)
    context.resources.redis.put_data(agg_date, agg_high)
    logger.info(f"Date: {agg_date} with daily high of ${agg_high} stored on Redis")



@graph
def week_3_pipeline():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    put_redis_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

def get_run_config(s3_key: str):
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
    "ops": {"get_s3_data": {"config": {"s3_key": s3_key}}},
}


docker = get_run_config("prefix/stock_9.csv") 

@static_partitioned_config(partition_keys=[str(num) for num in range(1, 11)])
def docker_config(partition_key: str):
    return {
        **docker,
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
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)

#This schedule should run the local_week_3_pipeline every 15 minutes.
local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

#This schedule should run the docker_week_3_pipeline at the beginning of every hour.
docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")

@sensor(job=docker_week_3_pipeline)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix"
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config=get_run_config(new_file)
        )