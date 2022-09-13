from typing import List
from datetime import datetime

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
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    s3_key = context.op_config["s3_key"]
    for row in context.resources.s3.get_data(s3_key):
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Get the date and high of the highest high",
)
def process_data(stock_list):
    highest: float = 0 # stocks cannot be lower than 0
    date_of_highest: datetime = None 
    for stock in stock_list:
      if stock.high > highest:
        highest = stock.high
        date_of_highest = stock.date
    return Aggregation(date=date_of_highest, high=highest)

@op(
    required_resource_keys={"redis"},
    description="Upload an aggregation to Redit",
)
def put_redis_data(context, agg):
    name = agg.date.strftime("%Y-%m-%d")
    value = str(agg.high)
    context.resources.redis.put_data(name, value)


@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

docker_base_config = {
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


def docker_config_sensed(s3_key: str = None):
    docker_reconfig = docker_base_config.copy()
    docker_reconfig["ops"]["get_s3_data"]["config"]["s3_key"] = s3_key
    return docker_reconfig

@static_partitioned_config(partition_keys=["1", "2", "3", "4","5","6","7","8","9","10"])
def docker_config(partition_key: str):
    docker_reconfig = docker_base_config.copy()
    s3_key = f"prefix/stock_{partition_key}.csv"
    docker_reconfig["ops"]["get_s3_data"]["config"]["s3_key"] = s3_key
    return docker_reconfig


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
    config=docker_base_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)

local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")

@sensor(
    job=docker_week_3_pipeline, 
    minimum_interval_seconds=30
)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://host.docker.internal:4566")
    if not new_files:
      yield SkipReason("No new s3 files found in bucket.")
      return
    for new_file in new_files:
      yield RunRequest(
          run_key=new_file,
          run_config=docker_config_sensed(new_file)
      )
