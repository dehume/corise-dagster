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
    get_dagster_logger,
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
def get_s3_data(context) -> List[Stock]:
    stocks = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(stock) for stock in stocks]


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"agg_max": Out(dagster_type=Aggregation)},
    tags={"kind": "transformation"},
    description="Determine the stock with the highest value"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    # Find the stock with the highest value
    max_stock = max(stocks, key=lambda x: x.high)
    # Log the output
    logger = get_dagster_logger()
    logger.info(f"Higest stock is: {max_stock}")
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    ins={"agg_max": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    out=Out(dagster_type=Nothing),
    tags={"kind": "redis"},
    description="Write to Redis"
)
def put_redis_data(context, agg_max: Aggregation) -> None:
    data = context.resources.redis.put_data(str(agg_max.date), str(agg_max.high))
    context.log.info(f"Write {data} to Redis.")


@graph
def week_3_pipeline():
    s3_data = get_s3_data()
    highest_stock = process_data(s3_data)
    put_redis_data(highest_stock)


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


# Create a fixed number of partitions (1-10)
partition_keys = [str(i) for i in range(1, 11)]

@static_partitioned_config(partition_keys=partition_keys)
def docker_config(partition_key: str):
    key = f'prefix/stock_{partition_key}.csv'
    return {
        "resources": {**docker["resources"]},
        "ops": {"get_s3_data": {"config": {"s3_key": key }}}
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
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)

# Schedule for local: Every 15 minutes
local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

# Schedule for docker: Start of every hour
docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")


@sensor(
    job=docker_week_3_pipeline, 
    minimum_interval_seconds=30,
    description="Check S3 bucket for new files every 30 seconds."
)
def docker_week_3_sensor(context):

    # Check for new files in the S3 bucket.
    new_files = get_s3_keys(
        bucket="dagster", 
        prefix="prefix", 
        endpoint_url="http://host.docker.internal:4566"
    )

    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return

    log = get_dagster_logger()
    log.info(f"RunRequest for {new_files}")

    # RunRequest for every new file.
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config={
                **docker, 
                "ops": {"get_s3_data": {"config": {"s3_key": new_file}}}
            }
        )
