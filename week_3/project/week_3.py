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
    out={"stocks":Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Accesses S3 to pull a list of Stocks"
)
def get_s3_data(context):
    output = list()
    
    for row in context.resources.s3.get_data(key_name=context.op_config["s3_key"]): 
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Takes list of stocks and outputs one with highest 'high' value"
)
def process_data(stocks):
    stock_choice = max(stocks, key=lambda x: x.high)
    logger = get_dagster_logger()
    logger.info(f'Picked top stock: {stock_choice}')
    return Aggregation(date=stock_choice.date, high=stock_choice.high)


@op(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    tags={"kind": "redis"},
    description="Upload aggregations to Redis",
)
def put_redis_data(context, aggregation) -> Nothing:
    logger = get_dagster_logger()
    logger.info(f'Writing the follow record to Redis: {aggregation}')
    context.resources.redis.put_data(name=str(aggregation.date), value=str(aggregation.high))


@graph
def week_3_pipeline():
    stock_list = get_s3_data()
    stock_choice = process_data(stock_list)
    put_redis_data(stock_choice)


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

@static_partitioned_config(partition_keys=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])
def docker_config(partition_key: str):
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
    op_retry_policy=RetryPolicy(max_retries=10, delay=1.0),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")


@sensor(
    job=docker_week_3_pipeline,
    minimum_interval_seconds=30
)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket='dagster',
        prefix="prefix",
        endpoint_url="http://host.docker.internal:4566"
    )
    logger = get_dagster_logger()
    logger.info(f'New files found: {new_files}')

    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
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
                "ops": {
                    "get_s3_data": {
                        "config": {
                            "s3_key": new_file
                        }
                    }
                },
            }
        )