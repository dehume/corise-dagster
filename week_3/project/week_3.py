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
    out={"stocks": Out(dagster_type=List[Stock], description="List of Stocks")},
)
def get_s3_data(context):
    output = list()
    data = context.resources.s3.get_data(context.op_config['s3_key'])
    for row in data:
        output.append(Stock.from_list(row))
    return output


@op(ins={"stocks": In(dagster_type=List[Stock])},
    out={'aggregation': Out(dagster_type=Aggregation)},
    description="Given a list of stocks return the Aggregated values")
def process_data(stocks):
    highest = max([stock.high for stock in stocks])
    for stock in stocks:
        if stock.high == highest:
            agg = Aggregation(date=stock.date, high=highest)
    return agg


@op(ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    required_resource_keys={"redis"},
    description='upload aggregated data to Redis')
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))


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

@static_partitioned_config(partition_keys=[ str(i) for i in range(1, 11) ])
def docker_config(partition_key: str):
    return {'ops': {'get_s3_data': {'config': {'s3_key': 'prefix/stock_1.csv'}}},
            'resources': {'redis': {'config': {'host': 'redis',
                                               'port': 6379}
                                    },
                          's3': {'config': {'access_key': 'test',
                                            'bucket': 'dagster',
                                            'endpoint_url': 'http://localstack:4566',
                                            'secret_key': 'test'}
                              }
                       }
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
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")

@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=1)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix"
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config={
                'resources': {'s3': {'config': {'bucket': 'dagster',
                                                 'access_key': 'test',
                                                 'secret_key': 'test',
                                                 'endpoint_url': 'http://localstack:4566'}
                                      },
                               'redis': {'config': {'host': 'redis',
                                                    'port': 6379}}},
                 'ops': {'get_s3_data': {'config': {'s3_key': new_file}}}}
        )