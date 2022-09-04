
from .sensors import get_s3_keys

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
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context):
    output = list()
    #key = context.resources.op_config["s3_key"]
    stocks = context.resources.s3.get_data("key_name")
    for row in stocks:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(out={"aggregation": Out(dagster_type=Aggregation, description= "return class with values")})
def process_data(stocks):
    high_val = 0
    date = datetime
    
    for stock in stocks:
        if stock.high > high_val:
            high_val = stock.high
            date = stock.date
    
    return Aggregation(date=date, high=high_val)



@op(out={"aggregation": Out(dagster_type=Aggregation, description= "return class with values")})
def process_data(stocks):
    high_val = 0
    date = datetime
    
    for stock in stocks:
        if stock.high > high_val:
            high_val = stock.high
            date = stock.date
    
    return Aggregation(date=date, high=high_val)

@op(
    #config_schema={"host": str, "port": int},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(aggregation.date, aggregation.high)


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
    "ops":  { 
        "get_s3_data": {"config": {"s3_key": 'prefix/{partition_key}'}},
    }
  }


@static_partitioned_config(partition_keys=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])
def docker_config(partition_key: str):
    return {
            'resources' : {
             'redis': {'config': 
             {'host': 'redis', 'port': 6379
              }
            }, 
          's3': {'config': 
              {
                  'access_key': 'test', 
                  'bucket': 'dagster', 
                  'endpoint_url': 'http://host.docker.internal:4566', 
                  'secret_key': 'test'
              }
            },
         },
        'ops': {
            "get_s3_data": {
                "config": {
                    "s3_key": partition_key
                    }
                }
            },
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


@sensor(job=week_3_pipeline, minimum_interval_seconds=30)
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
            run_config= docker_config(new_file)
        )


