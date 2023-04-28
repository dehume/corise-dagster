from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
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
    String
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock



@op(required_resource_keys={"s3"},
    config_schema={"s3_key": String},
    description="Meant to read stock data from the S3 client")
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    s3_key = context.op_config['s3_key']
    """s3 being the instance of the s3 resource defined in resources.py on which we call the get_data method of the class"""
    stocks = [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_key)]
    return stocks 


@op(description="Digest the list of stock data and return the stock with the maximum high value")
def process_data(context: OpExecutionContext, stocks: List[Stock]) -> Aggregation:
    max_high_value_stock = max(stocks, key=lambda s: s.high)
    aggregation = Aggregation(date=max_high_value_stock.date, high=max_high_value_stock.high)
    return aggregation


@op( required_resource_keys={"redis"},
     description="Meant to upload/write the processed data from aggregation to Redis client")
    
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation) :
    redis_data = context.resources.redis.put_data(aggregation.date.strftime("%Y%m%d"), str(aggregation.high))
    # redis_data = context.resources.redis.put_data(aggregation)

@op( required_resource_keys={"s3"},
     description="Meant to upload/write the processed data from aggregation to S3 client")

def put_s3_data(context: OpExecutionContext, aggregation: Aggregation) :
    s3_data = context.resources.s3.put_data(aggregation.date.strftime("%Y%m%d"), aggregation)


@graph
def machine_learning_graph():
    pass


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=str(range(0,10)))
def docker_config(partition_key):
    return {
        "resources": {
            "s3": {"config": S3},
            "redis": {"config": REDIS},
        },
        "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_{partition_key}.csv"}}},
}


""" Only reading from a mock s3 source and no reading from redis """
machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={'s3': mock_s3_resource, 'redis': ResourceDefinition.mock_resource()}
)

""" Using the resources we created to write to both s3 and redis 
and adding a retry policy for pushing data"""

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker_config,
    resource_defs={'s3': s3_resource, 'redis': redis_resource},
    op_retry_policy=RetryPolicy(max_entries=10, delay=1)
)


machine_learning_schedule_local = ScheduleDefinition(cron_schedule="*/15 * * * *", job=machine_learning_job_local)


@schedule(cron_schedule="0 * * * *", job=machine_learning_job_docker)
def machine_learning_schedule_docker():
    for partition_key in docker_config.get_partition_keys():
        yield RunRequest(run_key=partition_key, run_config=docker_config.get_run_config(partition_key))


@sensor(job=machine_learning_job_docker)
def machine_learning_sensor_docker():
    s3_keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://localstack:4566")
    if s3_keys:
        for key in s3_keys:
            yield RunRequest(run_key=key, run_config={"resources": {
                                                        "s3": {"config": S3},
                                                        "redis": {"config": REDIS},
                                                         },
                                                    "ops": {"get_s3_data": {"config": {"s3_key": key}}},} 
                                            )
    else:
         yield SkipReason("No new s3 files found in bucket")