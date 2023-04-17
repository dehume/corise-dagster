from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(required_resource_keys={"s3"},
    config_schema={"s3_key": String},
    description="Meant to read stock data from the S3 client")
# @solid(input_defs=[InputDefinition("s3_keys", List[String])])
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config['s3_key']
    """s3 being the instance of the s3 resource defined in resources.py on which we call the get_data method of the class"""
    # s3 = context.resources
    # stocks = []
    stocks = [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_key)]
    # for stock in context.resources.s3.get_data(s3_key):
    #     stocks.append(stock)
    return stocks 


@op(description="Digest the list of stock data and return the stock with the maximum high value")
def process_data(context, stocks: List[Stock]) -> Aggregation:
    max_high_value_stock = max(stocks, key=lambda s: s.high)
    aggregation = Aggregation(date=max_high_value_stock.date, high=max_high_value_stock.high)
    return aggregation


@op( required_resource_keys={"redis"},
     description="Meant to upload/write the processed data from aggregation to Redis client")
    
def put_redis_data(context, aggregation: Aggregation) :
    redis_data = context.resources.redis.put_data(aggregation.date.strftime("%Y%m%d"), str(aggregation.high))
    # redis_data = context.resources.redis.put_data(aggregation)

@op( required_resource_keys={"s3"},
     description="Meant to upload/write the processed data from aggregation to S3 client")

def put_s3_data(context, aggregation: Aggregation) :
    s3_data = context.resources.s3.put_data(aggregation.date.strftime("%Y%m%d"), aggregation)


@graph
def machine_learning_graph():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

""" Only reading from a mock s3 source and no reading from redis """
machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={'s3': mock_s3_resource, 'redis': ResourceDefinition.mock_resource()}
)

""" Using the resources we created to write to both s3 and redis """
machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={'s3': s3_resource, 'redis': redis_resource}
)
