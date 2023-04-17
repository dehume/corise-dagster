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


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"})
def get_s3_data(context: OpExecutionContext):
    # Pulling S3 file path from config and returning list of stocks 
    s3_key = context.op_config["s3_key"]
    s3_resource_info = context.resources.s3
    raw_data = s3_resource_info.get_data(key_name=s3_key)
    stock_list = []
    for stock in raw_data:
        stock_list.append(Stock.from_list(stock))
    return stock_list



@op(
    ins={"stocks": In(dagster_type=List, description="List of stocks to be processed")},
    out={"agg": Out(dagster_type=Aggregation, description="Stock with the max high and its date")}
    )
def process_data(stocks):
    # Pulling date and high from stock with the max high price   
    max_stock = max(stocks, key=lambda x: x.high)
    agg = Aggregation(date=max_stock.date, high=max_stock.high)    
    return agg


@op(
    ins={"agg": In(dagster_type=Aggregation, description="Aggregated data with maximum stock high and date")},
    required_resource_keys={"redis"},
    out=Out(dagster_type=Nothing)
    )
def put_redis_data(context, agg):
    # Getting date and high price info
    date_of_max = str(agg.date)
    high_of_max = str(agg.high)
    # Creating a redis client and writing to redis
    redis_client = context.resources.redis
    redis_client.put_data(name=date_of_max, value=high_of_max)


@op(
    ins={"agg": In(dagster_type=Aggregation, description="Aggregated data with maximum stock high and date")},
    required_resource_keys={"s3"},
    out=Out(dagster_type=Nothing)
    )
def put_s3_data(context, agg):
    # Getting date and high price info
    date_of_max = str(agg.date)
    high_of_max = str(agg.high)
    # Creating an S3 client and writing to S3
    s3_client = context.resources.s3
    s3_client.put_data(name=date_of_max, value=high_of_max)


@graph
def machine_learning_graph():
    stock_data = get_s3_data()
    max_stock_info = process_data(stock_data)
    put_redis_data(max_stock_info)
    put_s3_data(max_stock_info)
    pass 

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

# Job that does not use Docker
machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()}  
)

# Job that uses Docker
machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource}
)
