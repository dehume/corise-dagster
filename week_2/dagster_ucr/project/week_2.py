from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    key = context.op_config["s3_key"]
    data = context.resources.s3.get_data(key)

    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    out={"max_value": Out(dagster_type=Aggregation)},
    description="Filter for largest daily high stock value"
)
def process_data(stocks):
    #get max of daily highest stock value
    stock_high = max(stocks, key = lambda x: x.high)
    return Aggregation(date = stock_high.date, high = stock_high.high)


@op(
     ins={"stock_high": In(dagster_type=Aggregation)},
     required_resource_keys={"redis"},
     tags={"kind": "Redis"}
 )
def put_redis_data(context, stock_high):
    context.resources.redis.put_data(
        name = str(stock_high.date), 
        value = str(stock_high.high)
        )
    


@graph
def week_2_pipeline():
    stock_data = get_s3_data()
    agg_data = process_data(stock_data)
    put_redis_data(agg_data)



local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
