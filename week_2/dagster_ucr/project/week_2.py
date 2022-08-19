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
    s3_key = context.op_config["s3_key"]
    context.log.info(f's3_key is {s3_key}')
    s3 =context.resources.s3
    context.log.info(f's3 is {s3}') 
    context.log.info(f'list of keys are: {s3.get_keys()}')
    return([Stock.from_list(x) for x in s3.get_data(s3_key)])
 


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"highest_value": Out(dagster_type=Aggregation)},
    tags={"kind": "python"},
    description="Take the list of stocks and determine the Stock with the greatest high value"
)
def process_data(stocks):
    highest_value = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=highest_value.date, high=highest_value.high)


@op(
    ins={"highest_value": In(dagster_type=Aggregation)}
)
def put_redis_data(highest_value):
    pass


@graph
def week_2_pipeline():
    put_redis_data(process_data(get_s3_data()))


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
