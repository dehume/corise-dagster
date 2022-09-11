from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op, get_dagster_logger
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


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
def week_2_pipeline():
    stock_list = get_s3_data()
    stock_choice = process_data(stock_list)
    put_redis_data(stock_choice)


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
