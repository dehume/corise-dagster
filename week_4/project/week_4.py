from typing import List, Optional

from dagster import In, Nothing, OpExecutionContext, Out, asset, graph, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    group_name="corise",
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    dagster_type=List[Stock],
    op_tags={"kind": "s3"},
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    output = list()
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    group_name="corise",
    dagster_type=Aggregation,
    description="Take a list of Stonks and return the phattest one.",
)
def process_data(get_s3_data) -> Optional[Aggregation]:
    if len(get_s3_data) == 0:
        return None
    top = max(get_s3_data, key=lambda x: x.high)
    agg = Aggregation(date=top.date, high=top.high)
    return agg


@asset(
    group_name="corise",
    required_resource_keys={"redis"},
    description="Upload an Aggregation to Redis where the whole world can see it.",
    op_tags={"kind": "redis"},
)
def put_redis_data(context, process_data) -> Nothing:
    if process_data is not None:
        context.resources.redis.put_data(str(process_data.date), str(process_data.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
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
        }
    }
)
