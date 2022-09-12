from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    group_name="corise",
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    # out={"stocks": Out(dagster_type=List[Stock], description="List of Stocks")},
)
def get_s3_data(context):
    output = list()
    data = context.resources.s3.get_data(context.op_config["s3_key"])
    for row in data:
        output.append(Stock.from_list(row))
    return output


@asset(
    group_name="corise",
    # ins={"stocks": In(dagster_type=List[Stock])},
    # out={"aggregation": Out(dagster_type=Aggregation)},
    description="Given a list of stocks return the Aggregated values",
)
def process_data(stocks):
    if len(stocks) == 0:
        return None
    highest = max(stocks, key=lambda x: x.high)
    agg = Aggregation(date=highest.date, high=highest.high)
    return agg


@asset(
    group_name="corise",
    # ins={"aggregation": In(dagster_type=Aggregation)},
    # out=Out(dagster_type=Nothing),
    required_resource_keys={"redis"},
    description="upload aggregated data to Redis",
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"redis": redis_resource, "s3": s3_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {"config": {"host": "redis", "port": 6379}},
    },
)
