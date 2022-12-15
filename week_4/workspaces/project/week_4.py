from typing import List

from dagster import Nothing, String, asset, with_resources, IOManager, fs_io_manager
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(required_resource_keys={"s3"},
       config_schema={"s3_key": String},
       group_name="corise")
def get_s3_data(context) -> List[Stock]:
    s3key = context.op_config["s3_key"]
    return [Stock.from_list(st) for st in context.resources.s3.get_data(s3key)]


@asset(group_name="corise")
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    highestock = max(get_s3_data, key=lambda st: st.high)
    return Aggregation(date=highestock.date, high=highestock.high)


@asset(required_resource_keys={"redis"},
       group_name="corise")
def put_redis_data(context, process_data: Aggregation) -> Nothing:
    context.resources.redis.put_data(str(process_data.date),
                                     str(process_data.high))


@asset(required_resource_keys={"s3"},
       group_name="corise")
def put_s3_data(context, process_data: Aggregation) -> Nothing:
    context.resources.s3.put_data(str(process_data.date), process_data)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={"s3": s3_resource,
                   "redis": redis_resource,
                   "io_manager": fs_io_manager},
    resource_config_by_key={
        "s3": {
            "config": {"bucket": "dagster",
                        "access_key": "test",
                        "secret_key": "test",
                        "endpoint_url": "http://localstack:4566"
            },
        },
        "redis": {"config": {"host": "redis", "port": 6379}},
    },
    )
