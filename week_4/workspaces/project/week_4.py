from typing import List

from dagster import Nothing, String, asset, with_resources, AssetIn
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock

@asset(
    group_name="corise",
    config_schema={
        "s3_key": String
    },
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"}
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(item) for item in context.resources.s3.get_data(s3_key)]


@asset(
    group_name="corise",
    ins={"upstream": AssetIn("get_s3_data")}
)
def process_data(upstream) -> Aggregation:
    highest_stock = max(upstream, key= lambda stock: stock.high)
    return Aggregation(date=highest_stock.date,high=highest_stock.high)


@asset(
    group_name="corise",
    ins={"upstream": AssetIn("process_data")},
    required_resource_keys= {"redis"},
    op_tags={"kind": "redis"}

)
def put_redis_data(context, upstream) -> Nothing:
    context.resources.redis.put_data(name=str(upstream.date), value=str(upstream.high))


@asset(
    group_name="corise",
    ins={"upstream": AssetIn("process_data")},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"}
)
def put_s3_data(context, upstream) -> Nothing:
    context.resources.s3.put_data(key_name=str(upstream.date), data=upstream)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    },
    resource_config_by_key={
        "s3":{
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
