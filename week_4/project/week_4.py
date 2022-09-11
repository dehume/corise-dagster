from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={'s3'},
    description="List of Stocks",
    group_name="corise"
)
def get_s3_data(context):
    stocklist = list()
    s3_key = context.op_config["s3_key"]
    for row in context.resources.s3.get_data(s3_key):
        stock = Stock.from_list(row)
        stocklist.append(stock)
    return stocklist


@asset(
    description="Return Aggregation from stock list with the greatest `high` value",
    group_name='corise'
)
def process_data(get_s3_data):
    aggregation = max(get_s3_data, key=lambda stock: stock.high)
    return Aggregation(date=aggregation.date, high=aggregation.high)


@asset(
    description="Upload to Redis",
    required_resource_keys={"redis"},
    group_name="corise"
)
def put_redis_data(context, process_data) -> Nothing:
    context.resources.redis.put_data("agg_data", str(process_data))

get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster", "access_key": "test", "secret_key": "test", "endpoint_url": "http://host.docker.internal:4566"
            }
        },
        "redis": {
            "config": {
                "host": "redis", "port": 6379
            }
        }
    }
)