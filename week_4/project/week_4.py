from typing import List
from operator import attrgetter


from dagster import Nothing, asset, with_resources, OpExecutionContext
from project.resources import redis_resource, s3_resource, S3, Redis
from project.types import Aggregation, Stock


@asset(
    group_name="corise",
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    op_tags={"kind": "s3"},
    description="List of Stocks",
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    s3: S3 = context.resources.s3  # assinged to a variable for better typing support
    data = s3.get_data(key_name=context.op_config["s3_key"])

    output = [Stock.from_list(row) for row in data]
    return output


@asset(group_name="corise", description="keeps the Stock with the greatest `high` value")
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    highest_stock: Stock = max(get_s3_data, key=attrgetter("high"))
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@asset(
    group_name="corise",
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    description="persists the results to redis",
)
def put_redis_data(context: OpExecutionContext, process_data: Aggregation) -> Nothing:
    # Use your op logic from week 3 (you will need to make a slight change)
    redis: Redis = context.resources.redis  # assinged to a variable for better typing support
    redis.put_data(name=process_data.date.isoformat(), value=str(process_data.high))


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
        },
    },
)
