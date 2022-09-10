
from typing import List
from operator import attrgetter
from dagster import Nothing, AssetIn, AssetOut, asset, with_resources, resource, op, pipeline
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    group_name="corise",
    compute_kind="s3",
    # out={"stocks": AssetOut(dagster_type=List[Stock],
    #                        description="List of Stocks")},
    # Can we use AssetOut ?
)
def get_s3_data(context):
    # Use your op logic from week 3
    output = list()
    for csv_row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(csv_row)
        output.append(stock)
    return output


@asset(
    # Looks like AsseTIn is missning dagster_tye in ver 0.15.0 23 days ago somebody merged a PR
    #ins={"stocks": AssetIn(dagster_type=List[Stock])},
    #out={"highest_stock": AssetOut(dagster_type=Aggregation)},
    #description="Given a list of stocks, return an Aggregation with the highest value"
    group_name="corise",
    compute_kind="python",
)
def process_data(get_s3_data):
    # Use your op logic from week 3 (you will need to make a slight change)
    # Use your ops from week 2
    highest_stock = max(get_s3_data, key=attrgetter("high"))
    aggregation = Aggregation(date=highest_stock.date, high=highest_stock.high)
    return aggregation


@asset(
    #ins={"aggregation": AssetIn(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    description="Given a Aggregation, Upload to Redis",
    group_name="corise",
    compute_kind="redis",
)
def put_redis_data(context, process_data):
    # Use your op logic from week 3 (you will need to make a slight change)
    context.resources.redis.put_data(
        str(process_data.date), str(process_data.high))


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

# I feel we can use this way to declare the resource as well


@resource(config_schema={"s3_key": str})
def s3_resource():
    s3_res = {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        }
    }
    return s3_res
