from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": String},
    required_resource_keys={"s3"}
)
def get_s3_data(context) -> List[Stock]:
    s3bucket = context.op_config["s3_key"]
    return [Stock.from_list(st) for st in context.resources.s3.get_data(s3bucket)]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highestock = max(stocks, key=lambda st: st.high)
    return Aggregation(date=highestock.date, high=highestock.high)


@op(required_resource_keys={"redis"})
def put_redis_data(context, highest: Aggregation) -> Nothing:
    context.resources.redis.put_data(str(highest.date), str(highest.high))


@op(required_resource_keys={"s3"})
def put_s3_data(context, highest: Aggregation) -> Nothing:
    s3bucket = get_s3_data.config_field.config_type.fields["s3_key"],  # no, because we write over the csv file data
    context.resources.s3.put_data(str(highest.date), highest)


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

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()}
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource}
)
