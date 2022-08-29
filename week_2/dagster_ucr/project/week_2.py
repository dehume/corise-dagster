from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock, Stocks
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource

@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=Stocks)},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    with open(context.op_config["s3_key"]) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock.from_list(row)
            output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=Stocks)},
    out={"Aggregation": Out(dagster_type=Aggregation)},
    description="Find stock with highest value according to the 'high' field",
)
def process_data(stocks: Stocks) -> Aggregation:
    highest = sorted(stocks, key=lambda stock: stock.high, reverse=True)[0]
    aggregation = Aggregation(date=highest.date, high=highest.high)
    return aggregation


@op
def put_redis_data(aggregation: Aggregation) -> None:
    pass


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    put_redis_data(aggregation)


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
