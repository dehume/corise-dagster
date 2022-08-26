from typing import List
import csv
from datetime import datetime
from dagster import In, Nothing, Out, ResourceDefinition, graph, op, usable_as_dagster_type
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource
from pydantic import BaseModel

@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: list):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float

@op(
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context):
    output = list()
    #key = context.resources.op_config["s3_key"]
    stocks = context.resources.s3.get_data("key_name")
    for row in stocks:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(out={"aggregation": Out(dagster_type=Aggregation, description= "return class with values")})
def process_data(stocks):
    high_val = 0
    date = datetime
    
    for stock in stocks:
        if stock.high > high_val:
            high_val = stock.high
            date = stock.date
    
    return Aggregation(date=date, high=high_val)


#@op(ins={"aggregation": In(dagster_type=Aggregation, description="Aggregated Stocks Values")})
@op(
    #config_schema={"host": str, "port": int},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(aggregation.date, aggregation.high)


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
