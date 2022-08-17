import csv
from datetime import datetime
from heapq import nlargest
from typing import List

from dagster import (
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    job,
    op,
    usable_as_dagster_type,
)
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
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
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
config_schema={"nlargest": int},
out=DynamicOut(),
description= "Given the list of stocks return the aggregation with the greatest",
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    result = sorted(stocks, key= lambda p: p.high, reverse= True)
    i  = 1
    for stock in result[:context.op_config["nlargest"]]:
        i = i + 1
        yield DynamicOutput(Aggregation(date= stock.date, high = stock.high), mapping_key= str(i))

@op(
description= "Upload an Aggregation to Redis",
tags={"kind": "redis"}
)
def put_redis_data(aggregate: Aggregation)-> Nothing:
    pass


@job
def week_1_pipeline():
    aggregations = process_data(get_s3_data())
    aggregations.map(put_redis_data)
