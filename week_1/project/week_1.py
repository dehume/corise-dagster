# CoRise Dagster Course assignment #1

import csv
from datetime import datetime
from typing import List

from dagster import In, Nothing, Out, job, op, usable_as_dagster_type
from pydantic import BaseModel

from operator import attrgetter

# Create new Type 'Stock'
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

# Create new Type 'Aggregation'
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


@op (
    ins={"StockList": In(dagster_type=List[Stock])},
    out={"Aggregation": Out(dagster_type=Aggregation)},
    description="get highest stock"
)
def process_data(StockList):
    hi_stock : Stock = max(StockList, key=attrgetter("high"))
    stock_agg = Aggregation(date=hi_stock.date, high=hi_stock.high)
    return stock_agg


@op(
    ins={"agg": In(dagster_type=Aggregation)},
    tags={"kind": "redis"},
    description="Save to Redis - pass for now",
)
def put_redis_data(agg: Aggregation):
    pass


@job
def week_1_pipeline():
    s3_fetch = process_data(get_s3_data())
    put_redis_data(s3_fetch)
