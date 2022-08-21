import csv
from datetime import datetime
import heapq
from heapq import nlargest
from typing import List
from operator import attrgetter

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
    ins={"stock_list": In(dagster_type=List[Stock])},
    out=DynamicOut(),
    tags={"kind": "Aggregation"},
    description="Get a list of Aggregation based on the nlarges in config file",
)
def process_data(context,  stock_list):
    aggr_list = []
    numb_Items = context.op_config["nlargest"]
    # One more way to find the  nlargest items in a list
    # largest_stocks_ow = heapq.nlargest(numb_Items, stock_list,
    #                                   key=lambda stock: stock.high)
    largest_stocks = heapq.nlargest(numb_Items, stock_list,
                                    key=sortkey)
    [aggr_list.append(Aggregation(date=stock.date, high=stock.high))
     for stock in largest_stocks]
    print(aggr_list)
    for idx, aggregation in enumerate(aggr_list):
        yield DynamicOutput(aggregation, mapping_key=str(idx))


@op(description="Upload an Aggregation to Redis",
    tags={"kind": "redis"})
def put_redis_data(context, aggregation: Aggregation) -> None:
    print(aggregation)
    pass

# Define a function that returns a comparison key for Stock objects


def sortkey(stock):
    return stock.high


@job
def week_1_pipeline():
    aggregations = process_data(get_s3_data())
    aggr_data = aggregations.map(put_redis_data)
    # Why do we need this step?
    aggr_data.collect()
