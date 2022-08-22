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


def sortkey(stock):
    return stock.high

@op(
    config_schema={"nlargest": int},
    ins={"stock_list": In(dagster_type=List[Stock])},
    out=DynamicOut(),
    description="Return n highest stocks"
    )
def process_data(context, stock_list:list) -> Aggregation:
    n = context.op_config["nlargest"]
    n_highest_stocks = nlargest(n, stock_list, key=sortkey)
    for i, stock in enumerate(n_highest_stocks):
        yield DynamicOutput(Aggregation(date=stock.date, high=stock.high), mapping_key=str(i))


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    description="Put Aggregation data into Redis"
    )
def put_redis_data(aggregation):
    pass



@job
def week_1_pipeline():
    processed_stocks = process_data(get_s3_data())
    processed_stocks.map(put_redis_data)
