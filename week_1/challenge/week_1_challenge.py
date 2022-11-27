import csv
from datetime import datetime
from heapq import nlargest
from random import randint
from typing import Iterator, List, Union
import hashlib


from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    Output,
    String,
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
    def from_list(cls, input_list: List[List]):
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


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    description="Get a list of stocks from an S3 file",
    config_schema={"s3_key": String},
    out={
        "stocks": Out(dagster_type=List[Stock], description="List of Stock objects in the data file.", is_required=False),
        "empty_stocks": Out(dagster_type=Nothing, description="List of Stock objects in the data file.", is_required=False)
        },
    tags={"kind": "s3"}
)
def get_s3_data(context) -> Union[list[Stock], Nothing]:
    stocks = list(csv_helper(context.op_config["s3_key"]))
    if stocks:
        yield Output(stocks, "stocks")
    else:
        yield Output(None, "empty_stocks")


@op(
    description="Given a list of stocks return the top x Aggregations with the greatest high value",
    config_schema={"nlargest": int},
    ins={
        "stocks": In(dagster_type=List[Stock], description="List of Stock objects in the data file.")
        },
    out=DynamicOut()
)
def process_data(context, stocks: list[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key= lambda stock: stock.high)
    greatest_stock = sorted_stocks[-context.op_config["nlargest"]:]

    for stock in greatest_stock:
        yield DynamicOutput(Aggregation(date=stock.date, high=stock.high), mapping_key=f"{randint(1,1000000)}")


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    description="Notifiy if stock list is empty",
)
def empty_stock_notify(context, empty_stocks) -> Nothing:
    context.log.info("No stocks returned")


@op(
    description="Upload an Aggregation to Redis",
    ins={"agg": In(dagster_type=Aggregation)},
    tags={"kind": "redis"}
    )
def put_redis_data(context, agg: Aggregation) -> Nothing:
    pass


@job
def week_1_challenge():
    stocks, empty_stock = get_s3_data()
    empty_stock_notify(empty_stock)
    process = process_data(stocks)
    process.map(put_redis_data)

