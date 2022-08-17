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
    out=DynamicOut(Aggregation),
    description="Take a list of Stonks and return the N phattest ones.",
)
def process_data(context, stocks: List[Stock]):
    sorted_stocks = sorted(stocks, key=lambda x: x.high, reverse=True)
    n = context.op_config["nlargest"]
    top_n = sorted_stocks[:n]
    aggs = [Aggregation(date=a.date, high=a.high) for a in top_n]
    for i, agg in enumerate(aggs):
        yield DynamicOutput(agg, mapping_key=str(i))


@op(
    tags={"kind": "redis"},
    description="Upload an Aggregation to Redis, or at least pretend to.",
)
def put_redis_data(agg: Aggregation) -> Nothing:
    pass


@job
def week_1_pipeline():
    stocks = get_s3_data()
    aggs = process_data(stocks)
    agg_results = aggs.map(put_redis_data)
    # if put_redis_data gave results, we could collect them all with agg_results.collect()
