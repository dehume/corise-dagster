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
    get_dagster_logger
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


@op(out=DynamicOut(), config_schema={"n": int})
def process_data(context, raw_stocks: List[Stock]) -> DynamicOutput:
    n = context.op_config["n"]
    top_stocks = nlargest(n, raw_stocks, key=lambda x: x.high)
    aggregations = [Aggregation(date=stock.date, high=stock.high) for stock in top_stocks]
    get_dagster_logger().info(aggregations)
    for idx, stock in enumerate(aggregations):
        yield DynamicOutput(stock, mapping_key=str(idx+1))


@op
def put_redis_data(aggregation=DynamicOut):
    get_dagster_logger().info(f"Put Aggregation {aggregation} in redis")


@job
def week_1_pipeline():
    aggregates = process_data(get_s3_data())
    aggregates.map(put_redis_data)


week_1_pipeline.execute_in_process(
    run_config={'ops': {'get_s3_data': {'config': {'s3_key': "week_1/data/stock.csv"}}},
                {'process_data'}: {'config': {'n': 3}}}
)
