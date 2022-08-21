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


@op(
    config_schema={"nlargest": int},
    ins={"stocks": In(dagster_type=List[Stock])},
    out=DynamicOut(),
    description="Aggregate stock data",
)
def process_data(context,stocks: List[Stock]) -> Aggregation:
    stocks_sorted = sorted(stocks, key=lambda x: x.high, reverse=True)
    for i in range(context.op_config["nlargest"]):
        record = Aggregation(date=stocks_sorted[i].date, high=stocks_sorted[i].high)   
        yield DynamicOutput(value=record,mapping_key=f"nlargest_{str(i+1)}")
  


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(Nothing),
    tags={"kind": "redis"},
    description="Upload aggregation to Redis",
)
def put_redis_data(aggregation: Aggregation):
    log = get_dagster_logger()
    log.info(aggregation)
    pass



@job
def week_1_pipeline():
    stocks = get_s3_data()
    process_data(stocks).map(put_redis_data)
