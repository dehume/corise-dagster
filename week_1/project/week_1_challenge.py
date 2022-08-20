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
    ins={"stocks": In(dagster_type=List[Stock])},
    out=DynamicOut(Aggregation),
    description="Takes a list of stocks and outputs X of the highest value ones"
)
def process_data(context, stocks):
    top_n_criteria = context.op_config["nlargest"]
    context.log.info(f'Number of stocks to pick is {top_n_criteria}')
    
    if (top_n_criteria > len(stocks)):
        raise Exception('Value nlargest input is larger than the list of stocks available')

    highest_value_stock_list = sorted(stocks, key=lambda x: x.high, reverse=True)[:top_n_criteria]
    context.log.info(f'Picked top {top_n_criteria} stocks: {highest_value_stock_list}')
    for stock in highest_value_stock_list:
        yield DynamicOutput(Aggregation(date = stock.date, high = stock.high), mapping_key=str(stock.volume))


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    tags={"kind": "redis"},
    description="Upload an aggregation to redis"
)
def put_redis_data(context, aggregation):
    context.log.info(f'put_redis_data received the following aggregation: {aggregation}')
    pass


@job
def week_1_pipeline():
    stock_list = get_s3_data()
    n_highest_value_stocks = process_data(stock_list)
    n_highest_value_stocks.map(put_redis_data)
