import csv
from datetime import datetime
from typing import List

from dagster import In, Nothing, Out, job, op, usable_as_dagster_type
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
    ins={"stocks": In(dagster_type=List)},
    out={"agg": Out(dagster_type=Aggregation)},
    description="Determine the Stock with the greatest high value",
)
def process_data(stocks):
    sorted_stocks = sorted(stocks, key=lambda x: x.high, reverse=True) 
    high_day = sorted_stocks[0].date
    high_high = sorted_stocks[0].high
    output = Aggregation(date=high_day, high=high_high)
    return output


@op(
    ins={"agg": In(dagster_type=Aggregation)}
)
def put_redis_data(agg):
    pass


@job
def week_1_pipeline():
    put_redis_data(process_data(get_s3_data()))
