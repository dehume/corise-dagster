import csv
import logging
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


@op
def process_data(raw_stocks: List[Stock]) -> Aggregation:
    highest_stock = max(raw_stocks, key=lambda x: x.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op
def put_redis_data(aggregation: Aggregation):
    logging.info(f"Put {aggregation} in redis")
    pass


@job()
def week_1_pipeline():
    put_redis_data(process_data(get_s3_data()))


week_1_pipeline.execute_in_process(
    run_config={'ops': {'get_s3_data': {'config': {'s3_key': "week_1/data/stock.csv"}}}}
)
