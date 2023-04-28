import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
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
    def from_list(cls, input_list: List[str]):
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


@op(description="Read stock data from s3 and return them as a list-mimicking reading from data lake", config_schema={"s3_key": String})
def get_s3_data_op(context) -> List[Stock]:
    s3_key = context.op_config['s3_key']
    stocks = list(csv_helper(s3_key))
    return stocks 


@op(description="Digest the list og stock data and return an aggregation of the stock with the mac high value")
def process_data_op(context, stocks: List[Stock]) -> Aggregation:
    max_high_value_stock = max(stocks, key=lambda s: s.high)
    aggregation = Aggregation(date=max_high_value_stock.date, high=max_high_value_stock.high)
    return aggregation


@op(description="Meant to upload/write the processed data from aggregation to Redis")
def put_redis_data_op(context, aggregation: Aggregation):
    pass


@op(description="Meant to upload/write the processed data from aggregation to S3")
def put_s3_data_op(context, aggregation: Aggregation):
    pass

@job
def machine_learning_job():
    stocks = get_s3_data_op()
    aggregation = process_data_op(stocks)
    put_redis_data_op(aggregation)
    put_s3_data_op(aggregation)
