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
    def from_list(cls, input_list: Iterator[str]):
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


# This config schema will take in one parameter, a string name s3_key. The output of the op is a list of Stock.
@op(
    config_schema={"s3_key": String},
    out={"s3_data": Out(dagster_type=List[Stock], description="Get a list off stocks.")},
)
def get_s3_data_op(context):
    s3_key = context.op_config["s3_key"]
    stocks = list(csv_helper(s3_key))
    return stocks


@op(ins={"s3_data": In(dagster_type=List[Stock], description="")})
def process_data_op():
    pass


@op
def put_redis_data_op():
    pass


@op
def put_s3_data_op():
    pass


@job
# @job(config={"ops": {"get_s3_data_op": {"config": {"s3_key": "week_1/data/stock.csv"}}}})
def machine_learning_job():
    get_s3_data_op()


# Dagit
#     ops:
#   get_s3_data_op:
#     config:
#       s3_key: week_1/data/stock.csv
