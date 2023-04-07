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


@op(
    config_schema = {"s3_key": str},
    out = {"stocks": Out(dagster_type = List[Stock],
    description = "Get a list of stock data from s3_key")}
)

    #The information we are starting to work with in our pipeline for now comes
    #from a csv file, we will extract it using s3_key context and csv_helper function.
    #The input s3_key is a string, meanwhile the output we will get is a list of stock.


def get_s3_data_op(context):

    file_name = context.op_config["s3_key"]

    return list(csv_helper(file_name))


@op(description = "Process of data in order to get the date that contains the higher stock value",
    ins = {"stocks": In(dagster_type = List[Stock],
            description = "Stocks")},
    out = {"higher_value_data": Out(dagster_type = Aggregation,
            description = "Two fields, Date that contains the higher value and the higher value")}
)

    #Given a list of stocks from different dates, we will get the date that has
    #the higher stock value.
    #The input we will gave to the op is a list, and we want the result as the Aggregation format.


def process_data_op(context, stocks):
    
    higher = max(stocks, key = lambda x: x.high)

    return Aggregation(date = higher.date, high= higher.high)


    #We will use lambda in order to get the higher value and its corresponding date.

@op(
    description = "Upload data into Redis",
    ins = {"higher_value_data": In(dagster_type = Aggregation)}
)
def put_redis_data_op(context, higher_value_data):
    pass


@op(
    description = "Upload data into s3",
    ins = {"higher_value_data": In(dagster_type = Aggregation)}
)
def put_s3_data_op(context, higher_value_data):
    pass


@job
def machine_learning_job():
    higher_value_data = process_data_op(get_s3_data_op())
    put_redis_data_op(higher_value_data)
    put_s3_data_op(higher_value_data)
