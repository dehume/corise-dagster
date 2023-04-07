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


@op(
    config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List[Stock], description="Get a list off stocks.")},
    tags={"kind": "s3"},
    description="Get stock data from s3 mock file.",
)
def get_s3_data_op(context):
    s3_key = context.op_config["s3_key"]
    stocks = list(csv_helper(s3_key))
    return stocks


@op(
    ins={"stocks": In(dagster_type=List[Stock], description="Output of get s3 data.")},
    out={"aggregation": Out(dagster_type=Aggregation, description="Highest stock.")},
    description="Filter and return highest stock (mock aggregation).",
)
def process_data_op(context, stocks):
    high_val = max((s.high for s in stocks))
    high_stock = list(filter(lambda stock: stock.high == high_val, stocks))[0]
    aggregation = Aggregation(date=high_stock.date, high=high_stock.high)
    return aggregation


@op(
    ins={"aggregation": In(dagster_type=Aggregation, description="Output of process data.")},
    tags={"kind": "redis"},
    description="Upload aggregation to redis.",
)
def put_redis_data_op(context, aggregation) -> Nothing:
    pass


@op(
    ins={"aggregation": In(dagster_type=Aggregation, description="Output of process data.")},
    tags={"kind": "s3"},
    description="Upload aggregation to s3.",
)
def put_s3_data_op(context, aggregation) -> Nothing:
    pass


@job
def machine_learning_job():
    s3_data = get_s3_data_op()
    aggregation = process_data_op(s3_data)
    put_redis_data_op(aggregation)
    put_s3_data_op(aggregation)


# Dagit config
#  ops:
#   get_s3_data_op:
#     config:
#       s3_key: week_1/data/stock.csv
