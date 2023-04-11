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


@op(config_schema={"s3_key": String})
def get_s3_data_op(context):
    # Pulling S3 file path from config and returning list of stocks 
    s3_key_path = context.op_config["s3_key"]
    stock_list = [stock for stock in csv_helper(file_name=s3_key_path)]
    return stock_list


@op(
    ins={"stocks": In(dagster_type=List, description="List of stocks to be processed")},
    out={"agg": Out(dagster_type=Aggregation, description="Stock with the max high and its date")}
    )
def process_data_op(stocks):
    # Pulling date and high from stock with the max high price   
    max_stock = max(stocks, key=lambda x: x.high)
    agg = Aggregation(date=max_stock.date, high=max_stock.high)    
    return agg


@op(
    ins={"agg": In(dagster_type=Aggregation, description="Aggregated data with maximum stock high and date")},
    out=Out(dagster_type=Nothing)
    )
def put_redis_data_op(agg):
    pass


@op(
    ins={"agg": In(dagster_type=Aggregation, description="Aggregated data with maximum stock high and date")},
    out=Out(dagster_type=Nothing)
    )
def put_s3_data_op(agg):
    pass


@job
def machine_learning_job():
    stock_data = get_s3_data_op()
    max_stock_info = process_data_op(stock_data)
    put_redis_data_op(max_stock_info)
    put_s3_data_op(max_stock_info)