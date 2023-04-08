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


@op(config_schema={"s3_key": str}, out=Out())
def get_s3_data_op(context) -> List[Stock]:
    #pull s3_key from op config
    s3_key = context.op_config["s3_key"]
    
    #read data from csv to simulate s3
    stock_list = []
    for stock in csv_helper(s3_key):
        stock_list.append(stock)
    return stock_list


@op(ins={"stock_data": In(dagster_type=List[Stock])}, out=Out(dagster_type=Aggregation))
def process_data_op(context, stock_data) -> Aggregation:
    #find the max high stock value
    max_stock = max(stock_data, key=lambda stock: stock.high)
    #return the list of aggregation
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(ins={"aggr": In(dagster_type=Aggregation)}, out=Out())
def put_redis_data_op(context, aggr: Aggregation):
    pass


@op(ins={"aggr": In(dagster_type=Aggregation)}, out=Out())
def put_s3_data_op(context, aggr: Aggregation):
    pass


@job
def machine_learning_job():
    stock_data = get_s3_data_op()
    max_high = process_data_op(stock_data)
    put_redis_data_op(max_high)
    put_s3_data_op(max_high)
