import csv
from datetime import datetime
from random import randrange
from typing import List

from dagster import (
    DynamicOut,
    DynamicOutput,
    In,
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


def generate_mapping_key(datetime) -> str:
    return datetime.strftime("%m%d%Y%H%M%S") + str(randrange(100))
    
    
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
    tags={"kind": "pythong"},
    description="Take the list of stocks and determine  \
        the Stock with the nlargest value"
)
def process_data(context, stocks):
    no_stocks = len(stocks)
    nlargest = min(no_stocks, context.op_config["nlargest"])
    context.log.info(f"nlargest is {nlargest}")
    nlargest_values = sorted(stocks, key=lambda stock: stock.high)[0:nlargest]
    for stock in nlargest_values:
        yield DynamicOutput(stock, mapping_key=generate_mapping_key(stock.date))
        
@op(
    ins={"stock": In(dagster_type=Stock)},
    out={"aggregate": Out(dagster_type=Aggregation)},
    tags={"kind": "python"},
    description="Construct appregation"
)
def aggregate(stock: Stock) -> Aggregation:
    return Aggregation(date=stock.date, high=stock.high)


@op
def put_redis_data(nlargest):
    pass


@job
def week_1_pipeline():
    stocks = process_data(get_s3_data())
    nlargest = stocks.map(aggregate)
    put_redis_data(nlargest.collect())
