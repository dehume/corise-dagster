import csv
from typing import List

from dagster import Out, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: str
    close: float
    volume: int
    open: float
    high: float
    low: float


class Aggregation:
    pass


@op(out={"stocks": Out(dagster_type=List[Stock], description="List of Stocks")})
def get_s3_data():
    output = list()
    with open("../data/stock.csv") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock(
                date=row[0],
                close=float(row[1]),
                volume=int(float(row[2])),
                open=float(row[3]),
                high=float(row[4]),
                low=float(row[5]),
            )
            output.append(stock)
    return output


@op
def process_data():
    pass


@op
def put_redis_data():
    pass


@job
def week_1_pipeline():
    pass
