import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
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
    def from_list(cls, input_list: List[List]):
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
    description="Get a list of stocks from an S3 file"
)
def get_s3_data(context) -> List[Stock]:
    return list(csv_helper(context.op_config["s3_key"]))


@op(
    description="Given a list of stocks, return the Aggregation with the greatest high value"
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key=lambda stock: stock.high, reverse=True)
    # context.log.info("Stock date: {date}, Stock high: {high}".format(date = sorted_stocks[0].date, high = sorted_stocks[0].high))
    return Aggregation(date = sorted_stocks[0].date, high = sorted_stocks[0].high)

@op(
    description="Upload the Aggregation to Redis"
)
def put_redis_data(context, stock: Aggregation):
    context.log.info("Stock date: {date}, Stock high: {high}".format(date = stock.date, high = stock.high))


@job
def week_1_pipeline():
    # get_s3_data()
    # process_data(get_s3_data())
    put_redis_data(process_data(get_s3_data()))

