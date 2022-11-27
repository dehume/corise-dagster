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


@op(config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List[Stock],
                       description="List of Stock objects")})
def get_s3_data(context) -> List[Stock]:
    s3data = context.op_config["s3_key"]  # {'s3_key': 'week_1/data/stock.csv'}
    # with open(s3data, 'r') as s3:
    #     allstocks = s3.readlines()
    # stocks = [re.findall(r'"([^"]*)"', st) for st in allstocks]
    # return [Stock.from_list(stx) for stx in stocks]
    return list(csv_helper(s3data))


@op(ins={"stocks": In(dagster_type=List[Stock])},
    out={"data": Out(dagster_type=Aggregation,
                     description="Returns Aggregation from Stock with highest high value")})
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highestock = max(stocks, key=lambda st: st.high)
    return Aggregation(date=highestock.date, high=highestock.high)

@op(ins={"data": In(dagster_type=Aggregation)})
def put_redis_data(context, data:Aggregation):
    pass

@job
def week_1_pipeline():
    put_redis_data(process_data(get_s3_data()))
