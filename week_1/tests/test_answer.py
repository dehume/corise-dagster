import datetime

import pytest
from dagster import build_op_context
from project.week_1 import (
    Aggregation,
    Stock,
    get_s3_data,
    process_data,
    put_redis_data,
    week_1_pipeline,
)


@pytest.fixture
def file_path():
    return "week_1/data/stock.csv"


@pytest.fixture
def stocks():
    return [
        Stock(date=datetime.datetime(2022, 1, 1, 0, 0), close=10.0, volume=10, open=10.0, high=10.0, low=10.0),
        Stock(date=datetime.datetime(2022, 1, 2, 0, 0), close=10.0, volume=10, open=11.0, high=10.0, low=10.0),
        Stock(date=datetime.datetime(2022, 1, 3, 0, 0), close=10.0, volume=10, open=10.0, high=12.0, low=10.0),
        Stock(date=datetime.datetime(2022, 1, 4, 0, 0), close=10.0, volume=10, open=10.0, high=11.0, low=10.0),
    ]


@pytest.fixture
def aggregation():
    return Aggregation(date=datetime.datetime(2022, 1, 1, 0, 0), high=10.0)


@pytest.fixture
def stock_list():
    return ["2020/09/01", "10.0", "10", "10.0", "10.0", "10.0"]


def test_stock(stocks):
    assert isinstance(stocks[0], Stock)
    assert stocks[0].date.month == 1


def test_stock_class_method(stock_list):
    stock = Stock.from_list(stock_list)
    assert isinstance(stock, Stock)


def test_aggregation(aggregation):
    assert isinstance(aggregation, Aggregation)


def test_get_s3_data(file_path):
    with build_op_context(op_config={"s3_key": file_path}) as context:
        get_s3_data(context)


def test_process_data(stocks):
    assert process_data(stocks) == Aggregation(date=datetime.datetime(2022, 1, 3, 0, 0), high=12.0)


def test_put_redis_data(aggregation):
    put_redis_data(aggregation)


def test_job(file_path):
    week_1_pipeline.execute_in_process(run_config={"ops": {"get_s3_data": {"config": {"s3_key": file_path}}}})
