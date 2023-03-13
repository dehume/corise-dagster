import datetime

import pytest
from challenge.week_1_challenge import (
    empty_stock_notify_op,
    machine_learning_dynamic_job,
)
from dagster import build_op_context
from project.week_1 import (
    Aggregation,
    Stock,
    get_s3_data_op,
    machine_learning_job,
    process_data_op,
    put_redis_data_op,
    put_s3_data_op,
)


@pytest.fixture
def file_path():
    return "week_1/data/stock.csv"


@pytest.fixture
def empty_file_path():
    return "week_1/data/empty_stock.csv"


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
        result = get_s3_data_op(context)
        assert result[0] == Stock(
            date=datetime.datetime(2018, 10, 15, 0, 0),
            close=259.5900,
            volume=6189026.0000,
            open=259.0600,
            high=263.2800,
            low=254.5367,
        )


@pytest.mark.challenge
def test_get_s3_data_empty(empty_file_path):
    with build_op_context(op_config={"s3_key": empty_file_path}) as context:
        get_s3_data_op(context)


def test_process_data(stocks):
    with build_op_context() as context:
        assert process_data_op(context, stocks) == Aggregation(date=datetime.datetime(2022, 1, 3, 0, 0), high=12.0)
        assert process_data_op(context, stocks[::-1]) == Aggregation(
            date=datetime.datetime(2022, 1, 3, 0, 0), high=12.0
        )


def test_put_redis_data(aggregation):
    with build_op_context() as context:
        put_redis_data_op(context, aggregation)


def test_put_s3_data(aggregation):
    with build_op_context() as context:
        put_s3_data_op(context, aggregation)


def test_job(file_path):
    result = machine_learning_job.execute_in_process(
        run_config={"ops": {"get_s3_data_op": {"config": {"s3_key": file_path}}}}
    )
    assert result.success


@pytest.mark.challenge
def test_empty_stock_notify():
    with build_op_context() as context:
        empty_stock_notify_op(context, aggregation)


@pytest.mark.challenge
def test_job_challenge(file_path):
    result = machine_learning_dynamic_job.execute_in_process(
        run_config={
            "ops": {
                "get_s3_data_op": {"config": {"s3_key": file_path}},
                "process_data_op": {"config": {"nlargest": 2}},
            }
        }
    )
    assert result.success


@pytest.mark.challenge
def test_job_challenge_empty(empty_file_path):
    result = machine_learning_dynamic_job.execute_in_process(
        run_config={
            "ops": {
                "get_s3_data_op": {"config": {"s3_key": empty_file_path}},
                "process_data_op": {"config": {"nlargest": 2}},
            }
        }
    )
    assert result.success
    assert result.output_for_node("get_s3_data_op", "empty_stocks") is None
    assert result.output_for_node("empty_stock_notify_op") is None
