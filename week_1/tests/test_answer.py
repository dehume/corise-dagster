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
def stock():
    return Stock(date="2020-01-01", close=10.0, volume=10, open=10.0, high=10.0, low=10.0)


@pytest.fixture
def aggregation():
    return Aggregation(day="2020-01-01", high=10.0)


def test_stock(stock):
    assert isinstance(stock, Stock)


def test_aggregation(aggregation):
    assert isinstance(aggregation, Aggregation)


def test_get_s3_data():
    get_s3_data()


def test_process_data(stock):
    with build_op_context(op_config={"month": 9}) as context:
        process_data(context, [stock])


def test_put_redis_data(aggregation):
    put_redis_data(aggregation)


def test_job():
    week_1_pipeline.execute_in_process(run_config={"ops": {"process_data": {"config": {"month": 9}}}})
