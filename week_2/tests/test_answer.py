from datetime import datetime
from unittest.mock import MagicMock

import pytest
from dagster import ResourceDefinition, build_op_context

from dagster_ucr.project.week_2 import (
    get_s3_data,
    process_data,
    put_redis_data,
    week_2_pipeline,
)
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource


@pytest.fixture
def stock_list():
    return [
        ["2020/09/01", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020/09/02", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020/09/03", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020/09/04", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020/09/05", "10.0", "10", "10.0", "10.0", "10.0"],
    ]


@pytest.fixture
def stock():
    return Stock(date=datetime(2022, 1, 1, 0, 0), close=10.0, volume=10, open=10.0, high=10.0, low=10.0)


@pytest.fixture
def aggregation():
    return Aggregation(date=datetime(2022, 1, 1, 0, 0), high=10.0)


def test_stock(stock):
    assert isinstance(stock, Stock)
    assert stock.date.month == 1


def test_stock_class_method(stock_list):
    stock = Stock.from_list(stock_list[0])
    assert isinstance(stock, Stock)
    assert stock.date.month == 9


def test_aggregation(aggregation):
    assert isinstance(aggregation, Aggregation)


def test_get_s3_data(stock_list):
    s3_mock = MagicMock()
    s3_mock.get_data.return_value = stock_list
    with build_op_context(resources={"s3": s3_mock}) as context:
        get_s3_data(context)
        assert s3_mock.get_data.called


def test_process_data(stock):
    with build_op_context(op_config={"month": 9}) as context:
        process_data(context, [stock])


def test_put_redis_data(aggregation):
    redis_mock = MagicMock()
    with build_op_context(resources={"redis": redis_mock}) as context:
        put_redis_data(context, aggregation)
        assert redis_mock.put_data.called


def test_week_2_pipeline(stock_list):
    week_2_pipeline.execute_in_process(
        run_config={"ops": {"process_data": {"config": {"month": 9}}}},
        resources={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
    )
