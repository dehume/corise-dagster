import datetime
from unittest.mock import MagicMock

import pytest
from dagster import ResourceDefinition, build_op_context
from workspaces.project.week_2 import (
    get_s3_data,
    machine_learning_graph,
    process_data,
    put_redis_data,
    put_s3_data,
)
from workspaces.resources import mock_s3_resource
from workspaces.types import Aggregation, Stock


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


def test_get_s3_data(stock_list):
    s3_mock = MagicMock()
    s3_mock.get_data.return_value = [stock_list] * 10
    with build_op_context(op_config={"s3_key": "data/stock.csv"}, resources={"s3": s3_mock}) as context:
        get_s3_data(context)
        assert s3_mock.get_data.called


def test_process_data(stocks):
    with build_op_context() as context:
        assert process_data(context, stocks) == Aggregation(date=datetime.datetime(2022, 1, 3, 0, 0), high=12.0)


def test_put_redis_data(aggregation):
    redis_mock = MagicMock()
    with build_op_context(resources={"redis": redis_mock}) as context:
        put_redis_data(context, aggregation)
        assert redis_mock.put_data.called


def test_put_s3_data(aggregation):
    s3_mock = MagicMock()
    with build_op_context(resources={"s3": s3_mock}) as context:
        put_s3_data(context, aggregation)
        assert s3_mock.put_data.called


def test_week_2_job(stock_list):
    machine_learning_graph.execute_in_process(
        run_config={"ops": {"get_s3_data": {"config": {"s3_key": "data/stock.csv"}}}},
        resources={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
    )
