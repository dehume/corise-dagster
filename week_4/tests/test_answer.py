import datetime
from unittest.mock import MagicMock

import pytest
from dagster import AssetKey, build_op_context
from workspaces.config import REDIS, S3
from workspaces.project.week_4 import (
    get_s3_data,
    process_data,
    put_redis_data,
    put_s3_data,
)
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


@pytest.fixture
def resource_config():
    return {
        "resources": {
            "s3": {"config": S3},
            "redis": {"config": REDIS},
        },
    }


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


def test_get_s3_data_asset():
    assert get_s3_data.required_resource_keys == {"s3", "io_manager"}
    assert get_s3_data.group_names_by_key == {AssetKey(["get_s3_data"]): "default"}


def test_process_data_asset():
    assert process_data.required_resource_keys == {"io_manager"}
    assert process_data.group_names_by_key == {AssetKey(["process_data"]): "default"}


def test_put_redis_data_asset():
    assert put_redis_data.required_resource_keys == {"redis", "io_manager"}
    assert put_redis_data.group_names_by_key == {AssetKey(["put_redis_data"]): "default"}


def test_put_s3_data_asset():
    assert put_s3_data.required_resource_keys == {"s3", "io_manager"}
    assert put_s3_data.group_names_by_key == {AssetKey(["put_s3_data"]): "default"}
