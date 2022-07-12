from unittest.mock import MagicMock

import pytest
from dagster import ResourceDefinition, build_op_context

from dagster_ucr.week_2 import (
    get_s3_data,
    process_data,
    put_redis_data,
    week_2_pipeline,
)
from dagster_ucr.types import Aggregation, Stock


@pytest.fixture
def s3_data():
    return [
        ["2020-01-01", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020-01-02", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020-01-03", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020-01-04", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020-01-05", "10.0", "10", "10.0", "10.0", "10.0"],
    ]


@pytest.fixture
def stock():
    return Stock(date="2020-01-01", close=10.0, volume=10, open=10.0, high=10.0, low=10.0)


@pytest.fixture
def aggregation():
    return Aggregation(day="2020-01-01", high=10.0)


def test_get_s3_data(s3_data):
    s3_mock = MagicMock()
    s3_mock.get_file.return_value = s3_data
    with build_op_context(resources={"s3": s3_mock}) as context:
        get_s3_data(context)
        assert s3_mock.get_file.called


def test_process_data(stock):
    with build_op_context(op_config={"month": 9}) as context:
        process_data(context, [stock])


def test_put_redis_data(aggregation):
    redis_mock = MagicMock()
    with build_op_context(resources={"redis": redis_mock}) as context:
        put_redis_data(context, aggregation)
        assert redis_mock.put_data.called


def test_week_2_pipeline(s3_data):
    week_2_pipeline.execute_in_process(
        run_config={"ops": {"process_data": {"config": {"month": 9}}}},
        resources={"s3": ResourceDefinition.mock_resource(), "redis": ResourceDefinition.mock_resource()},
    )
