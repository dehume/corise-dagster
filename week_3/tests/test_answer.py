import datetime
from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    build_op_context,
)
from project.resources import mock_s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock
from project.week_3 import (
    docker_config,
    docker_week_3_pipeline,
    docker_week_3_schedule,
    docker_week_3_sensor,
    get_s3_data,
    local_week_3_pipeline,
    local_week_3_schedule,
    process_data,
    put_redis_data,
    week_3_pipeline,
)


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
def boto3_empty_return():
    return [
        {
            "Contents": [],
            "MaxKeys": 1000,
            "KeyCount": 0,
        }
    ]


@pytest.fixture
def resource_config():
    return {
        "resources": {
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://localstack:4566",
                }
            },
            "redis": {
                "config": {
                    "host": "redis",
                    "port": 6379,
                }
            },
        },
    }


@pytest.fixture
def boto3_return():
    return [
        {
            "Contents": [
                {
                    "Key": "key_1",
                    "LastModified": "2015, 1, 1",
                },
                {
                    "Key": "key_2",
                    "LastModified": "2015, 1, 2",
                },
            ],
            "MaxKeys": 1000,
            "KeyCount": 2,
        }
    ]


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
    assert process_data(stocks) == Aggregation(date=datetime.datetime(2022, 1, 3, 0, 0), high=12.0)


def test_put_redis_data(aggregation):
    redis_mock = MagicMock()
    with build_op_context(resources={"redis": redis_mock}) as context:
        put_redis_data(context, aggregation)
        assert redis_mock.put_data.called


def test_week_3_pipeline():
    week_3_pipeline.execute_in_process(
        run_config={"ops": {"get_s3_data": {"config": {"s3_key": "data/stock.csv"}}}},
        resources={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
    )


def test_docker_week_3_pipeline():
    assert docker_week_3_pipeline._solid_retry_policy == RetryPolicy(max_retries=10, delay=1)


def test_local_week_3_schedule():
    assert local_week_3_schedule.job == local_week_3_pipeline
    assert local_week_3_schedule.cron_schedule == "*/15 * * * *"


def test_docker_week_3_schedule():
    assert docker_week_3_schedule.job == docker_week_3_pipeline
    assert docker_week_3_schedule.cron_schedule == "0 * * * *"


@patch("boto3.client")
def test_get_s3_keys(mock, boto3_return):
    mock.return_value.list_objects_v2.side_effect = boto3_return
    result = get_s3_keys(bucket="bucket", prefix="prefix")

    mock.assert_called_with(service_name="s3")
    mock.return_value.list_objects_v2.assert_called_with(
        Bucket="bucket",
        Delimiter="",
        MaxKeys=1000,
        Prefix="prefix",
        StartAfter="",
    )
    assert result == ["key_1", "key_2"]


@patch("boto3.client")
def test_docker_week_3_sensor_none(mock, boto3_empty_return):
    mock.return_value.list_objects_v2.side_effect = boto3_empty_return
    result = docker_week_3_sensor(SensorEvaluationContext(None, None, None, None, None))
    assert next(result) == SkipReason(skip_message="No new s3 files found in bucket.")


@patch("boto3.client")
def test_docker_week_3_sensor_keys(mock, resource_config, boto3_return):
    mock.return_value.list_objects_v2.side_effect = boto3_return
    result = docker_week_3_sensor(SensorEvaluationContext(None, None, None, None, None))
    assert next(result) == RunRequest(
        run_key="key_1",
        run_config={
            **resource_config,
            "ops": {"get_s3_data": {"config": {"s3_key": "key_1"}}},
        },
        tags={},
        job_name=None,
    )
    assert next(result) == RunRequest(
        run_key="key_2",
        run_config={
            **resource_config,
            "ops": {"get_s3_data": {"config": {"s3_key": "key_2"}}},
        },
        tags={},
        job_name=None,
    )


def test_docker_config(resource_config):
    keys = docker_config.get_partition_keys()
    assert keys == [str(n) for n in range(1, 11)]
    assert docker_config.get_run_config_for_partition_key(keys[0]) == {
        **resource_config,
        "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_1.csv"}}},
    }
