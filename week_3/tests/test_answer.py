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
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
from workspaces.project.week_3 import (
    docker_config,
    get_s3_data,
    machine_learning_graph,
    machine_learning_job_docker,
    machine_learning_job_local,
    machine_learning_schedule_docker,
    machine_learning_schedule_local,
    machine_learning_sensor_docker,
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
            "s3": {"config": S3},
            "redis": {"config": REDIS},
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


def test_machine_learning_job():
    machine_learning_graph.execute_in_process(
        run_config={"ops": {"get_s3_data": {"config": {"s3_key": "data/stock.csv"}}}},
        resources={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
    )


def test_machine_learning_job_docker():
    assert machine_learning_job_docker._solid_retry_policy == RetryPolicy(max_retries=10, delay=1)


def test_machine_learning_schedule_local():
    assert machine_learning_schedule_local.job == machine_learning_job_local
    assert machine_learning_schedule_local.cron_schedule == "*/15 * * * *"


def test_machine_learning_schedule_docker():
    assert machine_learning_schedule_docker.job == machine_learning_job_docker
    assert machine_learning_schedule_docker.cron_schedule == "0 * * * *"


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
def test_machine_learning_sensor_docker_none(mock, boto3_empty_return):
    mock.return_value.list_objects_v2.side_effect = boto3_empty_return
    result = machine_learning_sensor_docker(SensorEvaluationContext(None, None, None, None, None))
    assert next(result) == SkipReason(skip_message="No new s3 files found in bucket.")


@patch("boto3.client")
def test_machine_learning_sensor_docker_keys(mock, resource_config, boto3_return):
    mock.return_value.list_objects_v2.side_effect = boto3_return
    result = machine_learning_sensor_docker(SensorEvaluationContext(None, None, None, None, None))
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
