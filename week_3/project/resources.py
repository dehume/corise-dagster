import csv
from random import randint
from typing import Iterator
from unittest.mock import MagicMock

import boto3
import redis
from dagster import Field, Int, String, resource


class S3:
    def __init__(self, bucket: str, access_key: str, secret_key: str, endpoint_url: str = None):
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.client = self._client()

    def _client(self):
        session = boto3.session.Session()
        return session.client(
            service_name="s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint_url,
        )

    def get_data(self, key_name: str) -> Iterator:
        obj = self.client.get_object(Bucket=self.bucket, Key=key_name)
        data = obj["Body"].read().decode("utf-8").split("\n")
        for record in csv.reader(data):
            yield record


class Redis:
    def __init__(self, host: str, port: int):
        self.client = redis.Redis(host=host, port=port)

    def put_data(self, name: str, value: str):
        # Occasional error
        if randint(0, 1) == 0:
            raise Exception("Injected occasional error")
        self.client.set(name, value)


@resource
def mock_s3_resource(context):
    stocks = [
        ["2020/09/01", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020/09/02", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020/09/03", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020/09/04", "10.0", "10", "10.0", "10.0", "10.0"],
        ["2020/09/05", "10.0", "10", "10.0", "10.0", "10.0"],
    ]
    s3_mock = MagicMock()
    s3_mock.get_data.return_value = stocks
    return s3_mock


@resource(
    config_schema={
        "bucket": Field(String),
        "access_key": Field(String),
        "secret_key": Field(String),
        "endpoint_url": Field(String),
    },
    description="A resource that can run S3",
)
def s3_resource(context) -> S3:
    """This resource defines a S3 client"""
    return S3(
        bucket=context.resource_config["bucket"],
        access_key=context.resource_config["access_key"],
        secret_key=context.resource_config["secret_key"],
        endpoint_url=context.resource_config["endpoint_url"],
    )


@resource(
    config_schema={
        "host": Field(String),
        "port": Field(Int),
    },
    description="A resource that can run Redis",
)
def redis_resource(context) -> Redis:
    """This resource defines a Redis client"""
    return Redis(
        host=context.resource_config["host"],
        port=context.resource_config["port"],
    )

