from os import access
from typing import Dict

import boto3
import redis
import sqlalchemy
from dagster import Field, Int, String, resource


class Postgres:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self._engine = sqlalchemy.create_engine(self.uri)

    @property
    def uri(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}/{self.database}"

    def execute_query(self, query: str):
        self._engine.execute(query)


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

    def get_file(self, key_name: str):
        return self._client.get_object(Bucket=self.bucket, Key=key_name)


class Redis:
    def __init__(self, host: str, port: int):
        self.client = redis.Redis(host=host, port=port, db=0)

    def put_data(self, set_name: str, data: Dict):
        self.client.set(set_name, data)


@resource(
    config_schema={
        "host": Field(String),
        "user": Field(String),
        "password": Field(String),
        "database": Field(String),
    },
    description="A resource that can run Postgres",
)
def postgres_resource(context) -> Postgres:
    """This resource defines a Postgres client"""
    return Postgres(
        host=context.resource_config["host"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        database=context.resource_config["database"],
    )


@resource()
def s3_resource(context) -> S3:
    pass


@resource()
def redis_resource(context) -> Redis:
    pass
