from typing import List

from dagster import Nothing, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset
def get_s3_data():
    # You can reuse the logic from the previous week
    pass


@asset
def process_data():
    # You can reuse the logic from the previous week
    pass


@asset
def put_redis_data():
    # You can reuse the logic from the previous week
    pass


@asset
def put_s3_data():
    # You can reuse the logic from the previous week
    pass


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources()
