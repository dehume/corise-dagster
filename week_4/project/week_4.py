from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset
def get_s3_data():
    # Use your op logic from week 3
    pass


@asset
def process_data():
    # Use your op logic from week 3 (you will need to make a slight change)
    pass


@asset
def put_redis_data():
    # Use your op logic from week 3 (you will need to make a slight change)
    pass


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources()
