from datetime import datetime
from typing import List

from dagster import (
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    asset,
    define_asset_job,
    load_assets_from_current_module,
)
from workspaces.types import Aggregation, Stock


@asset
def get_s3_data():
    pass


@asset
def process_data():
    pass


@asset
def put_redis_data():
    pass


@asset
def put_s3_data():
    pass


project_assets = load_assets_from_current_module()


machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
)

machine_learning_schedule = ScheduleDefinition(job=machine_learning_asset_job, cron_schedule="*/15 * * * *")
