from dagster import Definitions
from workspaces.config import REDIS, S3
from workspaces.project.week_4 import (
    machine_learning_asset_job,
    machine_learning_schedule,
    project_assets,
)
from workspaces.resources import redis_resource, s3_resource

s3_config = s3_resource.configured(S3)
redis_config = redis_resource.configured(REDIS)


definition = Definitions(
    schedules=[machine_learning_schedule],
    assets=[*project_assets],
    jobs=[machine_learning_asset_job],
    resources={
        "s3": s3_config,
        "redis": redis_config,
    },
)
