from dagster import Definitions
from workspaces.config import POSTGRES
from workspaces.content.etl import etl_asset_job, etl_assets
from workspaces.content.freshness import (
    fresh_asset_a,
    fresh_asset_b,
    fresh_asset_c,
    fresh_asset_d,
    freshness_alerting_sensor,
)
from workspaces.content.software_assets import corise_assets
from workspaces.resources import postgres_resource

postgres_config = postgres_resource.configured(POSTGRES)

definition = Definitions(
    sensors=[freshness_alerting_sensor],
    jobs=[etl_asset_job],
    assets=[
        fresh_asset_a,
        fresh_asset_d,
        fresh_asset_c,
        fresh_asset_b,
        *corise_assets,
        *etl_assets,
    ],
    resources={"database": postgres_config},
)
