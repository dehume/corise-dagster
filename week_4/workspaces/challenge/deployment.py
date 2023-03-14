from dagster import Definitions
from dagster_dbt import dbt_cli_resource
from workspaces.challenge.week_4_challenge import (
    create_dbt_table,
    dbt_assets,
    dbt_table,
    end,
)
from workspaces.config import DBT, POSTGRES
from workspaces.resources import postgres_resource

dbt_config = dbt_cli_resource.configured(DBT)
postgres_config = postgres_resource.configured(POSTGRES)

definition = Definitions(
    assets=dbt_assets + [create_dbt_table, dbt_table, end],
    resources={
        "dbt": dbt_config,
        "database": postgres_config,
    },
)
