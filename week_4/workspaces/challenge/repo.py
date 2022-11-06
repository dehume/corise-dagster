from dagster import repository, with_resources
from dagster_dbt import dbt_cli_resource
from workspaces.challenge.week_4_challenge import (
    create_dbt_table,
    dbt_assets,
    end,
    insert_dbt_data,
)
from workspaces.dbt_config import DBT_PROJECT_PATH
from workspaces.resources import postgres_resource


@repository
def repo():
    return with_resources(
        dbt_assets + [create_dbt_table, insert_dbt_data, end],
        resource_defs={
            "dbt": dbt_cli_resource.configured(
                {
                    "project_dir": DBT_PROJECT_PATH,
                    "profiles_dir": DBT_PROJECT_PATH,
                    "target": "test",
                }
            ),
            "database": postgres_resource.configured(
                {
                    "host": "postgresql",
                    "user": "postgres_user",
                    "password": "postgres_password",
                    "database": "postgres_db",
                }
            ),
        },
    )
