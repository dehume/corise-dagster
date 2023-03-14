from random import randint

from dagster import AssetIn, OpExecutionContext, asset
from dagster_dbt import load_assets_from_dbt_project
from workspaces.config import DBT_PROJECT_PATH

SOURCE_TABLE = "analytics.dbt_table"


@asset(
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
    key_prefix=["postgresql"],
)
def create_dbt_table(context):
    sql = "CREATE SCHEMA IF NOT EXISTS analytics;"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)


@asset(
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
    key_prefix=["postgresql"],
)
def dbt_table(context: OpExecutionContext, create_dbt_table):
    sql = f"INSERT INTO {SOURCE_TABLE} (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@asset
def end():
    pass
