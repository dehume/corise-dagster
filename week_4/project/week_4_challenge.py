from random import randint

from dagster import AssetIn, asset
from dagster_dbt import load_assets_from_dbt_project
from project.dbt_config import DBT_PROJECT_PATH


@asset(
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
)
def create_dbt_table(context):
    sql = "CREATE SCHEMA IF NOT EXISTS analytics;"
    context.resources.database.execute_query(sql)
    sql = "CREATE TABLE IF NOT EXISTS analytics.dbt_table (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)


@asset(
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
)
def insert_dbt_data(context, create_dbt_table):
    sql = "INSERT INTO analytics.dbt_table (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@asset
def final(context):
    context.log.info("Week 4 Challenge completed")