from random import randint

from dagster import (
    AssetSelection,
    String,
    asset,
    define_asset_job,
    load_assets_from_current_module,
)


@asset(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
)
def create_table(context) -> String:
    table_name = context.op_config["table_name"]
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@asset(
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
)
def insert_into_table(context, create_table):
    sql = f"INSERT INTO {create_table} (column_1) VALUES (1);"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


etl_assets = load_assets_from_current_module(
    group_name="etl",
)

etl_asset_job = define_asset_job(
    name="etl_asset_job",
    selection=AssetSelection.groups("etl"),
    config={"ops": {"create_table": {"config": {"table_name": "fake_table"}}}},
)
