from random import randint

from content.resources import postgres_resource
from dagster import String, asset, with_resources


@asset(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
    group_name="etl",
)
def create_table(context) -> String:
    table_name = context.op_config["table_name"]
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@asset(
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
    group_name="etl",
)
def insert_into_table(context, create_table):
    sql = f"INSERT INTO {create_table} (column_1) VALUES (1);"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


create_table_docker, insert_into_table_docker = with_resources(
    definitions=[create_table, insert_into_table],
    resource_defs={"database": postgres_resource},
    resource_config_by_key={
        "database": {
            "config": {
                "host": "postgresql",
                "user": "postgres_user",
                "password": "postgres_password",
                "database": "postgres_db",
            }
        },
    },
)
