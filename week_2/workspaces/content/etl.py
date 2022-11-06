from random import randint

from dagster import ResourceDefinition, String, graph, op
from workspaces.resources import postgres_resource


@op(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def create_table(context) -> String:
    table_name = context.op_config["table_name"]
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@op(
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def insert_into_table(context, table_name: String):
    sql = f"INSERT INTO {table_name} (column_1) VALUES (1);"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@graph
def etl():
    table = create_table()
    insert_into_table(table)


local = {"ops": {"create_table": {"config": {"table_name": "fake_table"}}}}

docker = {
    "resources": {
        "database": {
            "config": {
                "host": "postgresql",
                "user": "postgres_user",
                "password": "postgres_password",
                "database": "postgres_db",
            }
        }
    },
    "ops": {"create_table": {"config": {"table_name": "postgres_table"}}},
}

etl_local = etl.to_job(
    name="etl_local",
    config=local,
    resource_defs={"database": ResourceDefinition.mock_resource()},
)

etl_docker = etl.to_job(
    name="etl_docker",
    config=docker,
    resource_defs={"database": postgres_resource},
)
