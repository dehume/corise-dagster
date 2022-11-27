from dagster import In, IOManager, Nothing, Out, String, graph, io_manager, op
from workspaces.resources import postgres_resource


class PostgresIOManager(IOManager):
    def __init__(self):
        pass

    def handle_output(self):
        pass

    def load_input(self, context):
        pass


@io_manager(required_resource_keys={"postgres"})
def postgres_io_manager(init_context):
    return PostgresIOManager()


@op(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def create_table(context) -> String:
    table_name = context.op_config["table_name"]
    schema_name = table_name.split(".")[0]
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@op
def insert_data():
    pass


@op
def table_count():
    pass


@graph
def week_3_challenge():
    pass


docker = {
    "resources": {
        "database": {
            "config": {
                "host": "postgresql",
                "user": "postgres_user",
                "password": "postgres_password",
                "database": "postgres_db",
            }
        },
    },
    "ops": {"create_table": {"config": {"table_name": "analytics.table"}}},
}


week_3_challenge_docker = week_3_challenge.to_job()
