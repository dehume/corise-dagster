from dagster import (
    In,
    IOManager,
    Out,
    String,
    graph,
    io_manager,
    op,
    InputContext,
    OutputContext,
    InitResourceContext
)
from workspaces.resources import postgres_resource
from workspaces.types import PostgresRecord
import random as rand
import string

class PostgresIOManager(IOManager):
    def __init__(self,context: InitResourceContext):
        self.pg = postgres_resource(context)

    def handle_output(self, context: OutputContext, obj:PostgresRecord):
        table_name = context.config['table_name']
        v1, v2, v3 = obj.v1, obj.v2, obj.v3
        sql = f"INSERT INTO {table_name} VALUES ({v1},{v2},{v3});"
        self.pg.execute_query(sql)

    def load_input(self, context: InputContext):
        table_name = context.upstream_output.config["table_name"]
        sql= f"select count(*) from {table_name};"
        return self.pg.exeecute_query(sql)


@io_manager(
    output_config_schema={"table_name": str},
    required_resource_keys={"postgres"}
)
def postgres_io_manager(init_context):
    return PostgresIOManager(init_context)


@op(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    tags={"kind": "postgres"}
)
def create_table(context) -> String:
    table_name = context.op_config["table_name"]
    schema_name = table_name.split(".")[0]
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@op(
    out= Out(io_manager_key="postgres_io"),
    description= "insert 3 random values into the specified table_name (schema.table)."
)
def insert_random_data() -> PostgresRecord:
    v1, v2, v3 = [''.join(rand.choices(string.ascii_uppercase + string.digits, k=5)) for i in range(0,3)]
    postgres_record = PostgresRecord(v1=v1, v2=v2, v3=v3)
    return postgres_record


@op(
    description= "count how many records are in the specified table_name (schema.table)."
)
def table_count(table_name: String) -> int:
    pass


@graph
def week_3_challenge():
    tbl_name = create_table()
    insert_random_data(tbl_name)

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
    "ops": {
        "create_table":{
            "config": {
                "table_name": "analytics.table"
            }
        },
        "insert_data":{
            "outputs": {
                "result": {
                    "table_name": "analytics.table"
                }
            }
        }
    },
    "postgres_io": postgres_io_manager
}


week_3_challenge_docker = week_3_challenge.to_job(
    resource_defs= docker
)
