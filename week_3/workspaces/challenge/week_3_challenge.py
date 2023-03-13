import json
from random import randint
from typing import Dict, List

from dagster import (
    In,
    InitResourceContext,
    InputContext,
    IOManager,
    Nothing,
    OpExecutionContext,
    Out,
    OutputContext,
    String,
    graph,
    io_manager,
    op,
)
from sqlalchemy import column, table
from workspaces.config import ANALYTICS_TABLE, POSTGRES
from workspaces.resources import postgres_resource


class PostgresIOManager(IOManager):
    def __init__(self):
        pass

    def handle_output(self):
        pass

    def load_input(self):
        pass


@io_manager(required_resource_keys={"database"})
def postgres_io_manager(init_context: InitResourceContext):
    pass


@op(
    config_schema={"table_name": String},
    out=Out(String),
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def create_table(context: OpExecutionContext):
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
def dbt_graph():
    pass


docker = {
    "resources": {
        "database": {"config": POSTGRES},
        "postgres_io": {"config": {"schema_name": "analytics", "table_name": "table"}},
    },
    "ops": {"create_table": {"config": {"table_name": ANALYTICS_TABLE}}},
}


dbt_job_docker = dbt_graph.to_job(
    name="dbt_job_docker",
)
