from random import randint

from dagster_dbt import dbt_cli_resource, dbt_run_op, dbt_test_op, DbtOutput
from workspaces.resources import postgres_resource

from dagster import (
    In,
    Out,
    Output,
    Nothing,
    String,
    Any,
    graph,
    op,
    HookContext,
    failure_hook,
    success_hook
)

DBT_PROJECT_PATH = "/opt/dagster/dagster_home/dbt_test_project/."


@op(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def create_dbt_table(context) -> String:
    table_name = context.op_config["table_name"]
    schema_name = table_name.split(".")[0]
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name

@op(
    ins={"table_name": In(dagster_type=String)},
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def insert_dbt_data(context, table_name)-> Nothing:
    sql = f"INSERT INTO {table_name} (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 100)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")
    context.log.info("Batch inserted")


@op(
    required_resource_keys={"dbt"},
    tags={"kind": "dbt"}
)
def dbt_run() -> DbtOutput:
    dbt_run_op()


@op (
    required_resource_keys={"dbt"},
    out = {
        "success": Out(Any, is_required=False),
        "failure": Out(Any, is_required=False)
    },
    tags={"kind": "dbt"}

)
def dbt_test() -> DbtOutput:
    dbt_test_op()


@success_hook
def notify_success(context: HookContext):
    message = f"Op {context.op.name} finished successfully"
    context.log.info(message)


@failure_hook
def notify_failure(context: HookContext):
    message = f"Op {context.op.name} failed"
    context.log.info(message)

@graph
def week_2_challenge():
    tbl = create_dbt_table()
    dbt_test_op.with_hooks({notify_success, notify_failure})(dbt_run_op(insert_dbt_data(tbl)))

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
        "dbt": {
            "config": {
                "project_dir": DBT_PROJECT_PATH,
                "profiles_dir": DBT_PROJECT_PATH,
                "ignore_handled_error": True,
                "target": "test",
            },
        },
    },
    "ops": {"create_dbt_table": {"config": {"table_name": "analytics.dbt_table"}}},
}


week_2_challenge_docker = week_2_challenge.to_job(
    name="week_2_challenge_docker",
    config = docker,
    resource_defs = {
        "database": postgres_resource,
        "dbt": dbt_cli_resource
    }
)
