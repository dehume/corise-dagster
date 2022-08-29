# pyright: reportMissingImports=false
from random import randint

from dagster import In, Nothing, Out, Output, String, Int, graph, op
from dagster_dbt import dbt_cli_resource, dbt_run_op, dbt_test_op
from dagster_ucr.resources import postgres_resource

DBT_PROJECT_PATH = "/opt/dagster/dagster_home/dagster_ucr/dbt_test_project/."


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
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def insert_dbt_data(context, table_name: String):
    sql = f"INSERT INTO {table_name} (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@op(
    required_resource_keys={"dbt"},
    tags={"kind": "dbt"},
    ins={"start_after":In(Nothing)},
    description="dbt run step"
    )
def dbt_run(context):
    context.resources.dbt.run()


@op(
    required_resource_keys={"dbt"},
    tags={"kind": "dbt"},
    ins={"dbt_run": In(Nothing)},
    out={"dbt_success": Out(dagster_type=Int, is_required=False), "dbt_failure": Out(dagster_type=Int, is_required=False)},
    description="dbt test step"
    )
def dbt_test(context) -> Int:
    test_run = context.resources.dbt.test()
    test_result = test_run.return_code
    if test_result == 0:
        yield Output(test_result, "dbt_success")
    else:
        yield Output(test_result, "dbt_failure")
    


@op(
    tags={"kind": "dbt"},
    ins={"dbt_test_result":In(dagster_type=Int)},
    description="dbt success"
    )
def dbt_success(context, dbt_test_result: int):
    result_str = str(dbt_test_result)
    context.log.info(f"dbt run was a success, finishing with exit code of {result_str}")
    


@op(
    tags={"kind": "dbt"},
    ins={"dbt_test_result":In(dagster_type=Int)},
    description="dbt failure"
    )
def dbt_failure(context, dbt_test_result: int):
    result_str = str(dbt_test_result)
    context.log.info(f"dbt run was a failure, finishing with exit code of {result_str}")


@graph
def dbt():
    dbt_table = create_dbt_table()
    dbt_data = insert_dbt_data(dbt_table)
    test_result_1, test_result_2 = dbt_test(dbt_run(dbt_data))
    dbt_success(test_result_1)
    dbt_failure(test_result_2)


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


dbt_docker = dbt.to_job(
    name="dbt_docker",
    config=docker,
    resource_defs={
        "database": postgres_resource,
        "dbt": dbt_cli_resource,
    },
)
