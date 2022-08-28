from random import randint
from typing import List

from dagster import In, Nothing, Out, Output, String, graph, op
from dagster_dbt import DbtOutput, dbt_cli_resource, dbt_run_op, dbt_test_op
from dagster_dbt.cli import DbtCliOutput
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
    ins={"dbt_result": In(DbtCliOutput)},
    out={'passed': Out(dagster_type=str, is_required=False),
         'failed': Out(dagster_type=str, is_required=False)}
)
def run_test(context, dbt_result):
    context.log.info(f'dbt_result as input type is {type(dbt_result)}')
    status = dbt_result.result['results'][0]['status']
    context.log.info(f'dbt test status is: {status}')
    if status == "pass":
        yield Output(status, "passed")
    else:
        yield Output(status, "failed")

 

@op
def sucessed_op(context,_input):
    context.log.info("Passed")


@op
def failed_op(context,_input):
    context.log.info("Failed") 
    
     
@graph
def dbt():
    branch_1, branch_2 = run_test(dbt_test_op(dbt_run_op(insert_dbt_data(create_dbt_table()))))
    sucessed_op(branch_1)
    failed_op(branch_2)
    # run_test(dbt_test_op(dbt_run_op(insert_dbt_data(create_dbt_table()))))
    

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
