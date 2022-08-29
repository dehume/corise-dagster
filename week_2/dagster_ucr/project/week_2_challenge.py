from random import randint

from dagster import In, Out, Nothing, Output, graph, op, get_dagster_logger, usable_as_dagster_type
from dagster_dbt import dbt_cli_resource#, DbtCliOutput #, dbt_run_op, dbt_test_op
from dagster_ucr.resources import postgres_resource
from pydantic import BaseModel

DBT_PROJECT_PATH = "/opt/dagster/dagster_home/dagster_ucr/dbt_test_project/."

@usable_as_dagster_type(description="Output from dbt CLI command")
class DbtOutput(BaseModel):
    command_type: str
    return_code: int
    output_msg: str
    raw_logs: list

@op(
    config_schema={"table_name": str},
    required_resource_keys={"database"},
    out={"table_name": Out(dagster_type=str)},
    tags={"kind": "postgres"},
)
def create_dbt_table(context) -> str:
    table_name = context.op_config["table_name"]
    schema_name = table_name.split(".")[0]
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@op(
    required_resource_keys={"database"},
    ins={"table_name": In(dagster_type=str)},
    out={"completion_flag": Out(dagster_type=Nothing)},
    tags={"kind": "postgres"},
)
def insert_dbt_data(context, table_name: str) -> Nothing:
    sql = f"INSERT INTO {table_name} (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@op(
    required_resource_keys={"dbt"},
    ins={"completion_flag": In(dagster_type=Nothing)},
    out={"success_output": Out(dagster_type=str, is_required=False), "failure_output": Out(dagster_type=DbtOutput, is_required=False)},
    tags={"kind": "dbt"},
    description="Run a 'dbt run' command for the whole dbt project",
)
def dbt_run(context):
    raw_output = context.resources.dbt.run()
    output = DbtOutput(command_type='dbt_run', return_code=raw_output.return_code, output_msg=raw_output.logs[-1]["msg"], raw_logs=raw_output.logs)
    
    logger = get_dagster_logger()
    logger.info(f'Return code: {output.return_code}')
    logger.info(f'Output message: {output.output_msg}')
    logger.info(output.raw_logs)
    
    if output.return_code == 0:
        yield Output('dbt run step succeeded', "success_output")
    else:
        yield Output(output, "failure_output")


@op(
    required_resource_keys={"dbt"},
    ins={"success_output": In(dagster_type=str)},
    out={"success_output": Out(dagster_type=str, is_required=False), "failure_output": Out(dagster_type=DbtOutput, is_required=False)},
    tags={"kind": "dbt"},
    description="Run a 'dbt test' command for the whole dbt project",
)
def dbt_test(context, success_output):
    logger = get_dagster_logger()
    logger.info(success_output)
    raw_output = context.resources.dbt.test()
    output = DbtOutput(command_type='dbt_test', return_code=raw_output.return_code, output_msg=raw_output.logs[-1]["msg"], raw_logs=raw_output.logs)
    
    logger = get_dagster_logger()
    logger.info(f'Return code: {output.return_code}')
    logger.info(f'Output message: {output.output_msg}')
    logger.info(output.raw_logs)
    
    if output.return_code == 0:
        yield Output('dbt test step succeeded', "success_output")
    else:
        yield Output(output, "failure_output")


@op(
    ins={"failure_output": In(dagster_type=DbtOutput)},
    tags={"kind": "dbt"},
    description="Fail with some logs whenever a dbt command runs into errors"
)
def dbt_failure(failure_output) -> Nothing:
    logger = get_dagster_logger()
    logger.info(f'Caught failure on the following step: {failure_output.command_type}')
    logger.info(f'Logs: {failure_output.raw_logs}')


@op(
    ins={"success_output": In(dagster_type=str)},
    tags={"kind": "dbt"},
    description="End pipeline with success logs and joyful ALL CAPS"
)
def dbt_success(success_output) -> Nothing:
    logger = get_dagster_logger()
    logger.info(f'{success_output}')
    logger.info('PIPELINE SUCCESS! WOOHOO!!')


@graph
def dbt():
    created_table_name = create_dbt_table()

    dbt_run_success, dbt_run_failure = dbt_run(insert_dbt_data(created_table_name))
    dbt_failure(dbt_run_failure)

    dbt_test_success, dbt_test_failure = dbt_test(dbt_run_success)
    dbt_success(dbt_test_success)
    dbt_failure(dbt_test_failure)



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