from random import randint

from dagster import In, Nothing, String, graph, op, Output, Out,get_dagster_logger,HookContext,failure_hook,success_hook
from dagster_dbt import dbt_cli_resource,DbtCliOutput
from dagster_dbt.cli.utils import parse_run_results

from dagster_ucr.resources import postgres_resource



DBT_PROJECT_PATH = "/opt/dagster/dagster_home/dagster_ucr/dbt_test_project/."
DBT_TARGET_PATH  = "/tmp/target/." 

logger = get_dagster_logger()

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
    logger.info(f"sql looks like: {sql}")
    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@op(required_resource_keys={"dbt"},
    ins={"start": In(Nothing)},
    out=Out(dagster_type=DbtCliOutput),
    tags={"kind": "dbt"},)
def dbt_run(context):
    dbt_cli_output = context.resources.dbt.run()

    yield Output(dbt_cli_output)

@op(required_resource_keys={"dbt"},
    ins={"start": In(Nothing)},
    out=Out(dagster_type=DbtCliOutput),
    tags={"kind": "dbt"},)
def dbt_test(context):
    dbt_cli_output = context.resources.dbt.test()

    yield Output(dbt_cli_output)    


@success_hook()
def success_op(context: HookContext):
    results = parse_run_results(path=DBT_PROJECT_PATH,target_path=DBT_TARGET_PATH)
    message = f"***Op {context.op.name} success*** :x:  {results}"
    logger.info(message)
    
@failure_hook()
def failure_op(context: HookContext):
    results = parse_run_results(path=DBT_PROJECT_PATH,target_path=DBT_TARGET_PATH)
    message = f"***Op {context.op.name} failed*** :x:  {results}"
    logger.info(message)
   

@graph()
def dbt():
    dbt_test.with_hooks({success_op,failure_op})(dbt_run(insert_dbt_data(create_dbt_table())))

   


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
                "target_path": DBT_TARGET_PATH
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
    }
)
