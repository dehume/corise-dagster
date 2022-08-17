from datetime import datetime
from random import randint

from content.resources import postgres_resource
from dagster import (
    AssetMaterialization,
    ResourceDefinition,
    String,
    build_schedule_from_partitioned_job,
    daily_partitioned_config,
    graph,
    op,
    static_partitioned_config,
)


@op(
    config_schema={"table_name": String, "process_date": String},
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

    # New this week
    context.log_event(
        AssetMaterialization(
            asset_key="my_micro_batch",
            description="Inserting a random batch of records",
            metadata={"table_name": table_name, "number_of_rows": number_of_rows},
        )
    )


@graph
def etl():
    table = create_table()
    insert_into_table(table)


local = {"ops": {"create_table": {"config": {"table_name": "fake_table", "process_date": "2020-07-01"}}}}


@daily_partitioned_config(start_date=datetime(2022, 7, 1))
def local_config(start: datetime, _end: datetime):
    return {
        "ops": {"create_table": {"config": {"table_name": "fake_table", "process_date": start.strftime("%Y-%m-%d")}}}
    }


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
    "ops": {"create_table": {"config": {"table_name": "postgres_table", "process_date": "2020-07-01"}}},
}


@static_partitioned_config(partition_keys=["foo", "biz", "bar"])
def docker_config(partition_key: str):
    return {
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
        "ops": {"create_table": {"config": {"table_name": partition_key, "process_date": "2020-07-01"}}},
    }


etl_local = etl.to_job(
    name="etl_local",
    config=local_config,
    resource_defs={"database": ResourceDefinition.mock_resource()},
)

etl_docker = etl.to_job(
    name="etl_docker",
    config=docker_config,
    resource_defs={"database": postgres_resource},
)

etl_local_partitioned_schedule = build_schedule_from_partitioned_job(etl_local)
