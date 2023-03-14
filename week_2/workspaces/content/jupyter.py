from datetime import datetime
from typing import List

import dagstermill as dm
from dagster import (
    In,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    file_relative_path,
    graph,
    op,
)
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context: OpExecutionContext):
    output = list()
    for row in context.resources.s3.get_data(key_name=context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        output.append(stock)
    return output


process_jupyter_notebook = dm.define_dagstermill_op(
    "jupyter_process_data",
    notebook_path=file_relative_path(__file__, "process_data.ipynb"),
    ins={"stocks": In(dagster_type=List[Stock])},
    outs={"aggregation": Out(dagster_type=Aggregation)},
    description="(Notebook) Given a list of stocks return the Aggregation with the greatest high",
)


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
    description="Upload an Aggregation to Redis",
)
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation):
    context.resources.redis.put_data(
        name=str(aggregation.date),
        value=str(aggregation.high),
    )


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Upload an Aggregation to S3 file",
)
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation):
    s3_key = f"/aggregations/{datetime.today().strftime('%Y_%m_%d')}.csv"
    context.resources.s3.put_data(
        key_name=s3_key,
        data=aggregation,
    )


@graph
def week_2_graph_jupyter():
    stock_data = get_s3_data()
    agg_data = process_jupyter_notebook(stock_data)
    put_redis_data(agg_data)
    put_s3_data(agg_data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_job_jupyter_local = week_2_graph_jupyter.to_job(
    name="week_2_job_jupyter_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
        "output_notebook_io_manager": dm.local_output_notebook_io_manager,
    },
)

week_2_job_jupyter_docker = week_2_graph_jupyter.to_job(
    name="week_2_job_jupyter_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
        "output_notebook_io_manager": dm.local_output_notebook_io_manager,
    },
)
