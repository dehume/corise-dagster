from datetime import datetime

from dagster import (
    DagsterType,
    In,
    Nothing,
    Out,
    String,
    graph,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel, validator

DagName = DagsterType(
    type_check_fn=lambda _, value: "dag" in value, name="DagName", description="A string that must include `dag`"
)


@usable_as_dagster_type(description="A string that must include `dag`")
class PydanticDagName(BaseModel):
    name: str
    date_time: datetime

    @validator("name", allow_reuse=True)
    def name_must_contain_dag(cls, value):
        assert "dag" in value, "Name must contain `dag`"
        return value


@op(
    out={"name": Out(dagster_type=String, description="The generated name")},
)
def get_name():
    return "dagster"


@op(
    ins={"name": In(dagster_type=DagName, description="Generated dag name")},
    out=Out(Nothing),
)
def hello(context, name):
    context.log.info(f"Hello, {name}!")


@graph
def hello_dagster():
    hello(get_name())


job = hello_dagster.to_job()
