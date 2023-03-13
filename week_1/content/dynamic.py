from typing import List

from dagster import DynamicOut, DynamicOutput, Nothing, String, graph, op

# @op
# def get_name() -> List[String]:
#     return ["dagster", "mike", "molly"]


# @op
# def capitalize_name(names: List[String]) -> List[String]:
#     return [name.capitalize() for name in names]


# @op
# def hello(context, names: List[String]):
#     for name in names:
#         context.log.info(f"Hello, {name}!")


# @graph
# def hello_dagster():
#     hello(capitalize_name(get_name()))


@op(out=DynamicOut())
def get_name() -> String:
    for name in ["dagster", "mike", "molly"]:
        yield DynamicOutput(name, mapping_key=name)


@op
def capitalize_name(name: String) -> String:
    return name.capitalize()


@op
def hello(context, names: List[String]) -> Nothing:
    for name in names:
        context.log.info(f"Hello, {name}!")


@graph
def hello_dagster():
    names = get_name()
    capitalized_names = names.map(capitalize_name)
    hello(capitalized_names.collect())


job = hello_dagster.to_job()
