from email.policy import strict

from dagster import DynamicOut, DynamicOutput, graph, op

# @op
# def get_name() -> list:
#     return ["dagster", "mike", "molly"]


# @op
# def capitalize_name(names: list) -> list:
#     return [name.capitalize() for name in names]


# @op
# def hello(names: list):
#     for name in names:
#         print(f"Hello, {name}!")


# @graph
# def hello_dagster():
#     hello(capitalize_name(get_name()))


@op(out=DynamicOut())
def get_name() -> str:
    for name in ["dagster", "mike", "molly"]:
        yield DynamicOutput(name, mapping_key=name)


@op
def capitalize_name(name: str) -> str:
    return name.capitalize()


@op
def hello(names: list):
    for name in names:
        print(f"Hello, {name}!")


@graph
def hello_dagster():
    names = get_name()
    capitalized_names = names.map(capitalize_name)
    hello(capitalized_names.collect())


job = hello_dagster.to_job()
