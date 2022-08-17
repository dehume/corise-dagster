from dagster import graph, op


@op
def get_name() -> str:
    return "dagster"


# @op
# def capitalize_name(name: str) -> str:
#     return name.capitalize()


@op
def hello(name: str):
    print(f"Hello, {name}!")


@graph
def hello_dagster():
    # hello(capitalize_name(get_name()))
    hello(get_name())


job = hello_dagster.to_job()
