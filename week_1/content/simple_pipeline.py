from dagster import graph, op


@op
def get_name():
    return "dagster"


# @op
# def capitalize_name(name):
#     return name.capitalize()


@op
def hello(context, name):
    context.log.info(f"Hello, {name}!")


@graph
def hello_dagster():
    # hello(capitalize_name(get_name()))
    hello(get_name())


job = hello_dagster.to_job()
