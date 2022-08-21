from dagster import String, graph, op


@op(config_schema={"name": String})
def get_name(context) -> str:
    return context.op_config["name"]


@op
def hello(name: str):
    print(f"Hello, {name}!")


# @op(config_schema={"location": String})
# def hello(context, name: str):
#     location = context.op_config["location"]
#     print(f"Hello, {name}! How is {location}?")


@graph
def hello_dagster():
    hello(get_name())


# job = hello_dagster.to_job()


job = hello_dagster.to_job(config={"ops": {"get_name": {"config": {"name": "dagster"}}}})
# hello_chris = hello_dagster.to_job(config={"ops": {"get_name": {"config": {"name": "Chris"}}}})
# hello_emily = hello_dagster.to_job(config={"ops": {"get_name": {"config": {"name": "Emily"}}}})
