from dagster import Nothing, String, graph, op


@op(config_schema={"name": String})
def get_name(context) -> String:
    return context.op_config["name"]


@op
def hello(context, name: String) -> Nothing:
    context.log.info(f"Hello, {name}!")


# @op(config_schema={"location": String})
# def hello(context, name: String) -> Nohthing:
#     location = context.op_config["location"]
#     context.log.info(f"Hello, {name}! How is {location}?")


@graph
def hello_dagster():
    hello(get_name())


# job = hello_dagster.to_job()


job = hello_dagster.to_job(config={"ops": {"get_name": {"config": {"name": "dagster"}}}})
# hello_chris = hello_dagster.to_job(config={"ops": {"get_name": {"config": {"name": "Chris"}}}})
# hello_emily = hello_dagster.to_job(config={"ops": {"get_name": {"config": {"name": "Emily"}}}})
