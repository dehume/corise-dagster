from dagster import Nothing, Out, Output, String, graph, op


@op(
    config_schema={"name": String},
    out={
        "capitalized": Out(is_required=False, dagster_type=String),
        "not_capitalized": Out(is_required=False, dagster_type=String),
    },
)
def get_name(context) -> String:
    name = context.op_config["name"]
    # Determine if name is already capitalized
    if name[0].isupper():
        yield Output(name, "capitalized")
    else:
        yield Output(name, "not_capitalized")


@op
def capitalize_name(name: String) -> String:
    return name.capitalize()


@op
def hello(context, name: String) -> Nothing:
    context.log.info(f"Hello, {name}!")


@graph
def hello_dagster():
    capitalized, not_capitalized = get_name()
    hello(capitalize_name(not_capitalized))
    hello(capitalized)


job = hello_dagster.to_job()
