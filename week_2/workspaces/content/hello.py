from dagster import Nothing, String, graph, op


@op
def get_name() -> String:
    return "dagster"


@op
def capitalize_name(name: String) -> String:
    return name.capitalize()


@op
def hello(name: String) -> Nothing:
    print(f"Hello, {name}!")


@graph
def hello_dagster():
    hello(capitalize_name(get_name()))


job = hello_dagster.to_job(
    name="hello_dagster",
)
