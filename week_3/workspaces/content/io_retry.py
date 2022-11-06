from random import randint

from dagster import RetryPolicy, String, fs_io_manager, graph, op


@op
def time_consuming_step() -> String:
    return "dagster"


@op
def unreliable_step(name: String):
    if randint(0, 1) == 1:
        raise Exception("Flaky op")
    print(f"Hello, {name}!")


@graph
def hello_dagster():
    unreliable_step(time_consuming_step())


job_local_io_manager = hello_dagster.to_job(
    name="hello_local_io_manager",
    resource_defs={"io_manager": fs_io_manager},
)


job_local_io_manager_retry = hello_dagster.to_job(
    name="hello_local_io_manager_retry",
    resource_defs={"io_manager": fs_io_manager},
    op_retry_policy=RetryPolicy(max_retries=10),
)


# @op(retry_policy=RetryPolicy(max_retries=5, delay=0.2), out=Out(io_manager_key="fs_io"))
# def time_consuming_step() -> String:
#     return "dagster"


# @op(out=Out(io_manager_key="s3_io"))
# def unreliable_step(name: String):
#     if randint(0, 1) == 1:
#         raise Exception("Flaky op")
#     print(f"Hello, {name}!")


# @graph
# def hello_dagster():
#     unreliable_step(time_consuming_step())


# quiz = hello_dagster.to_job(
#     name="hello_local_io_manager",
#     resource_defs={
#         "fs_io": fs_io_manager,
#         "s3_io": s3_pickle_io_manager,
#     },
#     op_retry_policy=RetryPolicy(max_retries=3),
# )
