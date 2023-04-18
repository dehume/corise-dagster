import random

from dagster import AssetMaterialization, String, graph, op

FRUIT = ["apple", "orange", "lime", "lemon"]


@op
def random_asset(context):
    context.log_event(
        AssetMaterialization(
            asset_key="my_asset",
            description="Recording a random number and random fruit",
            metadata={"random_number": random.randint(0, 10), "random_fruit": random.choice(FRUIT)},
        )
    )


@graph
def asset_graph():
    random_asset()


asset_job = asset_graph.to_job()