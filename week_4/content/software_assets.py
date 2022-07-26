from dagster import asset


@asset
def a_asset():
    return 5


@asset(group_name="basic")
def b_asset():
    return 10


@asset
def c_asset(context, a_asset, b_asset):
    new_value = a_asset + b_asset
    context.log.info(f"New value: {new_value}")
    return new_value


@asset(group_name="complex")
def d_asset(context, c_asset):
    new_value = c_asset * 2
    context.log.info(f"New value: {new_value}")
    return new_value
