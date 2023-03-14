from dagster import asset, load_assets_from_current_module


@asset
def a_asset():
    return 5


@asset
def b_asset():
    return 10


@asset
def c_asset(context, a_asset, b_asset):
    new_value = a_asset + b_asset
    context.log.info(f"New value: {new_value}")
    return new_value


@asset
def d_asset(context, c_asset):
    new_value = c_asset * 2
    context.log.info(f"New value: {new_value}")
    return new_value


corise_assets = load_assets_from_current_module(
    group_name="corise",
)
