from dagster import (
    AssetSelection,
    FreshnessPolicy,
    FreshnessPolicySensorContext,
    asset,
    freshness_policy_sensor,
)


@asset(freshness_policy=FreshnessPolicy(maximum_lag_minutes=2))
def fresh_asset_a():
    return 5


@asset(freshness_policy=FreshnessPolicy(maximum_lag_minutes=1))
def fresh_asset_b():
    return 10


@asset(freshness_policy=FreshnessPolicy(maximum_lag_minutes=2))
def fresh_asset_c(context, fresh_asset_a, fresh_asset_b):
    new_value = fresh_asset_a + fresh_asset_b
    context.log.info(f"New value: {new_value}")
    return new_value


@asset(freshness_policy=FreshnessPolicy(maximum_lag_minutes=1))
def fresh_asset_d(context, fresh_asset_c):
    new_value = fresh_asset_c * 2
    context.log.info(f"New value: {new_value}")
    return new_value


def slack_alert(message: str) -> None:
    print(message)


@freshness_policy_sensor(
    asset_selection=AssetSelection.all(),
)
def freshness_alerting_sensor(context: FreshnessPolicySensorContext):
    sla = 5

    if context.minutes_late is None or context.previous_minutes_late is None:
        return
    if context.minutes_late >= sla and context.previous_minutes_late < sla:
        slack_alert(f"Asset with key {context.asset_key} is now more than {sla} minutes late.")
    elif context.minutes_late == 0 and context.previous_minutes_late >= sla:
        slack_alert(f"Asset with key {context.asset_key} is now on time.")
