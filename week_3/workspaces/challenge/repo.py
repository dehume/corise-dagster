from dagster import repository
from workspaces.challenge.week_3_challenge import week_3_challenge_docker


@repository
def repo():
    return [week_3_challenge_docker]
