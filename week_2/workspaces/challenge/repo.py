from dagster import repository
from workspaces.challenge.week_2_challenge import week_2_challenge_docker


@repository
def repo():
    return [week_2_challenge_docker]
