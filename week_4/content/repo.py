from content.etl import create_table_docker, insert_into_table_docker
from content.software_assets import a_asset, b_asset, c_asset, d_asset
from dagster import repository


@repository
def repo():
    return [a_asset, b_asset, c_asset, d_asset, create_table_docker, insert_into_table_docker]
