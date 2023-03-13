DBT_PROJECT_PATH = "/opt/dagster/dagster_home/dbt_test_project/."

POSTGRES = {
    "host": "postgresql",
    "user": "postgres_user",
    "password": "postgres_password",
    "database": "postgres_db",
}

DBT = {
    "project_dir": DBT_PROJECT_PATH,
    "profiles_dir": DBT_PROJECT_PATH,
    "ignore_handled_error": True,
    "target": "test",
    "target_path": "/opt/dagster/dagster_home/target",
}

S3 = {
    "bucket": "dagster",
    "access_key": "test",
    "secret_key": "test",
    "endpoint_url": "http://localstack:4566",
}

REDIS = {
    "host": "redis",
    "port": 6379,
}

S3_FILE = "prefix/stock.csv"
ANALYTICS_TABLE = "analytics.dbt_table"
