from dagster import build_init_resource_context
from dagster_ucr.resources import S3, Redis, redis_resource, s3_resource


def test_s3_resource():
    resource = s3_resource(
        build_init_resource_context(
            config={
                "bucket": "test",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localhost:4566",
            }
        )
    )
    assert type(resource) is S3


def test_redis_resource():
    resource = redis_resource(
        build_init_resource_context(
            config={
                "host": "test",
                "port": 6379,
            }
        )
    )
    assert type(resource) is Redis
