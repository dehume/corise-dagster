import workspaces.config as con
from dagster import build_init_resource_context
from workspaces.resources import S3, Redis, redis_resource, s3_resource


def test_s3_resource():
    resource = s3_resource(build_init_resource_context(config=con.S3))
    assert type(resource) is S3


def test_redis_resource():
    resource = redis_resource(build_init_resource_context(config=con.REDIS))
    assert type(resource) is Redis
