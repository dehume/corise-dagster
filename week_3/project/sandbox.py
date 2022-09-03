def return_list(key):
    list = {"resources": {"s3": {"config": {"bucket": "dagster","access_key": "test","secret_key": "test","endpoint_url": "http://host.docker.internal:4566",}},"redis": {"config": {"host": "redis","port": 6379,}},},"ops": {"get_s3_data": {"config": {"s3_key": key}}},}
    return list

print(return_list('fraser'))