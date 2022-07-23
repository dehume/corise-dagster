import boto3


def get_s3_keys(bucket: str, prefix: str = "", endpoint_url: str = None, since_key: str = None, max_keys: int = 1000):
    """Get S3 keys"""
    config = {"service_name": "s3"}
    if endpoint_url:
        config["endpoint_url"] = endpoint_url

    client = boto3.client(**config)

    cursor = ""
    contents = []

    while True:
        response = client.list_objects_v2(
            Bucket=bucket,
            Delimiter="",
            MaxKeys=max_keys,
            Prefix=prefix,
            StartAfter=cursor,
        )
        contents.extend(response.get("Contents", []))
        if response["KeyCount"] < max_keys:
            break

        cursor = response["Contents"][-1]["Key"]

    sorted_keys = [obj["Key"] for obj in sorted(contents, key=lambda x: x["LastModified"])]

    if not since_key or since_key not in sorted_keys:
        return sorted_keys

    for idx, key in enumerate(sorted_keys):
        if key == since_key:
            return sorted_keys[idx + 1 :]

    return []
