#/bin/bash
set -x

ENDPOINT_URL='http://localhost:4566'

aws --endpoint-url=$ENDPOINT_URL s3 mb s3://dagster
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock.csv s3://dagster/prefix/stock.csv
