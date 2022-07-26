#/bin/bash
set -x

ENDPOINT_URL='http://localhost:4566'

aws --endpoint-url=$ENDPOINT_URL s3 mb s3://dagster
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_1.csv s3://dagster/prefix/stock_1.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_2.csv s3://dagster/prefix/stock_2.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_3.csv s3://dagster/prefix/stock_3.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_4.csv s3://dagster/prefix/stock_4.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_5.csv s3://dagster/prefix/stock_5.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_6.csv s3://dagster/prefix/stock_6.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_7.csv s3://dagster/prefix/stock_7.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_8.csv s3://dagster/prefix/stock_8.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_9.csv s3://dagster/prefix/stock_9.csv
aws --endpoint-url=$ENDPOINT_URL s3 cp data/stock_10.csv s3://dagster/prefix/stock_10.csv