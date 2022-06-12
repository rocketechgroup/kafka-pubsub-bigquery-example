#!/bin/bash

gcloud config set project $1
bq mk --location EU --dataset kafka_to_bigquery
bq mk --table \
--schema sample_schema.json \
--time_partitioning_field transaction_time \
kafka_to_bigquery.transactions_with_metadata