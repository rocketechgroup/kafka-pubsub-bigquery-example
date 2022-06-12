#!/bin/bash

TEMPLATE_FILE_GCS_LOCATION=gs://dataflow-templates/latest/PubSub_Subscription_to_BigQuery # for prod use a specific version not latest
JOB_NAME=pubsub-to-bigquery
PROJECT_ID=rocketech-de-pgcp-sandbox
REGION_NAME=europe-west2
NETWORK=private
SUBNETWORK=https://www.googleapis.com/compute/v1/projects/rocketech-de-pgcp-sandbox/regions/europe-west2/subnetworks/dataflow
MAX_WORKERS=2
BIGQUERY_TABLE=rocketech-de-pgcp-sandbox:kafka_to_bigquery.transactions_with_metadata
INPUT_SUBSCRIPTION=projects/rocketech-de-pgcp-sandbox/subscriptions/kafka-relay-sub

gcloud dataflow jobs run ${JOB_NAME} \
    --project=${PROJECT_ID} \
    --region=${REGION_NAME} \
    --network=${NETWORK} \
    --subnetwork=${SUBNETWORK} \
    --disable-public-ips \
    --gcs-location=${TEMPLATE_FILE_GCS_LOCATION} \
    --max-workers=${MAX_WORKERS} \
    --parameters ^~^inputSubscription=${INPUT_SUBSCRIPTION}~outputTableSpec=${BIGQUERY_TABLE}