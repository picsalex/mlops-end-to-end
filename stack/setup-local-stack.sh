#!/usr/bin/env bash

set -Eeo pipefail
source src/config/.env


zenml artifact-store register s3_store --flavor=s3 --path=s3://zenml --key="$MINIO_ROOT_USER" --secret="$MINIO_ROOT_PASSWORD" --client_kwargs="{\"endpoint_url\": \"http://$MINIO_HOST:$MINIO_PORT\"}"
zenml experiment-tracker register local_mlflow_tracker  --flavor=mlflow --tracking_uri="$MLFLOW_TRACKING_URI" --tracking_username="$MLFLOW_TRACKING_USERNAME" --tracking_password="$MLFLOW_TRACKING_PASSWORD"
zenml model-deployer register local_mlflow_deployer  --flavor=mlflow
zenml stack register local_gitflow_stack \
    -a s3_store \
    -o default \
    -e local_mlflow_tracker \
    -d local_mlflow_deployer
zenml stack set local_gitflow_stack
