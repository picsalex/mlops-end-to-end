#!/usr/bin/env bash

zenml stack set default
zenml artifact-store delete s3_store
zenml experiment-tracker delete local_mlflow_tracker
zenml model-deployer delete local_mlflow_deployer
zenml stack delete local_gitflow_stack

zenml stack set local_gitflow_stack

sh ./stack/setup-local-stack.sh
