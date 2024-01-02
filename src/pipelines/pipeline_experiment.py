from zenml import pipeline

from src.steps.data.datalake_initializers import (
    minio_client_initializer,
    data_source_list_initializer,
)
from src.steps.training.train_model_step import decision_tree_trainer


@pipeline
def gitflow_experiment_pipeline():
    """Train and serve a new model if it performs better than the model
    currently served."""

    _ = minio_client_initializer()
    _ = data_source_list_initializer()

    # _ = dataset_creator(bucket_client=bucket_client, data_source_list=data_source_list)
    decision_tree_trainer()
