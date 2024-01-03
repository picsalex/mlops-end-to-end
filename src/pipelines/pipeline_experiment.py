from zenml import pipeline

from src.steps.data.datalake_initializers import (
    minio_client_initializer,
)
from src.steps.data.dataset_preparators import dataset_retriever
from src.steps.training.model_trainers import model_trainer


@pipeline
def gitflow_experiment_pipeline():
    """Train and serve a new model if it performs better than the model
    currently served."""

    bucket_client = minio_client_initializer()
    # data_source_list = data_source_list_initializer()
    # dataset = dataset_creator(
    #     bucket_client=bucket_client, data_source_list=data_source_list
    # )

    dataset = dataset_retriever(bucket_client, "01HK6604GDQ9ZBZZSPVYJY8W6S")
    # data_extractor(dataset=dataset, bucket_client=bucket_client)

    model_trainer(dataset=dataset)
