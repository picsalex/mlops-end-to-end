from prefect import flow, task, get_run_logger

from src.config.settings import (
    MINIO_RAW_DATASETS_BUCKET_NAME,
)
from src.data.models.model_bucket_client import BucketClient
from src.data.models.model_dataset import Dataset
from src.data.preparation.prepare_bucket import validate_bucket_connection
from src.data.services.service_data_uploader import DataUploaderService


def get_raw_dataset_bucket_name() -> str:
    """
    Retrieves the bucket name from the configuration.
    """
    return MINIO_RAW_DATASETS_BUCKET_NAME


@task
def validate_dataset(dataset: Dataset) -> None:
    """
    Checks if the provided dataset is valid.
    """
    logger = get_run_logger()

    try:
        dataset.validate_dataset_path()
    except Exception as e:
        logger.error(f"The dataset is not valid: {type(e).__name__}: {e}")
        raise


@task
def upload_data(
    data_uploader_service: DataUploaderService, bucket_name: str, dataset: Dataset
) -> None:
    """
    Uploads data from the provided path to the bucket under a unique dataset name.
    """
    logger = get_run_logger()

    try:
        data_uploader_service.upload_data(bucket_name=bucket_name, dataset=dataset)
    except TypeError:
        logger.error(
            f"Couldn't upload the dataset, type {type(dataset).__name__} is not supported."
        )
        raise


@flow(name="Prepare raw datasets", validate_parameters=False)
def prepare_raw_datasets(
    bucket_client: BucketClient, dataset_list: list[Dataset]
) -> None:
    """
    Flow for preparing data, which includes validating the data path, checking the bucket connection,
    configuring the bucket, and uploading the data.
    """
    data_uploader_service = DataUploaderService(bucket_client)
    validate_bucket_connection(bucket_client=bucket_client)

    for dataset in dataset_list:
        validate_dataset(dataset=dataset)

        upload_data(
            data_uploader_service=data_uploader_service,
            bucket_name=get_raw_dataset_bucket_name(),
            dataset=dataset,
        )
