import os

import ulid
from prefect import flow, get_run_logger, task

from src.config.settings import (
    MINIO_BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
)
from src.data.bucket_client import BucketClient, MinioClient


def get_bucket_client() -> BucketClient:
    """
    Retrieves a MinioClient instance with the provided credentials.
    """
    return MinioClient(
        MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )


def get_bucket_name() -> str:
    """
    Retrieves the bucket name from the configuration.
    """
    return MINIO_BUCKET_NAME


def generate_dataset_name() -> str:
    """
    Generates a unique dataset name using ULID.
    """
    return str(ulid.new())


def run_data_preparation(data_path: str) -> None:
    """
    Executes the data preparation flow with injected dependencies.
    """
    minio_client = get_bucket_client()
    bucket_name = get_bucket_name()

    data_preparation_flow(
        data_path=data_path, bucket_client=minio_client, bucket_name=bucket_name
    )


@task
def validate_data_path_exists(data_path: str) -> None:
    """
    Checks if the provided data_path exists and is not empty.
    """
    if not os.path.isdir(data_path):
        raise FileNotFoundError(
            f"The provided data path at {data_path} does not exist."
        )
    if not os.listdir(data_path):
        raise FileNotFoundError(f"The provided data path at {data_path} is empty.")


@task
def check_bucket_connection(bucket_client: BucketClient) -> None:
    """
    Validates the connection to the bucket using the provided client.
    """
    logger = get_run_logger()
    try:
        bucket_client.check_connection()
        logger.info("Connection to the bucket verified successfully.")
    except Exception as e:
        logger.error(f"Connection to the bucket failed: {e}")
        raise


@task
def configure_bucket(bucket_client: BucketClient, bucket_name: str) -> None:
    """
    Configures the bucket, creating it if it does not exist.
    """
    if not bucket_client.bucket_exists(bucket_name):
        bucket_client.make_bucket(bucket_name)


@task
def upload_data(
    bucket_client: BucketClient, bucket_name: str, data_path: str, dataset_name: str
) -> None:
    """
    Uploads data from the provided path to the bucket under a unique dataset name.
    """
    for current_directory, _, file_list in os.walk(data_path):
        for file_name in file_list:
            file_path_on_disk = os.path.join(current_directory, file_name)
            path_relative_to_data = os.path.relpath(file_path_on_disk, start=data_path)
            bucket_object_path = os.path.join(dataset_name, path_relative_to_data)
            bucket_client.upload_file(
                bucket_name, bucket_object_path, file_path_on_disk
            )


@flow(name="data_preparation")
def data_preparation_flow(
    data_path: str, bucket_client: BucketClient, bucket_name: str
) -> None:
    """
    Flow for preparing data, which includes validating the data path, checking the bucket connection,
    configuring the bucket, and uploading the data.
    """
    validate_data_path_exists(data_path=data_path)
    check_bucket_connection(bucket_client=bucket_client)
    configure_bucket(bucket_client=bucket_client, bucket_name=bucket_name)

    dataset_name = generate_dataset_name()
    upload_data(
        bucket_client=bucket_client,
        bucket_name=bucket_name,
        data_path=data_path,
        dataset_name=dataset_name,
    )
