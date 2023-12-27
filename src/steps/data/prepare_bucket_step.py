from typing import List

from zenml import step
from zenml.logger import get_logger

from src.config.settings import (
    MINIO_PENDING_ANNOTATIONS_BUCKET_NAME,
    MINIO_PENDING_REVIEWS_BUCKET_NAME,
    MINIO_DATA_SOURCES_BUCKET_NAME,
    MINIO_DATASETS_BUCKET_NAME,
)
from src.models.model_bucket_client import BucketClient


def get_bucket_names() -> List[str]:
    """
    Retrieve a list of bucket names to be created the bucket client.

    Returns:
        List[str]: A list of bucket names for creation and management.
    """
    return [
        MINIO_PENDING_ANNOTATIONS_BUCKET_NAME,
        MINIO_PENDING_REVIEWS_BUCKET_NAME,
        MINIO_DATA_SOURCES_BUCKET_NAME,
        MINIO_DATASETS_BUCKET_NAME,
    ]


@step(name="Validate connection to bucket client")
def validate_bucket_connection(bucket_client: BucketClient) -> None:
    """
    Validate the connection to the bucket_client.

    Args:
        bucket_client (BucketClient): The bucket Client to test the connection.

    Raises:
        ConnectionError: If the connection to the bucket_client fails.
    """
    logger = get_logger(__name__)

    try:
        bucket_client.check_connection()
        logger.info("Connection to the bucket Client verified successfully.")
    except Exception as e:
        logger.error(
            f"Connection to the storage service failed: {type(e).__name__}: {e}"
        )
        raise


@step(name="Setup bucket")
def setup_bucket(
    bucket_client: BucketClient, bucket_name: str, enable_versioning: bool
) -> None:
    """
    Set up a bucket in the provided bucket_client. Creates the bucket if it does not exist and enables versioning if specified.

    Args:
        bucket_client (BucketClient): The bucket client used for bucket operations.
        bucket_name (str): The name of the bucket to create or configure.
        enable_versioning (bool): Flag to enable versioning on the bucket.
    """
    logger = get_logger(__name__)

    if not bucket_client.bucket_exists(bucket_name):
        bucket_client.make_bucket(bucket_name, enable_versioning)
        logger.info(
            f"Successfully created bucket {bucket_name} (versioning set to {enable_versioning})"
        )
    else:
        logger.warning(f"The bucket {bucket_name} already exists. Skipping.")


@step(name="Prepare Buckets")
def prepare_buckets(
    bucket_client: BucketClient, bucket_names: list[str], enable_versioning: bool
) -> None:
    """
    ZenML pipeline to prepare the buckets for data storage and management.
    This flow creates and configures buckets as needed and enables versioning.
    """
    validate_bucket_connection(bucket_client)

    for bucket_name in bucket_names:
        setup_bucket(
            bucket_client=bucket_client,
            bucket_name=bucket_name,
            enable_versioning=enable_versioning,
        )
