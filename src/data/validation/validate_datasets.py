from prefect import flow, task

from src.config.settings import (
    MINIO_RAW_DATASETS_BUCKET_NAME,
    MINIO_DATASETS_BUCKET_NAME,
    MINIO_PENDING_REVIEWS_BUCKET_NAME,
)
from src.data.models.model_bucket_client import BucketClient
from src.data.models.model_dataset import Dataset


def get_raw_dataset_bucket_name() -> str:
    """
    Retrieves the bucket name from the configuration.
    """
    return MINIO_RAW_DATASETS_BUCKET_NAME


def get_dataset_bucket_name() -> str:
    """
    Retrieves the bucket name from the configuration.
    """
    return MINIO_DATASETS_BUCKET_NAME


def get_pending_reviews_bucket_name() -> str:
    """
    Retrieves the bucket name from the configuration.
    """
    return MINIO_PENDING_REVIEWS_BUCKET_NAME


@task
def fetch_dataset_info(bucket_name):
    # Fetch dataset information from Minio
    # This should return a list of tuples (image_file, json_annotation_file)
    pass


@task
def validate_json_annotations(annotation_file):
    # Here you'll use Great Expectations to validate each JSON file
    # This could be a checkpoint that validates a given file against your expectations
    pass


@task
def verify_image_file_existence(image_file):
    # Verify if each image file exists in Minio
    pass


@task
def handle_non_compliant_data(results):
    # Handle non-compliant data based on validation results
    pass


@flow
def dataset_verification_flow(bucket_client: BucketClient, dataset: Dataset):
    """
    A flow to verify the integrity and compliance of an image dataset.

    Args:
        bucket_client (BucketClient): The bucket Client to interact with the bucket.
        dataset (Dataset): The dataset to verify.
    """
    # validate_bucket_connection(bucket_client=bucket_client)
    #
    # validation_results =
    # for image_file, annotation_file in dataset_info:
    #     # Validate JSON annotations
    #     json_validation = validate_json_annotations(annotation_file)
    #     validation_results.append(json_validation)
    #
    #     # Verify image file existence
    #     image_existence = verify_image_file_existence(image_file)
    #     validation_results.append(image_existence)

    # Handle non-compliant data
    # handle_non_compliant_data()
    pass
