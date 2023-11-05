import os

import ulid
from prefect import flow, task

from src.config.settings import MINIO_BUCKET, MINIO_CLIENT


def check_data_path(data_path: str):
    if not os.path.isdir(data_path):
        raise FileNotFoundError(
            f"The provided data_path at {data_path} does not exists."
        )

    if os.listdir(data_path) == 0:
        raise FileNotFoundError(f"The provided data_path at {data_path} is empty.")


@task
def check_connection():
    return True


@task
def check_bucket_exists():
    if not MINIO_CLIENT.bucket_exists(MINIO_BUCKET):
        MINIO_CLIENT.make_bucket(MINIO_BUCKET)


@task
def upload_data(data_path: str):
    # Generate a ULID for the dataset
    dataset_name = str(ulid.new())

    for root, dirs, files in os.walk(data_path):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, start=data_path)
            destination_path = os.path.join(dataset_name, relative_path)
            MINIO_CLIENT.fput_object(MINIO_BUCKET, destination_path, local_path)


@flow(name="data_preparation")
def data_preparation(data_path: str):
    check_data_path(data_path)
    check_connection()
    check_bucket_exists()

    upload_data(data_path)
