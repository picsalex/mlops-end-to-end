import os

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
    MINIO_CLIENT.fput_object(MINIO_BUCKET, data_path)


@flow(name="data_preparation")
def data_preparation(data_path: str):
    check_data_path(data_path)
    check_connection()
    check_bucket_exists()

    upload_data(data_path)
