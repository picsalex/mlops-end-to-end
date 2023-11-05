from decouple import AutoConfig
from minio import Minio

config = AutoConfig(search_path="src/config")

MINIO_HOST: str = config("MINIO_HOST")
MINIO_PORT: str = config("MINIO_PORT")
MINIO_ENDPOINT: str = f"{MINIO_HOST}:{MINIO_PORT}"

MINIO_ROOT_USER: str = config("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD: str = config("MINIO_ROOT_PASSWORD")

MINIO_BUCKET: str = config("MINIO_BUCKET")

MINIO_CLIENT = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False,
)
