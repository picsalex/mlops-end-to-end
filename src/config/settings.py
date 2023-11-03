from decouple import config
from minio import Minio

MINIO_HOST: str = config("MINIO_HOST")
MINIO_PORT: str = config("MINIO_PORT")
MINIO_ENDPOINT: str = f"{MINIO_HOST}:{MINIO_PORT}"

ACCESS_KEY: str = config("ACCESS_KEY")
SECRET_KEY: str = config("SECRET_KEY")

MINIO_BUCKET: str = config("MINIO_BUCKET")

MINIO_CLIENT = Minio(
    MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
)
