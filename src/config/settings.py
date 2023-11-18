from decouple import AutoConfig

config = AutoConfig(search_path="src/config")

MINIO_HOST: str = config("MINIO_HOST")
MINIO_PORT: str = config("MINIO_PORT")
MINIO_ENDPOINT: str = f"{MINIO_HOST}:{MINIO_PORT}"

MINIO_ROOT_USER: str = config("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD: str = config("MINIO_ROOT_PASSWORD")

MINIO_PENDING_ANNOTATIONS_BUCKET_NAME: str = config(
    "MINIO_PENDING_ANNOTATIONS_BUCKET_NAME"
)
MINIO_RAW_DATASET_BUCKET_NAME: str = config("MINIO_RAW_DATASET_BUCKET_NAME")
MINIO_DATASETS_BUCKET_NAME: str = config("MINIO_DATASETS_BUCKET_NAME")

PREFECT_API_URL: str = config("PREFECT_API_URL")
PREFECT_SERVER_API_HOST: str = config("PREFECT_SERVER_API_HOST")
PREFECT_API_DATABASE_CONNECTION_URL: str = config("PREFECT_API_DATABASE_CONNECTION_URL")
