from src.config.settings import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from src.data.models.model_bucket_client import MinioClient
from src.data.models.model_dataset import HuggingFaceDataset

# 1
bucket_client = MinioClient(
    MINIO_ENDPOINT,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False,
)

# 2
# prepare_buckets_flow(
#     bucket_client=bucket_client,
#     bucket_names=get_bucket_names(),
#     enable_versioning=True
# )


# 3
dataset_list = [HuggingFaceDataset("kili-technology/plastic_in_river")]

# 4
# prepare_raw_datasets(bucket_client=bucket_client, dataset_list=dataset_list)
