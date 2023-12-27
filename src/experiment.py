from src.config.settings import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from src.models import HuggingFaceDataSource
from src.models import MinioClient
from src.steps.data.prepare_datasets_step import create_dataset, DatasetFlowConfig

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
data_source_list = [HuggingFaceDataSource("kili-technology/plastic_in_river", "", "")]

# 4
# prepare_data_sources_flow(bucket_client=bucket_client, data_source_list=data_source_list)

# 5
dataset = create_dataset(
    DatasetFlowConfig(bucket_client=bucket_client, data_source_list=data_source_list)
)
