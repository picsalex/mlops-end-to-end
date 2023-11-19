from src.config.settings import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from src.data.models.model_bucket_client import MinioClient
from src.data.models.model_dataset import HuggingFaceDataset
from src.data.preparation.prepare_raw_datasets import prepare_raw_datasets

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

# def retrieve_dataset_from_minio(minio_client, bucket_name, dataset_name):
#     """
#     Browse a dataset in MinIO and retrieve both images and their associated JSON files.
#
#     Args:
#         minio_client (Minio): A Minio client object.
#         bucket_name (str): The name of the MinIO bucket.
#         dataset_name (str): The name of the dataset.
#     """
#     objects = minio_client.list_objects(bucket_name, prefix=f"{dataset_name}/")
#
#     for obj in objects:
#         # Check if the object is an image or a JSON file
#         if obj.object_name.endswith('.png'):
#             image_path = obj.object_name
#             json_path = image_path.replace('/images/', '/annotations/').replace('.png', '.json')
#
#             # Download the image
#             image_data = minio_client.get_object(bucket_name, image_path).read()
#             # Process the image as needed...
#
#             # Download the associated JSON file
#             json_data = minio_client.get_object(bucket_name, json_path).read().decode('utf-8')
#             json_content = json.loads(json_data)
#             # Process the JSON data as needed...
#
#             # Example processing - printing the paths
#             print(f"Image: {image_path}, JSON: {json_path}")


# retrieve_dataset_from_minio(bucket_client.client, bucket_name="raw-datasets",
#                             dataset_name=f"{dataset_list[0].name}/images")
# 4
prepare_raw_datasets(bucket_client=bucket_client, dataset_list=dataset_list)
