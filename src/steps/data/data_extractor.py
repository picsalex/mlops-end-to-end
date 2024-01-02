from zenml import step

from src.models.model_bucket_client import BucketClient
from src.models.model_dataset import Dataset


@step(name="Extract the data from the bucket client")
def data_extractor(
    dataset: Dataset, bucket_client: BucketClient, destination_path: str = "datasets/"
) -> None:
    dataset.download(
        bucket_client=bucket_client, destination_root_path=destination_path
    )
