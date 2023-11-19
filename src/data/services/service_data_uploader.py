import os

from datasets import load_dataset

from src.data.models.model_bucket_client import BucketClient
from src.data.models.model_dataset import ImportedDataset, HuggingFaceDataset, Dataset


class DataUploaderService:
    def __init__(self, bucket_client: BucketClient):
        self.bucket_client = bucket_client

    def upload_data(self, bucket_name: str, dataset: Dataset) -> None:
        """
        Uploads data from the given dataset to a bucket using the bucket client.
        The upload method varies depending on the dataset type.

        Args:
            bucket_name (str): The bucket where the dataset will be uploaded.
            dataset (Dataset): The dataset to upload.
        """
        if isinstance(dataset, ImportedDataset):
            self._upload_imported_dataset(bucket_name, dataset)
        elif isinstance(dataset, HuggingFaceDataset):
            self._upload_huggingface_dataset(bucket_name, dataset)
        else:
            raise TypeError(f"Unsupported dataset type {type(dataset).__name__}")

    def _upload_imported_dataset(self, bucket_name: str, dataset: Dataset) -> None:
        """
        Upload method for ImportedDataset.
        """
        data_path = dataset.path
        dataset_name = dataset.name

        for current_directory, _, file_list in os.walk(data_path):
            for file_name in file_list:
                file_path_on_disk = os.path.join(current_directory, file_name)
                path_relative_to_data = os.path.relpath(
                    file_path_on_disk, start=data_path
                )
                bucket_object_path = os.path.join(dataset_name, path_relative_to_data)
                self.bucket_client.upload_file(
                    bucket_name,
                    bucket_object_path,
                    file_path_on_disk,
                    dataset.get_metadata().to_dict(),
                )

    def _upload_huggingface_dataset(self, bucket_name: str, dataset: Dataset) -> None:
        """
        Upload method for HuggingFaceDataset.
        """
        dataset_name = dataset.name

        hf_dataset = load_dataset(dataset_name)
        hf_dataset.save_to_disk(
            f"{self.bucket_client.get_fs_url_prefix()}/{bucket_name}/{dataset_name}",
            storage_options=self.bucket_client.get_fs_storage_option(),
        )
