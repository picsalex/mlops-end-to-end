import hashlib
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import PIL.Image
import tqdm
from datasets import load_dataset

from src.data.models.model_bucket_client import BucketClient
from src.data.models.model_dataset import ImportedDataset, HuggingFaceDataset, Dataset


class DataUploaderService:
    def __init__(self, bucket_client: BucketClient):
        self.bucket_client = bucket_client

    def upload_data(self, bucket_name: str, dataset: Dataset) -> None:
        """
        Uploads data from the given dataset to a specified bucket using the bucket client.
        The upload method varies depending on the dataset type.

        Args:
            bucket_name (str): Name of the bucket where the dataset will be uploaded.
            dataset (Dataset): Dataset object to be uploaded.
        """
        if isinstance(dataset, ImportedDataset):
            self._upload_imported_dataset(bucket_name, dataset)
        elif isinstance(dataset, HuggingFaceDataset):
            self._upload_huggingface_dataset(bucket_name, dataset)
        else:
            raise TypeError(f"Unsupported dataset type: {type(dataset).__name__}")

    def _upload_imported_dataset(
        self, bucket_name: str, dataset: ImportedDataset
    ) -> None:
        """
        Uploads an imported dataset to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the dataset will be uploaded.
            dataset (ImportedDataset): ImportedDataset object to be uploaded.
        """
        for current_directory, _, file_list in os.walk(dataset.path):
            for file_name in file_list:
                file_path_on_disk = os.path.join(current_directory, file_name)
                relative_path = os.path.relpath(file_path_on_disk, start=dataset.path)
                bucket_object_path = os.path.join(dataset.name, relative_path)
                self.bucket_client.upload_file(
                    bucket_name,
                    bucket_object_path,
                    file_path_on_disk,
                    dataset.get_metadata().to_dict(),
                )

    def _upload_huggingface_dataset(
        self, bucket_name: str, dataset: HuggingFaceDataset
    ) -> None:
        """
        Uploads a HuggingFace dataset to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the dataset will be uploaded.
            dataset (HuggingFaceDataset): HuggingFaceDataset object to be uploaded.
        """
        hf_dataset = load_dataset(dataset.name)

        max_workers = 10  # Adjust based on your environment

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            total_items = sum(len(hf_dataset[split]) for split in hf_dataset.keys())
            with tqdm.tqdm(
                total=total_items, desc="Scheduling uploads"
            ) as schedule_bar:
                for split in hf_dataset.keys():
                    for item in hf_dataset[split]:
                        # Schedule the upload task
                        future = executor.submit(
                            self._upload_task, bucket_name, dataset.name, item
                        )
                        futures.append(future)

                        schedule_bar.update(1)

            for _ in tqdm.tqdm(
                as_completed(futures), total=len(futures), desc="Uploading files"
            ):
                pass

    def _upload_task(self, bucket_name: str, dataset_name: str, item):
        """
        Task to upload an image and its corresponding JSON to the bucket.

        Args:
            bucket_name (str): Name of the bucket.
            dataset_name (str): Name of the dataset.
            item (dict): An item from the dataset containing image and metadata.
        """
        unique_id = self._hash_image(item["image"])

        image_path = f"{dataset_name}/images/{unique_id}.png"
        self._upload_image(bucket_name, image_path, item["image"])

        json_path = f"{dataset_name}/annotations/{unique_id}.json"
        self._upload_json(bucket_name, json_path, item["litter"])

    @staticmethod
    def _hash_image(image: PIL.Image) -> str:
        """
        Generates a SHA-256 hash for a given image.

        Args:
            image (PIL.Image): Image object to be hashed.

        Returns:
            str: Hexadecimal hash of the image.
        """
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format=image.format)
        img_byte_arr = img_byte_arr.getvalue()

        hasher = hashlib.sha256()
        hasher.update(img_byte_arr)
        return hasher.hexdigest()

    def _upload_image(
        self, bucket_name: str, image_path: str, image: PIL.Image
    ) -> None:
        """
        Uploads an image to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the image will be uploaded.
            image_path (str): Path within the bucket where the image will be stored.
            image (PIL.Image): Image object to be uploaded.
        """
        image_buffer = io.BytesIO()
        image.save(image_buffer, format="PNG")
        image_buffer.seek(0)
        self.bucket_client.upload_data(
            bucket_name,
            image_path,
            image_buffer,
            length=image_buffer.getbuffer().nbytes,
        )

    def _upload_json(self, bucket_name: str, json_path: str, data: dict) -> None:
        """
        Uploads a JSON file to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the JSON file will be uploaded.
            json_path (str): Path within the bucket where the JSON file will be stored.
            data (dict): Data to be serialized to JSON and uploaded.
        """
        json_data = json.dumps(data)
        json_buffer = io.BytesIO(json_data.encode())
        self.bucket_client.upload_data(
            bucket_name, json_path, json_buffer, length=len(json_data)
        )
