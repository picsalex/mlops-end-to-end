import hashlib
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import PIL.Image
import tqdm
from datasets import load_dataset

from src.models.model_bucket_client import BucketClient
from src.models.model_data_source import (
    LocalDataSource,
    HuggingFaceDataSource,
    DataSource,
)


class DataUploaderService:
    def __init__(self, bucket_client: BucketClient):
        self.bucket_client = bucket_client

    def upload_data(self, bucket_name: str, data_source: DataSource) -> None:
        """
        Uploads data from the given dataset to a specified bucket using the bucket client.
        The upload method varies depending on the dataset type.

        Args:
            bucket_name (str): Name of the bucket where the dataset will be uploaded.
            data_source (DataSource): Dataset object to be uploaded.
        """
        if isinstance(data_source, LocalDataSource):
            self._upload_imported_data_source(bucket_name, data_source)
        elif isinstance(data_source, HuggingFaceDataSource):
            self._upload_huggingface_data_source(bucket_name, data_source)
        else:
            raise TypeError(
                f"Unsupported data source's type: {type(data_source).__name__}"
            )

    def _upload_imported_data_source(
        self, bucket_name: str, data_source: LocalDataSource
    ) -> None:
        """
        Uploads a local dataset to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the dataset will be uploaded.
            data_source (LocalDataSource): A LocalDataset object to upload.
        """
        for current_directory, _, file_list in os.walk(data_source.root_folder_path):
            for file_name in file_list:
                file_path_on_disk = os.path.join(current_directory, file_name)
                relative_path = os.path.relpath(
                    file_path_on_disk, start=data_source.root_folder_path
                )
                bucket_object_path = os.path.join(data_source.name, relative_path)
                self.bucket_client.upload_file(
                    bucket_name,
                    bucket_object_path,
                    file_path_on_disk,
                    data_source.get_metadata().to_dict(),
                )

    def _upload_huggingface_data_source(
        self, bucket_name: str, data_source: HuggingFaceDataSource
    ) -> None:
        """
        Uploads a HuggingFace dataset to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the dataset will be uploaded.
            data_source (HuggingFaceDataSource): HuggingFaceDataSource object to be uploaded.
        """
        hf_data_source = load_dataset(data_source.dataset_name)

        max_workers = 10

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            total_items = sum(
                len(hf_data_source[split]) for split in hf_data_source.keys()
            )
            with tqdm.tqdm(
                total=total_items, desc="Scheduling uploads"
            ) as schedule_bar:
                for split in hf_data_source.keys():
                    for item in hf_data_source[split]:
                        future = executor.submit(
                            self._upload_task,
                            bucket_name,
                            data_source.name,
                            item,
                            data_source.get_metadata().to_dict(),
                        )
                        futures.append(future)

                        schedule_bar.update(1)

            for _ in tqdm.tqdm(
                as_completed(futures), total=len(futures), desc="Uploading files"
            ):
                pass

    def _upload_task(
        self,
        bucket_name: str,
        dataset_name: str,
        item: dict,
        metadata: dict | None = None,
    ):
        """
        Task to upload an image and its corresponding JSON to the bucket.

        Args:
            bucket_name (str): Name of the bucket.
            dataset_name (str): Name of the dataset.
            item (dict): An item from the dataset containing image and metadata.
            metadata (metadata: dict | None): The file's metadata.
        """
        unique_id = self._hash_image(item["image"])

        image_path = f"{dataset_name}/images/{unique_id}.png"
        self._upload_image(
            bucket_name=bucket_name,
            image_path=image_path,
            image=item["image"],
            metadata=metadata,
        )

        json_path = f"{dataset_name}/annotations/{unique_id}.json"
        item["litter"]["image_path"] = image_path

        self._upload_json(
            bucket_name=bucket_name,
            json_path=json_path,
            data=item["litter"],
            metadata=metadata,
        )

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

    def _upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        metadata: dict | None = None,
    ) -> None:
        """
        Uploads a file to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the image will be uploaded.
            object_name (str): Name of the bucket where the image will be uploaded.
            file_path (str): Path within the bucket where the image will be stored.
            metadata (PIL.Image): Image object to be uploaded.
        """
        self.bucket_client.upload_file(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
            metadata=metadata,
        )

    def _upload_image(
        self,
        bucket_name: str,
        image_path: str,
        image: PIL.Image,
        metadata: dict | None = None,
    ) -> None:
        """
        Uploads an image to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the image will be uploaded.
            image_path (str): Path within the bucket where the image will be stored.
            image (PIL.Image): Image object to be uploaded.
            metadata (metadata: dict | None): The image's metadata.
        """
        image_buffer = io.BytesIO()
        image.save(image_buffer, format="PNG")
        image_buffer.seek(0)
        self.bucket_client.upload_data(
            bucket_name=bucket_name,
            object_name=image_path,
            data=image_buffer,
            length=image_buffer.getbuffer().nbytes,
            metadata=metadata,
        )

    def _upload_json(
        self, bucket_name: str, json_path: str, data: dict, metadata: dict | None = None
    ) -> None:
        """
        Uploads a JSON file to a specified bucket.

        Args:
            bucket_name (str): Name of the bucket where the JSON file will be uploaded.
            json_path (str): Path within the bucket where the JSON file will be stored.
            data (dict): Data to be serialized to JSON and uploaded.
            metadata (metadata: dict | None): The json's metadata.
        """
        json_data = json.dumps(data)
        json_buffer = io.BytesIO(json_data.encode())
        self.bucket_client.upload_data(
            bucket_name=bucket_name,
            object_name=json_path,
            data=json_buffer,
            length=len(json_data),
            metadata=metadata,
        )
