import os
from abc import ABC, abstractmethod
from datetime import datetime

import requests
import ulid

from src.data.models.model_dataset_metadata import DatasetMetadata, DatasetSourceType


class Dataset(ABC):
    def __init__(self, path: str):
        self.path = path
        self.name = Dataset.get_dataset_name()

    @abstractmethod
    def validate_dataset_path(self) -> None:
        pass

    @abstractmethod
    def get_metadata(self) -> DatasetMetadata:
        pass

    @staticmethod
    def get_dataset_name() -> str:
        return str(ulid.new())


class ImportedDataset(Dataset):
    def validate_dataset_path(self) -> None:
        """
        Validate an imported dataset's path.
        Checks if the provided path exists and is a directory. Raises an exception if not.

        Raises:
            FileNotFoundError: If the dataset path does not exist.
            NotADirectoryError: If the dataset path exists but is not a directory.
        """
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"The dataset path '{self.path}' does not exist.")

        if not os.path.isdir(self.path):
            raise NotADirectoryError(f"The path '{self.path}' is not a directory.")

    def get_metadata(self) -> DatasetMetadata:
        """
        Retrieve metadata information for the HuggingFace dataset.

        Returns:
            DatasetMetadata: An object containing metadata for the dataset.
        """
        return DatasetMetadata(
            name=self.get_dataset_name(),
            source=DatasetSourceType.IMPORTED,
            creation_date=datetime.now(),
            last_modified_date=datetime.now(),
        )


class HuggingFaceDataset(Dataset):
    def __init__(self, path: str, api_token: str = None):
        super().__init__(path)
        self.api_token = api_token

    def validate_dataset_path(self) -> None:
        """
        Check if the dataset identifier exists in HuggingFace's dataset registry.
        Raises specific exceptions based on the dataset's availability and access requirements.

        Raises:
            ValueError: If the dataset is not valid or not found in HuggingFace's registry.
            PermissionError: If the dataset is gated and requires a private API token.
            FileNotFoundError: If there is an issue with the request or the dataset is not found.
        """
        headers = {}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        api_url = f"https://datasets-server.huggingface.co/is-valid?dataset={self.path}"

        try:
            response = requests.get(api_url, headers=headers)

            if response.status_code == 401:
                raise PermissionError(
                    "The dataset is gated and requires a private API token for access."
                )
            elif response.status_code == 404:
                raise FileNotFoundError(
                    "There is an issue with the request, or the dataset is not found on HuggingFace."
                )
            elif not response.ok:
                raise ValueError(
                    f"An error occurred with the request. Status code: {response.status_code}"
                )

            data = response.json()
            if not (data.get("viewer", False) or data.get("preview", False)):
                raise ValueError(
                    f"The dataset '{self.path}' is not valid or not available on HuggingFace."
                )
        except requests.RequestException as e:
            raise ConnectionError(f"Failed to connect to HuggingFace server: {e}")

    def get_metadata(self) -> DatasetMetadata:
        """
        Retrieve metadata information for the HuggingFace dataset.

        Returns:
            DatasetMetadata: An object containing metadata for the dataset.
        """
        return DatasetMetadata(
            name=self.path,
            source=DatasetSourceType.HUGGING_FACE,
            creation_date=datetime.now(),
            last_modified_date=datetime.now(),
        )
