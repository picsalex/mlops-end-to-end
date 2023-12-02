import random
from typing import Optional

import ulid


class Dataset:
    def __init__(
        self,
        bucket_name,
        seed: str = "e2e",
        annotations_path: str = "annotations",
        images_path: str = "images",
        distribution_weights: Optional[list[float]] = None,
    ):
        if distribution_weights is None:
            distribution_weights = [0.6, 0.2, 0.2]

        self.uuid = self.get_data_source_uuid()
        self.bucket_name = bucket_name
        self.annotations_path = annotations_path
        self.images_path = images_path
        self.distribution_weights = distribution_weights
        self.random_instance = random.Random(seed)

    def format_bucket_image_path(self, image_file_path: str, split_name: str) -> str:
        """
        Formats the bucket path for an image file based on the dataset's UUID and folder distribution.

        Args:
            image_file_path (str): The original path of the image file.
            split_name (str): The name of the split (train, test, or validation) the file will go into.

        Returns:
            str: A formatted bucket path for the image file.
        """
        image_filename = image_file_path.split("/")[-1]
        return f"{self.uuid}/{split_name}/{self.images_path}/{image_filename}"

    def format_bucket_annotation_path(
        self, annotation_file_path: str, split_name: str
    ) -> str:
        """
        Formats the bucket path for an annotation file based on the dataset's UUID and folder distribution.

        Args:
            annotation_file_path (str): The original path of the annotation file.
            split_name (str): The name of the split (train, test, or validation) the file will go into.

        Returns:
            str: A formatted bucket path for the annotation file.
        """
        annotation_filename = annotation_file_path.split("/")[-1]
        return f"{self.uuid}/{split_name}/{self.annotations_path}/{annotation_filename}"

    @staticmethod
    def get_data_source_uuid() -> str:
        """
        Generates a new ULID (Universally Unique Lexicographically Sortable Identifier) for the dataset.

        Returns:
            str: A ULID string.
        """
        return str(ulid.new())

    def get_next_split(self) -> str:
        """
        Randomly selects a folder ("train", "test", or "validation") based on the specified distribution weights.

        Returns:
            str: The name of the selected folder.
        """
        folders = ["train", "test", "validation"]
        return self.random_instance.choices(folders, self.distribution_weights)[0]
