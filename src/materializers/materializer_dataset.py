import json
import os
from typing import Type

from zenml.enums import ArtifactType
from zenml.io import fileio
from zenml.materializers.base_materializer import BaseMaterializer

from src.models.model_dataset import Dataset


class DatasetMaterializer(BaseMaterializer):
    ASSOCIATED_TYPES = (Dataset,)
    ASSOCIATED_ARTIFACT_TYPE = ArtifactType.DATA

    def save(self, dataset: Dataset) -> None:
        """Serialize the Dataset object."""
        serialized_dataset = {
            "uuid": dataset.uuid,
            "seed": dataset.seed,
            "bucket_name": dataset.bucket_name,
            "annotations_path": dataset.annotations_path,
            "images_path": dataset.images_path,
            "distribution_weights": dataset.distribution_weights,
        }

        data_path = os.path.join(self.uri, "dataset_config.json")
        with fileio.open(data_path, "w") as f:
            json.dump(serialized_dataset, f)

    def load(self, data_type: Type[Dataset]) -> Dataset:
        """Deserialize the Dataset object."""
        data_path = os.path.join(self.uri, "dataset_config.json")
        with fileio.open(data_path, "r") as f:
            serialized_dataset = json.load(f)

        dataset = Dataset(
            bucket_name=serialized_dataset["bucket_name"],
            seed=serialized_dataset["seed"],
            annotations_path=serialized_dataset["annotations_path"],
            images_path=serialized_dataset["images_path"],
            distribution_weights=serialized_dataset["distribution_weights"],
        )
        dataset.uuid = serialized_dataset["uuid"]  # Manually setting the uuid

        return dataset
