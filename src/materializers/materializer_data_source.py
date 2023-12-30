# Assuming this is in a file like src/materializers/data_source_materializer.py

import json
import os
from typing import Type

from zenml.enums import ArtifactType
from zenml.io import fileio
from zenml.materializers.base_materializer import BaseMaterializer

from src.models.model_data_source import (
    LocalDataSource,
    HuggingFaceDataSource,
    DataSourceList,
)


class DataSourceMaterializer(BaseMaterializer):
    ASSOCIATED_TYPES = (DataSourceList,)
    ASSOCIATED_ARTIFACT_TYPE = ArtifactType.DATA

    def save(self, data_source_list: DataSourceList) -> None:
        """Serialize a DataSourceList object."""
        serialized_data_sources = []
        for data_source in data_source_list.data_sources:
            data_source_info = {
                "class": data_source.__class__.__name__,
                "root_folder_path": data_source.root_folder_path,
                "object_folder_path": data_source.object_folder_path,
                "annotation_folder_path": data_source.annotation_folder_path,
                "uuid": data_source.uuid,
            }

            if isinstance(data_source, HuggingFaceDataSource):
                data_source_info["dataset_name"] = data_source.dataset_name
                data_source_info["api_token"] = data_source.api_token

            serialized_data_sources.append(data_source_info)

        data_path = os.path.join(self.uri, "data_sources_config.json")
        with fileio.open(data_path, "w") as f:
            json.dump(serialized_data_sources, f)

    def load(self, data_type: Type[DataSourceList]) -> DataSourceList:
        """Deserialize a DataSourceList object."""
        data_path = os.path.join(self.uri, "data_sources_config.json")
        with fileio.open(data_path, "r") as f:
            serialized_data_sources = json.load(f)

        data_sources = []
        for data_source_info in serialized_data_sources:
            if data_source_info["class"] == "HuggingFaceDataSource":
                data_source = HuggingFaceDataSource(
                    dataset_name=data_source_info["dataset_name"],
                    api_token=data_source_info.get("api_token"),
                )
            elif data_source_info["class"] == "LocalDataSource":
                data_source = LocalDataSource(
                    root_folder_path=data_source_info["root_folder_path"],
                    object_folder_path=data_source_info["object_folder_path"],
                    annotation_folder_path=data_source_info["annotation_folder_path"],
                )
            else:
                raise ValueError(
                    f"Unknown DataSource class: {data_source_info['class']}"
                )

            data_sources.append(data_source)

        return DataSourceList(data_sources)
