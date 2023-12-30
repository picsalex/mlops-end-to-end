import json
import os
from typing import Type

from zenml.enums import ArtifactType
from zenml.io import fileio
from zenml.materializers.base_materializer import BaseMaterializer

from src.config.settings import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from src.models.model_bucket_client import MinioClient, BucketClient


class BucketClientMaterializer(BaseMaterializer):
    ASSOCIATED_TYPES = (BucketClient, MinioClient)
    ASSOCIATED_ARTIFACT_TYPE = ArtifactType.DATA

    def load(self, data_type: Type[BucketClient]) -> BucketClient:
        """Deserialize BucketClient object."""
        data_path = os.path.join(self.uri, "bucket_client_config.json")
        with fileio.open(data_path, "r") as f:
            config = json.load(f)

        if config["class"] == "MinioClient":
            return MinioClient(
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_ROOT_USER,
                secret_key=MINIO_ROOT_PASSWORD,
                secure=config["secure"],
            )
        else:
            raise NotImplementedError(
                f"Deserialization for {config['class']} not implemented"
            )

    def save(self, bucket_client: BucketClient) -> None:
        """Serialize BucketClient object."""
        if isinstance(bucket_client, MinioClient):
            config = {"class": "MinioClient", "secure": bucket_client.secure}
        else:
            raise NotImplementedError(
                f"Serialization for {type(bucket_client)} not implemented"
            )

        data_path = os.path.join(self.uri, "bucket_client_config.json")
        with fileio.open(data_path, "w") as f:
            json.dump(config, f)
