import io
import json
from typing import Any, Optional

import tqdm
import urllib3
from PIL import Image
from pydantic import BaseModel
from zenml import step
from zenml.logger import get_logger

from src.config.settings import (
    MINIO_DATA_SOURCES_BUCKET_NAME,
    MINIO_DATASETS_BUCKET_NAME,
)
from src.models.model_bucket_client import BucketClient
from src.models.model_data_source import DataSource
from src.models.model_dataset import Dataset
from src.steps.data.prepare_bucket_step import validate_bucket_connection


class DatasetFlowConfig(BaseModel):
    bucket_client: BucketClient
    data_source_list: list[DataSource]

    class Config:
        arbitrary_types_allowed = True


def get_data_sources_bucket_name() -> str:
    """
    Retrieves the bucket name from the configuration.
    """
    return MINIO_DATA_SOURCES_BUCKET_NAME


def get_dataset_bucket_name() -> str:
    """
    Retrieves the bucket name from the configuration.
    """
    return MINIO_DATASETS_BUCKET_NAME


def is_annotation_file_valid(json_data: Any) -> bool:
    required_keys = {"label", "bbox", "image_path"}
    if not all(key in json_data for key in required_keys):
        return False

    if not json_data["label"] or not isinstance(json_data["label"], list):
        return False

    if not json_data["bbox"] or not isinstance(json_data["bbox"], list):
        return False

    for bbox in json_data["bbox"]:
        if not (
            isinstance(bbox, list)
            and len(bbox) == 4
            and all(isinstance(num, (int, float)) for num in bbox)
        ):
            return False

    return True


def is_image_file_valid(
    image_file_bucket_response: urllib3.response.HTTPResponse
) -> bool:
    try:
        image_data = io.BytesIO(image_file_bucket_response.data)
        with Image.open(image_data) as img:
            img.verify()
        return True
    except Exception:
        return False


def get_json_data_if_valid(
    annotation_file_bucket_response: urllib3.response.HTTPResponse
) -> Optional[dict]:
    logger = get_logger(__name__)

    try:
        json_data = json.loads(annotation_file_bucket_response.data.decode("utf-8"))

        if is_annotation_file_valid(json_data=json_data):
            return json_data
        else:
            return None
    except json.JSONDecodeError:
        logger.error(
            f"Invalid .json format for object {annotation_file_bucket_response.geturl()}"
        )
        return None
    except Exception as e:
        logger.error(
            f"Error while decoding object {annotation_file_bucket_response.geturl()}: {e}"
        )
        return None


def copy_object(
    bucket_client: BucketClient,
    source_bucket_name,
    source_object_name: str,
    destination_bucket_name: str,
    destination_object_name: str,
) -> None:
    bucket_client.copy_object(
        source_bucket_name=source_bucket_name,
        source_object_name=source_object_name,
        destination_bucket_name=destination_bucket_name,
        destination_object_name=destination_object_name,
    )


@step(name="Prepare the dataset inside the bucket")
def prepare_dataset(
    bucket_client: BucketClient,
    source_bucket_name: str,
    dataset: Dataset,
    data_source: DataSource,
):
    logger = get_logger(__name__)
    data_source_bucket_annotation_path = f"{data_source.name}/annotations/"

    for annotation_bucket_object in tqdm.tqdm(
        bucket_client.list_objects(
            bucket_name=source_bucket_name,
            prefix=data_source_bucket_annotation_path,
        )
    ):
        try:
            annotation_file_path = annotation_bucket_object.object_name

            if annotation_file_path.lower().endswith(".json"):
                annotation_file_bucket_response = bucket_client.get_object(
                    bucket_name=source_bucket_name,
                    object_name=annotation_bucket_object.object_name,
                )

                if annotation_json_data := get_json_data_if_valid(
                    annotation_file_bucket_response=annotation_file_bucket_response
                ):
                    image_file_path = annotation_json_data["image_path"]

                    image_file_bucket_response = bucket_client.get_object(
                        bucket_name=source_bucket_name, object_name=image_file_path
                    )

                    if is_image_file_valid(
                        image_file_bucket_response=image_file_bucket_response
                    ):
                        split_name = dataset.get_next_split()
                        copy_object(
                            bucket_client=bucket_client,
                            source_bucket_name=source_bucket_name,
                            source_object_name=annotation_file_path,
                            destination_bucket_name=dataset.bucket_name,
                            destination_object_name=dataset.format_bucket_annotation_path(
                                annotation_file_path=annotation_file_path,
                                split_name=split_name,
                            ),
                        )
                        copy_object(
                            bucket_client=bucket_client,
                            source_bucket_name=source_bucket_name,
                            source_object_name=image_file_path,
                            destination_bucket_name=dataset.bucket_name,
                            destination_object_name=dataset.format_bucket_image_path(
                                image_file_path=image_file_path, split_name=split_name
                            ),
                        )
        except Exception as e:
            logger.error(f"Error while retrieving bucket object: {e}")
            raise
        finally:
            annotation_file_bucket_response.close()
            annotation_file_bucket_response.release_conn()

            image_file_bucket_response.close()
            image_file_bucket_response.release_conn()


@step(name="Create dataset")
def create_dataset(config: DatasetFlowConfig) -> Dataset:
    bucket_client = config.bucket_client
    data_source_list = config.data_source_list
    dataset = Dataset(bucket_name=get_dataset_bucket_name())

    validate_bucket_connection(bucket_client=bucket_client)

    for data_source in data_source_list:
        prepare_dataset(
            bucket_client=bucket_client,
            source_bucket_name=get_data_sources_bucket_name(),
            dataset=dataset,
            data_source=data_source,
        )

    return dataset
