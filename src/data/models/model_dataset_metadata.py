import json
from datetime import datetime
from enum import Enum
from typing import List, Optional


class DatasetSourceType(Enum):
    HUGGING_FACE = "HuggingFace"
    KAGGLE = "Kaggle"
    IMPORTED = "Imported"
    GENERATED = "Generated"
    OTHER = "Other"


class DatasetMetadata:
    def __init__(
        self,
        name: str,
        source: DatasetSourceType,
        creation_date: datetime,
        last_modified_date: datetime,
        size: Optional[int] = None,
        number_of_records: Optional[int] = None,
        description: Optional[str] = None,
        version: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ):
        """
        Initialize the DatasetMetadata object with dataset properties.
        """
        self.name = name
        self.source = source
        self.creation_date = creation_date
        self.last_modified_date = last_modified_date
        self.size = size
        self.number_of_records = number_of_records
        self.description = description
        self.version = version
        self.tags = tags

    def to_dict(self) -> dict:
        """
        Convert the dataset metadata to a dictionary.

        Returns:
            dict: Dictionary representation of the dataset metadata.
        """
        # Using vars(self) or self.__dict__ here to convert object attributes to a dictionary
        return {
            k: (v.isoformat() if isinstance(v, datetime) else v)
            for k, v in vars(self).items()
        }

    def serialize(self) -> str:
        """
        Serialize the dataset metadata to a JSON string.
        """
        return json.dumps(self.to_dict())

    def __str__(self):
        """
        String representation of the DatasetMetadata object.
        """
        return self.serialize()
