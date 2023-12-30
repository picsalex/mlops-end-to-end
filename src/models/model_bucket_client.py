from abc import ABC, abstractmethod
from typing import BinaryIO, Generator, Any

import urllib3
from minio import Minio, S3Error
from minio.commonconfig import ENABLED, CopySource
from minio.datatypes import Object
from minio.helpers import ObjectWriteResult
from minio.versioningconfig import VersioningConfig


class BucketClient(ABC):
    @abstractmethod
    def check_connection(self) -> None:
        pass

    @abstractmethod
    def bucket_exists(self, bucket_name: str) -> bool:
        pass

    @abstractmethod
    def make_bucket(self, bucket_name: str, enable_versioning: bool):
        pass

    @abstractmethod
    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        metadata: dict | None = None,
    ):
        pass

    @abstractmethod
    def upload_data(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        metadata: dict | None = None,
    ):
        pass

    @abstractmethod
    def list_objects(self, bucket_name: str, prefix: str = None):
        pass

    @abstractmethod
    def get_object(self, bucket_name: str, object_name: str):
        pass

    @abstractmethod
    def copy_object(
        self,
        source_bucket_name: str,
        source_object_name: str,
        destination_bucket_name: str,
        destination_object_name: str,
    ) -> ObjectWriteResult:
        pass


class MinioClient(BucketClient):
    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, secure: bool = False
    ):
        self.secure = secure

        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.secure,
        )

    def check_connection(self) -> None:
        try:
            self.client.list_buckets()
        except S3Error as e:
            raise e
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MinIO: {e}")

    def bucket_exists(self, bucket_name: str) -> bool:
        return self.client.bucket_exists(bucket_name)

    def make_bucket(self, bucket_name: str, enable_versioning: bool):
        self.client.make_bucket(bucket_name)
        if enable_versioning:
            self.client.set_bucket_versioning(
                bucket_name=bucket_name, config=VersioningConfig(ENABLED)
            )

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        metadata: dict | None = None,
    ):
        self.client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
            metadata=metadata,
        )

    def upload_data(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        metadata: dict | None = None,
    ):
        self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            metadata=metadata,
            length=length,
        )

    def list_objects(
        self, bucket_name: str, prefix: str = None
    ) -> Generator[Object, Any, None]:
        try:
            return self.client.list_objects(bucket_name=bucket_name, prefix=prefix)
        except S3Error as e:
            raise e

    def get_object(
        self, bucket_name: str, object_name: str
    ) -> urllib3.response.BaseHTTPResponse:
        try:
            return self.client.get_object(
                bucket_name=bucket_name, object_name=object_name
            )
        except S3Error as e:
            raise e

    def copy_object(
        self,
        source_bucket_name: str,
        source_object_name: str,
        destination_bucket_name: str,
        destination_object_name: str,
    ) -> ObjectWriteResult:
        try:
            return self.client.copy_object(
                bucket_name=destination_bucket_name,
                object_name=destination_object_name,
                source=CopySource(source_bucket_name, source_object_name),
            )
        except S3Error as e:
            raise e
