from abc import ABC, abstractmethod
from typing import BinaryIO

from minio import Minio, S3Error
from minio.commonconfig import ENABLED
from minio.versioningconfig import VersioningConfig


class BucketClient(ABC):
    @abstractmethod
    def get_fs_url_prefix(self) -> str:
        pass

    @abstractmethod
    def get_fs_storage_option(self) -> dict:
        pass

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


class MinioClient(BucketClient):
    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, secure: bool = False
    ):
        self.client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )

        self._storage_options = {
            "endpoint_url": f"http://{endpoint}",
            "key": access_key,
            "secret": secret_key,
        }

    def get_fs_url_prefix(self) -> str:
        return "s3://"

    def get_fs_storage_option(self) -> dict:
        return self._storage_options

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
