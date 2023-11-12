from abc import ABC, abstractmethod

from minio import Minio, S3Error


class BucketClient(ABC):
    @abstractmethod
    def check_connection(self) -> None:
        pass

    @abstractmethod
    def bucket_exists(self, bucket_name: str) -> bool:
        pass

    @abstractmethod
    def make_bucket(self, bucket_name: str):
        pass

    @abstractmethod
    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        pass


class MinioClient(BucketClient):
    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, secure: bool = False
    ):
        self.client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=secure
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

    def make_bucket(self, bucket_name: str):
        self.client.make_bucket(bucket_name)

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        self.client.fput_object(bucket_name, object_name, file_path)
