from __future__ import annotations

from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.config import Settings


class ObjectStore:
    def __init__(self, settings: Settings) -> None:
        s3_config = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"} if settings.archive_s3_force_path_style else {},
        )

        self.client = boto3.client(
            "s3",
            endpoint_url=settings.archive_s3_endpoint,
            aws_access_key_id=settings.archive_s3_access_key,
            aws_secret_access_key=settings.archive_s3_secret_key,
            region_name=settings.archive_s3_region,
            config=s3_config,
        )
        self.bucket = settings.archive_bucket
        self.verify_bucket = settings.archive_s3_verify_bucket

    def ensure_bucket(self) -> None:
        if not self.verify_bucket:
            return

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            raise RuntimeError(f"object store bucket is not reachable: {self.bucket}") from exc

    def put_file(self, *, key: str, path: Path, content_type: str) -> None:
        self.client.upload_file(
            str(path),
            self.bucket,
            key,
            ExtraArgs={"ContentType": content_type},
        )

    def put_bytes(self, *, key: str, data: bytes, content_type: str) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )

    def exists(self, *, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False