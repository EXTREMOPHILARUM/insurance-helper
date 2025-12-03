"""Cloudflare R2 storage integration."""

import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class R2Uploader:
    """Upload files to Cloudflare R2 storage."""

    def __init__(
        self,
        account_id: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ):
        """Initialize R2 uploader.

        Args:
            account_id: Cloudflare account ID (or R2_ACCOUNT_ID env var)
            access_key_id: R2 access key (or R2_ACCESS_KEY_ID env var)
            secret_access_key: R2 secret key (or R2_SECRET_ACCESS_KEY env var)
            bucket_name: R2 bucket name (or R2_BUCKET_NAME env var)
        """
        self.account_id = account_id or os.environ.get("R2_ACCOUNT_ID")
        self.bucket = bucket_name or os.environ.get("R2_BUCKET_NAME")
        access_key = access_key_id or os.environ.get("R2_ACCESS_KEY_ID")
        secret_key = secret_access_key or os.environ.get("R2_SECRET_ACCESS_KEY")

        if not all([self.account_id, self.bucket, access_key, secret_key]):
            raise ValueError(
                "R2 credentials required. Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, "
                "R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME environment variables."
            )

        self.client = boto3.client(
            "s3",
            endpoint_url=f"https://{self.account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",  # R2 uses 'auto' for region
            config=Config(signature_version="s3v4"),
        )

        # Public URL base (requires public bucket or custom domain)
        self._public_url_base: Optional[str] = None

    @property
    def public_url_base(self) -> str:
        """Get public URL base for the bucket."""
        if self._public_url_base is None:
            # Default to r2.dev subdomain (requires public access enabled)
            self._public_url_base = f"https://{self.bucket}.r2.dev"
        return self._public_url_base

    def set_public_url_base(self, url: str) -> None:
        """Set custom public URL base (e.g., custom domain)."""
        self._public_url_base = url.rstrip("/")

    def upload_file(self, local_path: Path, r2_key: str, verify: bool = True) -> str:
        """Upload a file to R2 and return the public URL.

        Args:
            local_path: Path to local file
            r2_key: Key (path) in R2 bucket
            verify: Verify file exists after upload

        Returns:
            Public URL of uploaded file

        Raises:
            RuntimeError: If verification fails
        """
        self.client.upload_file(
            str(local_path),
            self.bucket,
            r2_key,
            ExtraArgs={"ContentType": self._get_content_type(local_path)},
        )

        if verify and not self.file_exists(r2_key):
            raise RuntimeError(f"Upload verification failed: {r2_key}")

        return f"{self.public_url_base}/{r2_key}"

    def upload_fileobj(self, fileobj, r2_key: str, content_type: str = "application/octet-stream") -> str:
        """Upload a file object to R2.

        Args:
            fileobj: File-like object to upload
            r2_key: Key (path) in R2 bucket
            content_type: MIME type of the file

        Returns:
            Public URL of uploaded file
        """
        self.client.upload_fileobj(
            fileobj,
            self.bucket,
            r2_key,
            ExtraArgs={"ContentType": content_type},
        )
        return f"{self.public_url_base}/{r2_key}"

    def file_exists(self, r2_key: str) -> bool:
        """Check if a file exists in R2.

        Args:
            r2_key: Key (path) in R2 bucket

        Returns:
            True if file exists
        """
        try:
            self.client.head_object(Bucket=self.bucket, Key=r2_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def delete_file(self, r2_key: str) -> bool:
        """Delete a file from R2.

        Args:
            r2_key: Key (path) in R2 bucket

        Returns:
            True if deleted successfully
        """
        try:
            self.client.delete_object(Bucket=self.bucket, Key=r2_key)
            return True
        except ClientError:
            return False

    def list_files(self, prefix: str = "") -> list[str]:
        """List files in R2 bucket with given prefix.

        Args:
            prefix: Key prefix to filter by

        Returns:
            List of keys matching prefix
        """
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        return keys

    def _get_content_type(self, path: Path) -> str:
        """Get content type based on file extension."""
        ext = path.suffix.lower()
        content_types = {
            ".pdf": "application/pdf",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls": "application/vnd.ms-excel",
            ".csv": "text/csv",
            ".json": "application/json",
        }
        return content_types.get(ext, "application/octet-stream")

    def generate_r2_key(self, product_type: str, relative_path: str) -> str:
        """Generate R2 key from product type and relative path.

        Args:
            product_type: Type of product (life, nonlife, health, life_list)
            relative_path: Relative path within downloads directory

        Returns:
            R2 key like "life/FY-2024/Insurer/file.pdf"
        """
        # Ensure forward slashes and no leading slash
        return f"{product_type}/{relative_path}".replace("\\", "/").lstrip("/")
