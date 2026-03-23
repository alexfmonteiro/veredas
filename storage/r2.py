"""Cloudflare R2 storage backend for staging/production."""

from __future__ import annotations

import asyncio
import os

import boto3
import structlog

logger = structlog.get_logger()


class R2StorageBackend:
    """Cloudflare R2 storage via S3-compatible API (boto3)."""

    def __init__(
        self,
        bucket_name: str | None = None,
        account_id: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        endpoint: str | None = None,
    ) -> None:
        self._bucket = bucket_name or os.environ["R2_BUCKET_NAME"]
        endpoint_url = endpoint or os.environ["R2_ENDPOINT"]
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id or os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=secret_access_key or os.environ["R2_SECRET_ACCESS_KEY"],
            region_name="auto",
        )

    async def read(self, key: str) -> bytes:
        def _read() -> bytes:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read()

        return await asyncio.to_thread(_read)

    async def write(self, key: str, data: bytes) -> None:
        def _write() -> None:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=data)

        await asyncio.to_thread(_write)
        logger.debug("storage_write", backend="r2", key=key, size=len(data))

    async def list_keys(self, prefix: str) -> list[str]:
        def _list() -> list[str]:
            paginator = self._client.get_paginator("list_objects_v2")
            keys: list[str] = []
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            return sorted(keys)

        return await asyncio.to_thread(_list)

    async def exists(self, key: str) -> bool:
        def _exists() -> bool:
            try:
                self._client.head_object(Bucket=self._bucket, Key=key)
                return True
            except self._client.exceptions.ClientError:
                return False

        return await asyncio.to_thread(_exists)

    async def delete(self, key: str) -> None:
        def _delete() -> None:
            self._client.delete_object(Bucket=self._bucket, Key=key)

        await asyncio.to_thread(_delete)
        logger.debug("storage_delete", backend="r2", key=key)
