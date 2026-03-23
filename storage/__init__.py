"""Storage factory — picks backend from STORAGE_BACKEND env var."""

from __future__ import annotations

import os

from storage.local import LocalStorageBackend
from storage.protocol import StorageBackend
from storage.r2 import R2StorageBackend

__all__ = ["StorageBackend", "LocalStorageBackend", "R2StorageBackend", "get_storage_backend"]


def get_storage_backend(base_dir: str | None = None) -> StorageBackend:
    """Factory: returns the appropriate storage backend based on STORAGE_BACKEND env var."""
    backend = os.environ.get("STORAGE_BACKEND", "local")
    if backend == "r2":
        return R2StorageBackend()
    return LocalStorageBackend(base_dir or os.environ.get("LOCAL_DATA_DIR", "./data/local"))
