"""StorageBackend Protocol — all file I/O goes through this interface."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class StorageBackend(Protocol):
    """Protocol for storage backends. Implementations: LocalStorageBackend, R2StorageBackend."""

    async def read(self, key: str) -> bytes: ...

    async def write(self, key: str, data: bytes) -> None: ...

    async def list_keys(self, prefix: str) -> list[str]: ...

    async def exists(self, key: str) -> bool: ...

    async def delete(self, key: str) -> None: ...
