"""Local filesystem storage backend for dev/test."""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog

logger = structlog.get_logger()


class LocalStorageBackend:
    """Local filesystem storage. Zero external dependencies."""

    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        return self._base_dir / key

    async def read(self, key: str) -> bytes:
        path = self._resolve(key)
        return await asyncio.to_thread(path.read_bytes)

    async def write(self, key: str, data: bytes) -> None:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(path.write_bytes, data)
        logger.debug("storage_write", backend="local", key=key, size=len(data))

    async def list_keys(self, prefix: str) -> list[str]:
        prefix_path = self._base_dir / prefix
        if not prefix_path.exists():
            return []

        def _list() -> list[str]:
            base = self._base_dir
            if prefix_path.is_dir():
                return sorted(
                    str(p.relative_to(base))
                    for p in prefix_path.rglob("*")
                    if p.is_file()
                )
            # prefix is a partial path — match files starting with prefix
            parent = prefix_path.parent
            name_prefix = prefix_path.name
            return sorted(
                str(p.relative_to(base))
                for p in parent.iterdir()
                if p.is_file() and p.name.startswith(name_prefix)
            )

        return await asyncio.to_thread(_list)

    async def exists(self, key: str) -> bool:
        path = self._resolve(key)
        return await asyncio.to_thread(path.exists)

    async def delete(self, key: str) -> None:
        path = self._resolve(key)
        if await asyncio.to_thread(path.exists):
            await asyncio.to_thread(path.unlink)
            logger.debug("storage_delete", backend="local", key=key)
