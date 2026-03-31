"""Disk-based RUFH server implementation.

Stores upload data on the filesystem with metadata in sidecar files.
Automatically cleans up uploads that have exceeded their max-age.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..headers import UploadLimits

from ..core import (
    RufhServer,
    Upload,
)


@dataclass
class _UploadMetadata:
    """Metadata for an upload stored on disk."""

    upload_id: str
    uri: str
    offset: int
    complete: bool
    length: int | None = None
    limits: UploadLimits | None = None
    max_age: int | None = None
    created_at: float | None = None
    updated_at: float | None = None

    def to_upload(self, data: bytes) -> Upload:
        """Reconstruct an Upload from metadata and data."""
        return Upload(
            upload_id=self.upload_id,
            uri=self.uri,
            data=bytearray(data),
            offset=self.offset,
            complete=self.complete,
            length=self.length,
            limits=self.limits,
            max_age=self.max_age,
        )

    @classmethod
    def from_upload(cls, upload: Upload) -> _UploadMetadata:
        """Create metadata from an Upload."""
        now = time.time()
        return cls(
            upload_id=upload.upload_id,
            uri=upload.uri,
            offset=upload.offset,
            complete=upload.complete,
            length=upload.length,
            limits=upload.limits,
            max_age=upload.max_age,
            created_at=now,
            updated_at=now,
        )

    def is_expired(self) -> bool:
        """Check if the upload has exceeded its max-age."""
        if self.max_age is None:
            return False
        if self.updated_at is None:
            return False
        return (time.time() - self.updated_at) > self.max_age


class DiskRufhServer(RufhServer):
    """RUFH server that stores upload data on the filesystem.

    Each upload is stored as two files in the storage directory:
      - ``{upload_id}.data`` - the raw upload bytes
      - ``{upload_id}.meta`` - JSON metadata

    A background cleanup thread runs periodically to remove expired uploads.
    Thread-safe for concurrent access.

    Parameters
    ----------
    storage_dir:
        Directory where upload files are stored. Created if it doesn't exist.
    base_url:
        Base URL for upload URIs.
    limits:
        Optional upload limits to apply.
    cleanup_interval:
        Seconds between cleanup checks (default: 60). Set to 0 to disable.
    """

    def __init__(
        self,
        storage_dir: str | Path,
        *,
        base_url: str = "http://localhost:8000",
        limits: UploadLimits | None = None,
        cleanup_interval: int = 60,
    ) -> None:
        super().__init__(limits=limits, base_url=base_url)
        self._storage_dir = Path(storage_dir)
        self._cleanup_interval = cleanup_interval
        self._cleanup_running = True
        self._cleanup_thread: threading.Thread | None = None
        self._storage_dir.mkdir(parents=True, exist_ok=True)

        if cleanup_interval > 0:
            self._cleanup_thread = threading.Thread(
                target=self._cleanup_loop,
                daemon=True,
                name="DiskRufhServer-cleanup",
            )
            self._cleanup_thread.start()

    def _data_path(self, upload_id: str) -> Path:
        """Return the path to an upload's data file."""
        return self._storage_dir / f"{upload_id}.data"

    def _meta_path(self, upload_id: str) -> Path:
        """Return the path to an upload's metadata file."""
        return self._storage_dir / f"{upload_id}.meta"

    def _read_meta(self, upload_id: str) -> _UploadMetadata | None:
        """Read metadata for an upload."""
        meta_path = self._meta_path(upload_id)
        if not meta_path.exists():
            return None
        try:
            with open(meta_path) as f:
                data = json.load(f)
            return _UploadMetadata(
                upload_id=data["upload_id"],
                uri=data["uri"],
                offset=data["offset"],
                complete=data["complete"],
                length=data.get("length"),
                max_age=data.get("max_age"),
                created_at=data.get("created_at"),
                updated_at=data.get("updated_at"),
            )
        except (json.JSONDecodeError, KeyError, OSError):
            return None

    def _write_meta(self, metadata: _UploadMetadata) -> None:
        """Write metadata to disk."""
        meta_path = self._meta_path(metadata.upload_id)
        data = {
            "upload_id": metadata.upload_id,
            "uri": metadata.uri,
            "offset": metadata.offset,
            "complete": metadata.complete,
            "length": metadata.length,
            "max_age": metadata.max_age,
            "created_at": metadata.created_at,
            "updated_at": metadata.updated_at,
        }
        with open(meta_path, "w") as f:
            json.dump(data, f)

    def _delete_files(self, upload_id: str) -> None:
        """Delete both data and metadata files for an upload."""
        data_path = self._data_path(upload_id)
        meta_path = self._meta_path(upload_id)
        if data_path.exists():
            data_path.unlink()
        if meta_path.exists():
            meta_path.unlink()

    def _list_upload_ids(self) -> list[str]:
        """List all upload IDs currently on disk."""
        ids: list[str] = []
        for path in self._storage_dir.iterdir():
            if path.suffix == ".meta":
                ids.append(path.stem)
        return ids

    def _cleanup_loop(self) -> None:
        """Background thread that periodically cleans up expired uploads."""
        while self._cleanup_running:
            time.sleep(self._cleanup_interval)
            self._cleanup_expired()

    def _cleanup_expired(self) -> None:
        """Remove all expired uploads from disk."""
        with self._lock:
            for upload_id in self._list_upload_ids():
                meta = self._read_meta(upload_id)
                if meta is not None and meta.is_expired():
                    self._delete_files(upload_id)

    def _store_upload(self, upload: Upload) -> None:
        """Persist an upload to disk."""
        data_path = self._data_path(upload.upload_id)
        with open(data_path, "wb") as f:
            f.write(upload.data)

        existing = self._read_meta(upload.upload_id)
        now = time.time()
        metadata = _UploadMetadata(
            upload_id=upload.upload_id,
            uri=upload.uri,
            offset=upload.offset,
            complete=upload.complete,
            length=upload.length,
            limits=upload.limits,
            max_age=upload.max_age,
            created_at=existing.created_at if existing else now,
            updated_at=now,
        )
        self._write_meta(metadata)

    def _delete_upload(self, upload_id: str) -> None:
        """Remove an upload from disk."""
        self._delete_files(upload_id)

    def _get_upload(self, uri: str) -> Upload | None:
        """Load an upload from disk by URI."""
        for upload_id in self._list_upload_ids():
            meta = self._read_meta(upload_id)
            if meta is not None and meta.uri == uri:
                if meta.is_expired():
                    self._delete_files(upload_id)
                    return None
                data_path = self._data_path(upload_id)
                if data_path.exists():
                    with open(data_path, "rb") as f:
                        data = f.read()
                    return meta.to_upload(data)
                self._delete_files(upload_id)
                return None
        return None

    def get_upload_info(self, uri: str) -> Upload | None:
        """Get upload info for a URI (without loading data)."""
        for upload_id in self._list_upload_ids():
            meta = self._read_meta(upload_id)
            if meta is not None and meta.uri == uri:
                if meta.is_expired():
                    self._delete_files(upload_id)
                    return None
                return meta.to_upload(b"")
        return None

    def shutdown(self) -> None:
        """Stop the cleanup thread. Call before discarding the server."""
        self._cleanup_running = False
        if self._cleanup_thread is not None:
            self._cleanup_thread.join(timeout=5)
