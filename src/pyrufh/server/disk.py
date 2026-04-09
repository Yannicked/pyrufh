"""Disk-based RUFH server implementation.

Stores upload data on the filesystem with metadata in sidecar files.
Automatically cleans up uploads that have exceeded their max-age.
Thread-safe for concurrent access within a single process, and uses
file locking for safety across multiple processes.
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    from ..headers import UploadLimits

from ..core import (
    DigestMismatchError,
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
    repr_digest: dict[str, str] | None = None

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

    Data is streamed to disk during append to avoid loading entire files
    into memory. Digest computation is done directly from the file on disk.

    File locking is used to ensure safe concurrent access across multiple
    processes (e.g., when running with Gunicorn or uvicorn workers).

    A background cleanup thread runs periodically to remove expired uploads.

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

    def _lock_path(self, upload_id: str) -> Path:
        """Return the path to an upload's lock file."""
        return self._storage_dir / f"{upload_id}.lock"

    def _meta_path(self, upload_id: str) -> Path:
        """Return the path to the upload's metadata file."""
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
                repr_digest=data.get("repr_digest"),
            )
        except (json.JSONDecodeError, KeyError, OSError):
            return None

    def _write_meta(self, metadata: _UploadMetadata) -> None:
        """Write metadata to disk atomically using a temp file."""
        meta_path = self._meta_path(metadata.upload_id)
        temp_path = meta_path.with_suffix(".meta.tmp")
        data = {
            "upload_id": metadata.upload_id,
            "uri": metadata.uri,
            "offset": metadata.offset,
            "complete": metadata.complete,
            "length": metadata.length,
            "max_age": metadata.max_age,
            "created_at": metadata.created_at,
            "updated_at": metadata.updated_at,
            "repr_digest": metadata.repr_digest,
        }
        with open(temp_path, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.rename(temp_path, meta_path)

    def _acquire_lock(self, upload_id: str) -> int:
        """Acquire an exclusive lock on the upload.

        Returns a file descriptor to the lock file.
        """
        lock_path = self._lock_path(upload_id)
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(lock_fd)
            raise
        return lock_fd

    def _release_lock(self, lock_fd: int, upload_id: str) -> None:
        """Release the exclusive lock on the upload."""
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)

    def _delete_files(self, upload_id: str) -> None:
        """Delete data, metadata, and lock files for an upload."""
        data_path = self._data_path(upload_id)
        meta_path = self._meta_path(upload_id)
        lock_path = self._lock_path(upload_id)
        for path in (data_path, meta_path, lock_path):
            if path.exists():
                path.unlink()

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

    def _append_chunk_to_file(self, upload_id: str, chunk: bytes) -> int:
        """Append a chunk to the data file on disk with exclusive locking.

        Returns the number of bytes written.
        """
        lock_fd = self._acquire_lock(upload_id)
        try:
            with open(self._data_path(upload_id), "ab") as f:
                f.write(chunk)
                f.flush()
                os.fsync(f.fileno())
                return len(chunk)
        finally:
            self._release_lock(lock_fd, upload_id)

    def _compute_file_digest(self, upload_id: str, algorithm: str) -> bytes | None:
        """Compute a digest directly from the file on disk without loading it all into memory.

        Returns None if the file doesn't exist or is empty.
        """
        from ..headers import compute_digest

        data_path = self._data_path(upload_id)
        if not data_path.exists():
            return None

        lock_fd = self._acquire_lock(upload_id)
        try:
            with open(data_path, "rb") as f:
                return compute_digest(algorithm, f)
        finally:
            self._release_lock(lock_fd, upload_id)

    def _store_upload(self, upload: Upload) -> None:
        """Persist an upload to disk.

        Note: For DiskRufhServer, data is already streamed to disk during append.
        This method updates metadata.
        """
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
            repr_digest=None,
        )
        self._write_meta(metadata)

    def _delete_upload(self, upload_id: str) -> None:
        """Remove an upload from disk."""
        self._delete_files(upload_id)

    def _get_upload(self, uri: str) -> Upload | None:
        """Load an upload from disk by URI.

        Returns an Upload with data loaded into memory. For large files,
        consider using get_upload_info() instead to avoid memory usage.
        """
        for upload_id in self._list_upload_ids():
            meta = self._read_meta(upload_id)
            if meta is not None and meta.uri == uri:
                if meta.is_expired():
                    self._delete_files(upload_id)
                    return None
                data_path = self._data_path(upload_id)
                if data_path.exists():
                    lock_fd = self._acquire_lock(upload_id)
                    try:
                        with open(data_path, "rb") as f:
                            data = f.read()
                    finally:
                        self._release_lock(lock_fd, upload_id)
                    return Upload(
                        upload_id=meta.upload_id,
                        uri=meta.uri,
                        data=bytearray(data),
                        offset=meta.offset,
                        complete=meta.complete,
                        length=meta.length,
                        limits=meta.limits,
                        max_age=meta.max_age,
                        repr_digest=None,
                    )
                self._delete_files(upload_id)
                return None
        return None

    def get_upload_info(self, uri: str) -> Upload | None:
        """Get upload info for a URI (without loading data into memory).

        Returns an Upload with empty data. Use compute_digest() to verify
        integrity without loading the entire file.
        """
        for upload_id in self._list_upload_ids():
            meta = self._read_meta(upload_id)
            if meta is not None and meta.uri == uri:
                if meta.is_expired():
                    self._delete_files(upload_id)
                    return None
                return Upload(
                    upload_id=meta.upload_id,
                    uri=meta.uri,
                    data=bytearray(),
                    offset=meta.offset,
                    complete=meta.complete,
                    length=meta.length,
                    limits=meta.limits,
                    max_age=meta.max_age,
                    repr_digest=None,
                )
        return None

    def compute_digest(self, uri: str, algorithm: str = "sha-256") -> bytes | None:
        """Compute a digest for an upload directly from disk.

        This avoids loading the entire file into memory by streaming
        the digest computation.

        Parameters
        ----------
        uri:
            The upload resource URI.
        algorithm:
            The digest algorithm (e.g., "sha-256", "sha-512").

        Returns
        -------
        bytes | None
            The digest bytes, or None if the upload doesn't exist.
        """
        upload_id = uri.rsplit("/", 1)[-1]
        return self._compute_file_digest(upload_id, algorithm)

    def create_upload(
        self,
        data: bytes | BinaryIO,
        *,
        method: str = "POST",
        complete: bool = False,
        length: int | None = None,
        content_type: str | None = None,
        uri: str | None = None,
        content_digest: dict[str, bytes] | None = None,
        repr_digest: dict[str, bytes] | None = None,
        want_repr_digest: dict[str, int] | None = None,
        want_content_digest: dict[str, int] | None = None,
    ) -> tuple[Upload, int]:
        """Create a new upload resource and stream data directly to disk."""
        from ..headers import compute_digest

        if isinstance(data, (bytes, bytearray)):
            body = bytes(data)
        else:
            body = data.read() if hasattr(data, "read") else data

        if content_digest:
            for alg, expected in content_digest.items():
                computed = compute_digest(alg, body)
                if computed != expected:
                    raise DigestMismatchError(
                        header_name="Content-Digest",
                        algorithm=alg,
                        expected=expected,
                        actual=computed,
                    )

        content_length = len(body)  # type: ignore[arg-type]
        inferred_length = length

        if complete and inferred_length is None and content_length > 0:
            inferred_length = content_length

        if uri is not None:
            upload_id = uri.rsplit("/", 1)[-1]
        else:
            upload_id = self._generate_upload_id()
            uri = self._build_uri(upload_id)

        lock_fd = self._acquire_lock(upload_id)
        try:
            with open(self._data_path(upload_id), "wb") as f:
                f.write(body)  # type: ignore[arg-type]
                f.flush()
                os.fsync(f.fileno())

            computed_repr_digest: dict[str, bytes] | None = None
            if want_repr_digest and complete and len(body) > 0:  # type: ignore[arg-type]
                computed_repr_digest = {}
                for alg in sorted(want_repr_digest.keys()):
                    if want_repr_digest[alg] > 0:
                        computed_repr_digest[alg] = compute_digest(alg, body)

            upload = Upload(
                upload_id=upload_id,
                uri=uri,
                data=bytearray(),
                offset=content_length,
                complete=complete,
                length=inferred_length,
                limits=self._limits,
                repr_digest=computed_repr_digest,
            )

            if repr_digest:
                for alg, expected in repr_digest.items():
                    if len(body) > 0:
                        computed = compute_digest(alg, body)
                        if computed != expected:
                            raise DigestMismatchError(
                                header_name="Repr-Digest",
                                algorithm=alg,
                                expected=expected,
                                actual=computed,
                            )

            with self._lock:
                self._uploads[upload_id] = upload

            now = time.time()
            metadata = _UploadMetadata(
                upload_id=upload.upload_id,
                uri=upload.uri,
                offset=upload.offset,
                complete=upload.complete,
                length=upload.length,
                limits=upload.limits,
                max_age=upload.max_age,
                created_at=now,
                updated_at=now,
                repr_digest=None,
            )
            self._write_meta(metadata)

            status = 200 if complete and upload.offset == upload.length else 201
        finally:
            self._release_lock(lock_fd, upload_id)

        return upload, status

    def append(
        self,
        uri: str,
        data: bytes | BinaryIO,
        *,
        upload_offset: int,
        complete: bool = False,
        upload_length: int | None = None,
        content_digest: dict[str, bytes] | None = None,
        want_repr_digest: dict[str, int] | None = None,
    ) -> Upload:
        """Append data to an upload resource (§4.4).

        Data is streamed directly to disk to avoid memory usage.
        """
        from ..headers import compute_digest

        upload = self._get_upload(uri)
        if upload is None:
            from ..core import UploadNotFoundError

            raise UploadNotFoundError(uri)

        if isinstance(data, (bytes, bytearray)):
            body = bytes(data)
        else:
            body = data.read() if hasattr(data, "read") else data

        if content_digest:
            for alg, expected in content_digest.items():
                computed = compute_digest(alg, body)
                if computed != expected:
                    raise DigestMismatchError(
                        header_name="Content-Digest",
                        algorithm=alg,
                        expected=expected,
                        actual=computed,
                    )

        written = self._append_chunk_to_file(upload.upload_id, body)
        upload.offset += written

        if complete:
            upload.finish(upload_length)
            if want_repr_digest and len(body) > 0:
                computed_repr_digest = {}
                for alg in sorted(want_repr_digest.keys()):
                    if want_repr_digest[alg] > 0:
                        d = self._compute_file_digest(upload.upload_id, alg)
                        if d is not None:
                            computed_repr_digest[alg] = d
                upload.repr_digest = computed_repr_digest

        with self._lock:
            self._uploads[upload.upload_id] = upload

        self._store_upload(upload)
        return upload

    def shutdown(self) -> None:
        """Stop the cleanup thread. Call before discarding the server."""
        self._cleanup_running = False
        if self._cleanup_thread is not None:
            self._cleanup_thread.join(timeout=5)
