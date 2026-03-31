"""Tests for the RUFH server disk storage."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from pyrufh import UploadNotFoundError
from pyrufh.server.disk import DiskRufhServer

if TYPE_CHECKING:
    from pathlib import Path


class TestDiskRufhServer:
    """Tests for DiskRufhServer."""

    def test_create_and_retrieve_upload(self, tmp_path: Path):
        """Test creating an upload and retrieving it from disk."""
        server = DiskRufhServer(
            storage_dir=tmp_path,
            base_url="http://example.com",
        )

        upload, status = server.create_upload(
            b"hello world",
            complete=True,
        )

        assert status == 200
        assert upload.complete is True
        assert upload.offset == 11

        # Verify files exist
        data_path = tmp_path / f"{upload.upload_id}.data"
        meta_path = tmp_path / f"{upload.upload_id}.meta"
        assert data_path.exists()
        assert meta_path.exists()

        # Retrieve and verify
        retrieved = server.get_offset(upload.uri)
        assert retrieved.offset == 11
        assert retrieved.complete is True

        # Verify data file contents
        assert data_path.read_bytes() == b"hello world"

        server.shutdown()

    def test_upload_persistence_across_restarts(self, tmp_path: Path):
        """Test that uploads persist when server is recreated."""
        # Create initial upload
        server1 = DiskRufhServer(
            storage_dir=tmp_path,
            base_url="http://example.com",
        )
        upload, _ = server1.create_upload(b"persistent data", complete=True)
        server1.shutdown()

        # Create new server instance with same storage
        server2 = DiskRufhServer(
            storage_dir=tmp_path,
            base_url="http://example.com",
        )

        # Upload should still be accessible
        retrieved = server2.get_offset(upload.uri)
        assert retrieved.offset == 15

        server2.shutdown()

    def test_cleanup_of_expired_uploads(self, tmp_path: Path):
        """Test that expired uploads are cleaned up."""
        # Create server with very short cleanup interval
        server = DiskRufhServer(
            storage_dir=tmp_path,
            base_url="http://example.com",
            cleanup_interval=1,
        )

        # Create an upload with max_age
        upload, _ = server.create_upload(b"short-lived", complete=False)
        upload.max_age = 2  # 2 seconds
        server._store_upload(upload)

        # Verify upload exists
        assert server.get_offset(upload.uri).offset == 11

        # Wait for expiration
        time.sleep(2.5)

        # Trigger cleanup manually and check
        server._cleanup_expired()

        with pytest.raises(UploadNotFoundError):  # UploadNotFoundError
            server.get_offset(upload.uri)

        server.shutdown()

    def test_append_updates_timestamp(self, tmp_path: Path):
        """Test that appending updates the updated_at timestamp."""
        server = DiskRufhServer(
            storage_dir=tmp_path,
            base_url="http://example.com",
            cleanup_interval=0,  # Disable auto cleanup
        )

        upload, _ = server.create_upload(b"hello", complete=False)

        # Get original timestamp
        original_meta = server._read_meta(upload.upload_id)
        assert original_meta is not None
        original_updated = original_meta.updated_at
        assert original_updated is not None

        time.sleep(0.1)

        # Append data
        updated = server.append(
            upload.uri,
            b" world",
            upload_offset=5,
            complete=True,
        )

        # Check timestamp changed
        new_meta = server._read_meta(updated.upload_id)
        assert new_meta is not None
        new_updated = new_meta.updated_at
        assert new_updated is not None
        assert new_updated > original_updated

        server.shutdown()

    def test_delete_removes_files(self, tmp_path: Path):
        """Test that cancelling removes both data and meta files."""
        server = DiskRufhServer(
            storage_dir=tmp_path,
            base_url="http://example.com",
        )

        upload, _ = server.create_upload(b"to delete", complete=True)

        data_path = tmp_path / f"{upload.upload_id}.data"
        meta_path = tmp_path / f"{upload.upload_id}.meta"

        assert data_path.exists()
        assert meta_path.exists()

        server.cancel(upload.uri)

        assert not data_path.exists()
        assert not meta_path.exists()

        server.shutdown()

    def test_multiple_uploads(self, tmp_path: Path):
        """Test storing multiple uploads."""
        server = DiskRufhServer(
            storage_dir=tmp_path,
            base_url="http://example.com",
        )

        upload1, _ = server.create_upload(b"first", complete=True)
        upload2, _ = server.create_upload(b"second", complete=True)
        upload3, _ = server.create_upload(b"third", complete=True)

        assert server.get_offset(upload1.uri).offset == 5
        assert server.get_offset(upload2.uri).offset == 6
        assert server.get_offset(upload3.uri).offset == 5

        server.shutdown()
