"""Tests for the RUFH server implementations."""

from __future__ import annotations

import pytest

from pyrufh.server import (
    InMemoryRufhServer,
    UploadAlreadyCompleteError,
    UploadLengthMismatchError,
    UploadNotFoundError,
    UploadOffsetMismatchError,
)


class TestInMemoryRufhServer:
    """Tests for InMemoryRufhServer."""

    def test_create_upload_complete(self):
        """Test creating a complete upload in one request."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, status = server.create_upload(
            b"hello world",
            complete=True,
            length=11,
        )

        assert status == 200
        assert upload.complete is True
        assert upload.offset == 11
        assert upload.length == 11
        assert "http://example.com/uploads/" in upload.uri

    def test_create_upload_incomplete(self):
        """Test creating an upload that is not complete."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, status = server.create_upload(
            b"hello",
            complete=False,
        )

        assert status == 201
        assert upload.complete is False
        assert upload.offset == 5

    def test_create_upload_infers_length(self):
        """Test that length is inferred from Content-Length when complete."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _status = server.create_upload(
            b"hello world",
            complete=True,
        )

        assert upload.length == 11

    def test_get_offset(self):
        """Test retrieving upload offset."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _ = server.create_upload(b"hello world", complete=True)
        retrieved = server.get_offset(upload.uri)

        assert retrieved.offset == 11
        assert retrieved.complete is True

    def test_get_offset_not_found(self):
        """Test getting offset for non-existent upload."""
        server = InMemoryRufhServer(base_url="http://example.com")

        with pytest.raises(UploadNotFoundError):
            server.get_offset("http://example.com/uploads/nonexistent")

    def test_append(self):
        """Test appending data to an upload."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _ = server.create_upload(b"hello", complete=False)

        updated = server.append(
            upload.uri,
            b" world",
            upload_offset=5,
            complete=False,
        )

        assert updated.offset == 11
        assert upload.offset == 11

    def test_append_with_complete(self):
        """Test appending final data with complete=True."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _ = server.create_upload(b"hello", complete=False, length=11)

        updated = server.append(
            upload.uri,
            b" world",
            upload_offset=5,
            complete=True,
            upload_length=11,
        )

        assert updated.complete is True
        assert updated.offset == 11

    def test_append_offset_mismatch(self):
        """Test appending with wrong offset raises error."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _ = server.create_upload(b"hello world", complete=False)

        with pytest.raises(UploadOffsetMismatchError) as exc_info:
            server.append(
                upload.uri,
                b" extra",
                upload_offset=0,  # Wrong offset
            )

        assert exc_info.value.expected_offset == 11
        assert exc_info.value.provided_offset == 0

    def test_append_to_complete_upload(self):
        """Test appending to already complete upload raises error."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _ = server.create_upload(b"hello world", complete=True)

        with pytest.raises(UploadAlreadyCompleteError):
            server.append(
                upload.uri,
                b" extra",
                upload_offset=11,
            )

    def test_append_length_mismatch(self):
        """Test appending with wrong length raises error."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _ = server.create_upload(b"hello", complete=False, length=20)

        with pytest.raises(UploadLengthMismatchError):
            server.append(
                upload.uri,
                b" world",
                upload_offset=5,
                complete=True,
                upload_length=20,  # Wrong length
            )

    def test_cancel(self):
        """Test cancelling an upload."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _ = server.create_upload(b"hello world", complete=False)

        server.cancel(upload.uri)

        with pytest.raises(UploadNotFoundError):
            server.get_offset(upload.uri)

    def test_cancel_not_found(self):
        """Test cancelling non-existent upload raises error."""
        server = InMemoryRufhServer(base_url="http://example.com")

        with pytest.raises(UploadNotFoundError):
            server.cancel("http://example.com/uploads/nonexistent")

    def test_get_upload_info(self):
        """Test getting upload info."""
        server = InMemoryRufhServer(base_url="http://example.com")

        upload, _ = server.create_upload(b"hello world", complete=True)

        info = server.get_upload_info(upload.uri)

        assert info is not None
        assert info.offset == 11
        assert info.complete is True

    def test_get_upload_info_not_found(self):
        """Test getting info for non-existent upload."""
        server = InMemoryRufhServer(base_url="http://example.com")

        info = server.get_upload_info("http://example.com/uploads/nonexistent")

        assert info is None
