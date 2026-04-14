"""Tests for the Flask integration."""

from __future__ import annotations

import pytest

from pyrufh.server import InMemoryRufhServer


class TestFlaskIntegration:
    """Tests for Flask server integration."""

    @pytest.fixture
    def app(self):
        """Create a Flask test app."""
        try:
            from flask import Flask  # ty: ignore

            from pyrufh.server.flask import setup_flask_routes
        except ImportError:
            pytest.skip("Flask not installed")

        flask_app = Flask(__name__)
        server = InMemoryRufhServer(base_url="http://localhost:5000")
        setup_flask_routes(flask_app, server)
        flask_app.config["TESTING"] = True
        return flask_app

    @pytest.fixture
    def client(self, app):
        """Create a test client."""
        return app.test_client()

    def test_create_upload_complete(self, client):
        """Test creating a complete upload via Flask."""
        response = client.post(
            "/uploads/test-file",
            data=b"hello world",
            headers={
                "Content-Type": "text/plain",
                "Upload-Complete": "?1",
                "Upload-Length": "11",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 200
        assert response.headers.get("Upload-Complete") == "?1"
        assert response.headers.get("Upload-Offset") == "11"
        assert "Location" in response.headers

    def test_create_upload_incomplete(self, client):
        """Test creating an incomplete upload via Flask."""
        response = client.post(
            "/uploads/test-file",
            data=b"hello",
            headers={
                "Content-Type": "text/plain",
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 201
        assert response.headers.get("Upload-Complete") == "?0"
        assert response.headers.get("Upload-Offset") == "5"

    def test_get_offset(self, client):
        """Test getting upload offset via HEAD request."""
        # First create an upload
        client.post(
            "/uploads/my-file",
            data=b"test content",
            headers={
                "Upload-Complete": "?1",
                "Upload-Length": "12",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Then get offset
        response = client.head(
            "/uploads/my-file",
            headers={"Upload-Draft-Interop-Version": "8"},
        )

        assert response.status_code == 200
        assert response.headers.get("Upload-Offset") == "12"
        assert response.headers.get("Upload-Complete") == "?1"

    def test_get_offset_not_found(self, client):
        """Test HEAD for non-existent upload."""
        response = client.head(
            "/uploads/nonexistent",
            headers={"Upload-Draft-Interop-Version": "8"},
        )

        assert response.status_code == 404

    def test_append_upload(self, client):
        """Test appending to an upload via PATCH."""
        # First create incomplete upload
        client.post(
            "/uploads/large-file",
            data=b"first part",
            headers={
                "Upload-Complete": "?0",
                "Upload-Length": "21",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Append the rest
        response = client.patch(
            "/uploads/large-file",
            data=b"second part",
            headers={
                "Content-Type": "application/partial-upload",
                "Upload-Offset": "10",
                "Upload-Complete": "?1",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 200
        assert response.headers.get("Upload-Offset") == "21"

    def test_append_wrong_content_type(self, client):
        """Test append with wrong content type."""
        # Create upload
        client.post(
            "/uploads/file",
            data=b"content",
            headers={
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Try to append with wrong content type
        response = client.patch(
            "/uploads/file",
            data=b"more",
            headers={
                "Content-Type": "text/plain",  # Should be application/partial-upload
                "Upload-Offset": "7",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 415

    def test_append_missing_offset(self, client):
        """Test append without Upload-Offset header."""
        # Create upload
        client.post(
            "/uploads/file",
            data=b"content",
            headers={
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Try to append without offset
        response = client.patch(
            "/uploads/file",
            data=b"more",
            headers={
                "Content-Type": "application/partial-upload",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 400

    def test_append_offset_mismatch(self, client):
        """Test append with wrong offset returns 409."""
        # Create upload
        client.post(
            "/uploads/file",
            data=b"content",
            headers={
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Try to append with wrong offset
        response = client.patch(
            "/uploads/file",
            data=b"more",
            headers={
                "Content-Type": "application/partial-upload",
                "Upload-Offset": "0",  # Wrong offset, should be 7
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 409

    def test_cancel_upload(self, client):
        """Test cancelling an upload via DELETE."""
        # Create upload
        client.post(
            "/uploads/to-cancel",
            data=b"content",
            headers={
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Cancel it
        response = client.delete(
            "/uploads/to-cancel",
            headers={"Upload-Draft-Interop-Version": "8"},
        )

        assert response.status_code == 204

        # Verify it's gone
        head_response = client.head(
            "/uploads/to-cancel",
            headers={"Upload-Draft-Interop-Version": "8"},
        )
        assert head_response.status_code == 404

    def test_cancel_not_found(self, client):
        """Test cancelling non-existent upload."""
        response = client.delete(
            "/uploads/nonexistent",
            headers={"Upload-Draft-Interop-Version": "8"},
        )

        assert response.status_code == 404

    def test_upload_creation_with_put(self, client):
        """Test upload creation with PUT method."""
        response = client.put(
            "/uploads/put-file",
            data=b"content via put",
            headers={
                "Content-Type": "text/plain",
                "Upload-Complete": "?1",
                "Upload-Length": "15",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 200
        assert response.headers.get("Upload-Offset") == "15"
