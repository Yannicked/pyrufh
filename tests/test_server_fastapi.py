"""Tests for the FastAPI integration."""

from __future__ import annotations

import pytest

from pyrufh.server import InMemoryRufhServer


class TestFastAPIIntegration:
    """Tests for FastAPI server integration."""

    @pytest.fixture
    def app(self):
        """Create a FastAPI test app."""
        try:
            from fastapi import FastAPI  # ty: ignore

            from pyrufh.server.fastapi import setup_fastapi_routes
        except ImportError:
            pytest.skip("FastAPI not installed")

        fastapi_app = FastAPI()
        server = InMemoryRufhServer(base_url="http://localhost:8000")
        setup_fastapi_routes(fastapi_app, server)
        return fastapi_app

    @pytest.fixture
    def client(self, app):
        """Create a test client."""
        from httpx import ASGITransport, AsyncClient

        # FastAPI requires async client
        return AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        )

    @pytest.mark.anyio
    async def test_create_upload_complete(self, client):
        """Test creating a complete upload via FastAPI."""
        response = await client.post(
            "/uploads/test-file",
            content=b"hello world",
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

    @pytest.mark.anyio
    async def test_create_upload_incomplete(self, client):
        """Test creating an incomplete upload via FastAPI."""
        response = await client.post(
            "/uploads/test-file",
            content=b"hello",
            headers={
                "Content-Type": "text/plain",
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 201
        assert response.headers.get("Upload-Complete") == "?0"
        assert response.headers.get("Upload-Offset") == "5"

    @pytest.mark.anyio
    async def test_get_offset(self, client):
        """Test getting upload offset via HEAD request."""
        # First create an upload
        await client.post(
            "/uploads/my-file",
            content=b"test content",
            headers={
                "Upload-Complete": "?1",
                "Upload-Length": "12",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Then get offset
        response = await client.head(
            "/uploads/my-file",
            headers={"Upload-Draft-Interop-Version": "8"},
        )

        assert response.status_code == 200
        assert response.headers.get("Upload-Offset") == "12"
        assert response.headers.get("Upload-Complete") == "?1"

    @pytest.mark.anyio
    async def test_get_offset_not_found(self, client):
        """Test HEAD for non-existent upload."""
        response = await client.head(
            "/uploads/nonexistent",
            headers={"Upload-Draft-Interop-Version": "8"},
        )

        assert response.status_code == 404

    @pytest.mark.anyio
    async def test_append_upload(self, client):
        """Test appending to an upload via PATCH."""
        # First create incomplete upload
        await client.post(
            "/uploads/large-file",
            content=b"first part",
            headers={
                "Upload-Complete": "?0",
                "Upload-Length": "21",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Append the rest
        response = await client.patch(
            "/uploads/large-file",
            content=b"second part",
            headers={
                "Content-Type": "application/partial-upload",
                "Upload-Offset": "10",
                "Upload-Complete": "?1",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 200
        assert response.headers.get("Upload-Offset") == "21"

    @pytest.mark.anyio
    async def test_append_wrong_content_type(self, client):
        """Test append with wrong content type."""
        # Create upload
        await client.post(
            "/uploads/file",
            content=b"content",
            headers={
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Try to append with wrong content type
        response = await client.patch(
            "/uploads/file",
            content=b"more",
            headers={
                "Content-Type": "text/plain",  # Should be application/partial-upload
                "Upload-Offset": "7",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 415

    @pytest.mark.anyio
    async def test_append_missing_offset(self, client):
        """Test append without Upload-Offset header."""
        # Create upload
        await client.post(
            "/uploads/file",
            content=b"content",
            headers={
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Try to append without offset
        response = await client.patch(
            "/uploads/file",
            content=b"more",
            headers={
                "Content-Type": "application/partial-upload",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 400

    @pytest.mark.anyio
    async def test_append_offset_mismatch(self, client):
        """Test append with wrong offset returns 409."""
        # Create upload
        await client.post(
            "/uploads/file",
            content=b"content",
            headers={
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Try to append with wrong offset
        response = await client.patch(
            "/uploads/file",
            content=b"more",
            headers={
                "Content-Type": "application/partial-upload",
                "Upload-Offset": "0",  # Wrong offset, should be 7
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 409

    @pytest.mark.anyio
    async def test_cancel_upload(self, client):
        """Test cancelling an upload via DELETE."""
        # Create upload
        await client.post(
            "/uploads/to-cancel",
            content=b"content",
            headers={
                "Upload-Complete": "?0",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        # Cancel it
        response = await client.delete(
            "/uploads/to-cancel",
            headers={"Upload-Draft-Interop-Version": "8"},
        )

        assert response.status_code == 204

        # Verify it's gone
        head_response = await client.head(
            "/uploads/to-cancel",
            headers={"Upload-Draft-Interop-Version": "8"},
        )
        assert head_response.status_code == 404

    @pytest.mark.anyio
    async def test_cancel_not_found(self, client):
        """Test cancelling non-existent upload."""
        response = await client.delete(
            "/uploads/nonexistent",
            headers={"Upload-Draft-Interop-Version": "8"},
        )

        assert response.status_code == 404

    @pytest.mark.anyio
    async def test_upload_creation_with_put(self, client):
        """Test upload creation with PUT method."""
        response = await client.put(
            "/uploads/put-file",
            content=b"content via put",
            headers={
                "Content-Type": "text/plain",
                "Upload-Complete": "?1",
                "Upload-Length": "15",
                "Upload-Draft-Interop-Version": "8",
            },
        )

        assert response.status_code == 200
        assert response.headers.get("Upload-Offset") == "15"
