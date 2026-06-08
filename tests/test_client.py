"""Tests for RufhClient using pytest-httpx to mock HTTP interactions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

from pyrufh import (
    CompletedUploadError,
    MismatchingOffsetError,
    OffsetRetrievalError,
    RufhClient,
    UploadCancellationError,
    UploadCreationError,
)

TARGET_URL = "https://example.com/upload"
UPLOAD_RESOURCE_URI = "https://example.com/uploads/abc123"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _creation_headers(
    *,
    location: str = UPLOAD_RESOURCE_URI,
    offset: int = 0,
    complete: bool = False,
    length: int | None = None,
    limits: str | None = None,
) -> dict[str, str]:
    h = {
        "Location": location,
        "Upload-Complete": "?1" if complete else "?0",
    }
    if offset:
        h["Upload-Offset"] = str(offset)
    if length is not None:
        h["Upload-Length"] = str(length)
    if limits:
        h["Upload-Limit"] = limits
    return h


# ---------------------------------------------------------------------------
# §4.2  Upload Creation
# ---------------------------------------------------------------------------


class TestUploadCreation:
    def test_single_request_complete(self, httpx_mock: HTTPXMock):
        """The server completes the upload in a single request (§4.2, Example A)."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=200,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?1",
                "Upload-Offset": "100",
                "Content-Type": "application/json",
            },
            content=b'{"id": "abc123"}',
        )

        with RufhClient() as client:
            result = client.create_upload(TARGET_URL, b"x" * 100, complete=True)

        assert result.complete is True
        assert result.final_response is not None
        assert result.final_response.status_code == 200
        assert result.upload_resource.offset == 100

    def test_partial_upload_creation(self, httpx_mock: HTTPXMock):
        """Server creates upload resource but data not yet complete (§4.2, Example B)."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=201,
            headers=_creation_headers(complete=False, length=1000),
        )

        with RufhClient() as client:
            result = client.create_upload(TARGET_URL, b"x" * 100, complete=False, length=1000)

        assert result.complete is False
        assert result.final_response is None
        assert result.upload_resource.uri == UPLOAD_RESOURCE_URI
        assert result.upload_resource.length == 1000

    def test_4xx_raises_creation_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=405,
        )

        with RufhClient() as client, pytest.raises(UploadCreationError) as exc_info:
            client.create_upload(TARGET_URL, b"data")

        assert exc_info.value.status_code == 405

    def test_5xx_raises_creation_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=500,
        )

        with RufhClient() as client, pytest.raises(UploadCreationError) as exc_info:
            client.create_upload(TARGET_URL, b"data")

        assert exc_info.value.status_code == 500

    def test_missing_location_raises(self, httpx_mock: HTTPXMock):
        """Server returns 201 without Location header - should raise."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=201,
            headers={"Upload-Complete": "?0"},
        )

        with RufhClient() as client, pytest.raises(UploadCreationError, match="Location"):
            client.create_upload(TARGET_URL, b"data", complete=False)

    def test_upload_limit_parsed(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=201,
            headers={
                **_creation_headers(complete=False),
                "Upload-Limit": "max-size=5000",
            },
        )

        with RufhClient() as client:
            result = client.create_upload(TARGET_URL, b"x" * 50, complete=False)

        assert result.upload_resource.limits is not None
        assert result.upload_resource.limits.max_size == 5000

    def test_interop_version_header_sent(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=201,
            headers=_creation_headers(complete=False),
        )

        with RufhClient() as client:
            client.create_upload(TARGET_URL, b"data", complete=False)

        request = httpx_mock.get_requests()[0]
        assert "upload-draft-interop-version" in {k.lower() for k in request.headers}
        assert request.headers["upload-draft-interop-version"] == "8"


# ---------------------------------------------------------------------------
# §4.3  Offset Retrieval
# ---------------------------------------------------------------------------


class TestOffsetRetrieval:
    def test_basic_offset_retrieval(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="HEAD",
            url=UPLOAD_RESOURCE_URI,
            status_code=204,
            headers={
                "Upload-Offset": "50000",
                "Upload-Complete": "?0",
                "Upload-Length": "100000",
                "Upload-Limit": "max-age=3600",
                "Cache-Control": "no-store",
            },
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI)
            client.get_offset(resource)

        assert resource.offset == 50000
        assert resource.complete is False
        assert resource.length == 100000
        assert resource.limits is not None
        assert resource.limits.max_age == 3600

    def test_completed_upload(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="HEAD",
            url=UPLOAD_RESOURCE_URI,
            status_code=204,
            headers={
                "Upload-Offset": "100000",
                "Upload-Complete": "?1",
                "Upload-Length": "100000",
                "Cache-Control": "no-store",
            },
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI)
            client.get_offset(resource)

        assert resource.offset == 100000
        assert resource.complete is True

    def test_missing_offset_raises(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="HEAD",
            url=UPLOAD_RESOURCE_URI,
            status_code=204,
            headers={"Upload-Complete": "?0"},
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI)
            with pytest.raises(OffsetRetrievalError, match="Upload-Offset"):
                client.get_offset(resource)

    def test_4xx_raises(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="HEAD",
            url=UPLOAD_RESOURCE_URI,
            status_code=404,
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI)
            with pytest.raises(OffsetRetrievalError):
                client.get_offset(resource)

    def test_redirect_followed(self, httpx_mock: HTTPXMock):
        new_uri = "https://example.com/uploads/redirected"
        httpx_mock.add_response(
            method="HEAD",
            url=UPLOAD_RESOURCE_URI,
            status_code=308,
            headers={"Location": new_uri},
        )
        httpx_mock.add_response(
            method="HEAD",
            url=new_uri,
            status_code=204,
            headers={
                "Upload-Offset": "0",
                "Upload-Complete": "?0",
            },
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI)
            client.get_offset(resource)

        assert resource.uri == new_uri
        assert resource.offset == 0


# ---------------------------------------------------------------------------
# §4.4  Upload Append
# ---------------------------------------------------------------------------


class TestUploadAppend:
    def test_partial_append(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=204,
            headers={
                "Upload-Complete": "?0",
                "Upload-Offset": "200",
            },
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=100)
            response = client.append(resource, b"x" * 100, complete=False)

        assert response.status_code == 204
        assert resource.offset == 200
        assert resource.complete is False

    def test_final_append(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=200,
            headers={"Upload-Complete": "?1"},
            content=b'{"done": true}',
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=900, length=1000)
            response = client.append(resource, b"x" * 100, complete=True)

        assert response.status_code == 200
        assert resource.complete is True

    def test_mismatching_offset_raises(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=409,
            headers={
                "Content-Type": "application/problem+json",
                "Upload-Offset": "50",
            },
            json={
                "type": "https://iana.org/assignments/http-problem-types#mismatching-upload-offset",
                "title": "offset from request does not match offset of resource",
                "expected-offset": 50,
                "provided-offset": 100,
            },
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=100)
            with pytest.raises(MismatchingOffsetError) as exc_info:
                client.append(resource, b"data", complete=False)

        assert exc_info.value.expected_offset == 50
        assert exc_info.value.provided_offset == 100

    def test_completed_upload_raises(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=400,
            headers={"Content-Type": "application/problem+json"},
            json={
                "type": "https://iana.org/assignments/http-problem-types#completed-upload",
                "title": "upload is already completed",
            },
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=0)
            with pytest.raises(CompletedUploadError):
                client.append(resource, b"data", complete=False)

    def test_correct_content_type_sent(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=204,
            headers={"Upload-Complete": "?0"},
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=0)
            client.append(resource, b"data", complete=False)

        request = httpx_mock.get_requests()[0]
        assert request.headers["content-type"] == "application/partial-upload"


# ---------------------------------------------------------------------------
# §4.5  Upload Cancellation
# ---------------------------------------------------------------------------


class TestUploadCancellation:
    def test_successful_cancel(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="DELETE",
            url=UPLOAD_RESOURCE_URI,
            status_code=204,
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI)
            client.cancel(resource)  # Should not raise.

    def test_cancel_4xx_raises(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="DELETE",
            url=UPLOAD_RESOURCE_URI,
            status_code=404,
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI)
            with pytest.raises(UploadCancellationError):
                client.cancel(resource)


# ---------------------------------------------------------------------------
# §12.1  Optimistic Upload (client.upload)
# ---------------------------------------------------------------------------


class TestOptimisticUpload:
    def test_single_request_success(self, httpx_mock: HTTPXMock):
        """All data uploaded in one request; server responds with 200 Upload-Complete: ?1."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=200,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?1",
                "Upload-Offset": "200",
            },
            content=b'{"ok": true}',
        )

        with RufhClient() as client:
            response = client.upload(TARGET_URL, b"x" * 200)

        assert response.status_code == 200

    def test_multi_chunk_upload(self, httpx_mock: HTTPXMock):
        """Server returns 201 (Upload-Complete: ?0), then client sends PATCH to finish."""
        # Creation request - partial
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=201,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?0",
                "Upload-Offset": "3",
            },
        )
        # Final append
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=200,
            headers={"Upload-Complete": "?1"},
            content=b'{"ok": true}',
        )

        data = b"x" * 6
        with RufhClient(chunk_size=3) as client:
            response = client.upload(TARGET_URL, data)

        assert response.status_code == 200


# ---------------------------------------------------------------------------
# §12.2  Careful Upload (client.upload_carefully)
# ---------------------------------------------------------------------------


class TestCarefulUpload:
    def test_careful_upload(self, httpx_mock: HTTPXMock):
        """Empty creation followed by two PATCH requests."""
        # Empty creation request.
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=201,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?0",
                "Upload-Offset": "0",
            },
        )
        # First append (partial).
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=204,
            headers={
                "Upload-Complete": "?0",
                "Upload-Offset": "4",
            },
        )
        # Second (final) append.
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=200,
            headers={"Upload-Complete": "?1"},
            content=b'{"ok": true}',
        )

        data = b"abcdefgh"  # 8 bytes
        with RufhClient(chunk_size=4) as client:
            response = client.upload_carefully(TARGET_URL, data)

        assert response.status_code == 200

        requests = httpx_mock.get_requests()
        # Creation: empty body
        assert requests[0].content == b""
        assert requests[0].headers["upload-complete"] == "?0"
        # First PATCH: 4 bytes
        assert len(requests[1].content) == 4
        # Second PATCH: 4 bytes, complete
        assert len(requests[2].content) == 4
        assert requests[2].headers["upload-complete"] == "?1"


# ---------------------------------------------------------------------------
# RFC 9530 Digest Integration
# ---------------------------------------------------------------------------


class TestDigestCreation:
    def test_want_digest_true_enables_sha256(self, httpx_mock: HTTPXMock):
        """When want_digest=True, client uses sha-256."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=200,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?1",
                "Upload-Offset": "11",
            },
        )

        with RufhClient() as client:
            client.create_upload(
                TARGET_URL,
                b"hello world",
                complete=True,
                want_digest=True,
            )

        request = httpx_mock.get_requests()[0]
        assert "content-digest" in {k.lower() for k in request.headers}
        assert "sha-256" in request.headers["content-digest"].lower()

    def test_want_digest_list_algorithms(self, httpx_mock: HTTPXMock):
        """When want_digest is a list, those algorithms are used."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=200,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?1",
                "Upload-Offset": "11",
            },
        )

        with RufhClient() as client:
            client.create_upload(
                TARGET_URL,
                b"hello world",
                complete=True,
                want_digest=["sha-256", "sha-512"],
            )

        request = httpx_mock.get_requests()[0]
        cd = request.headers["content-digest"].lower()
        assert "sha-256" in cd
        assert "sha-512" in cd

    def test_want_digest_requests_repr_digest_on_complete(self, httpx_mock: HTTPXMock):
        """When want_digest is set with complete=True, Want-Repr-Digest is sent."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=200,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?1",
                "Upload-Offset": "11",
            },
        )

        with RufhClient() as client:
            client.create_upload(
                TARGET_URL,
                b"hello world",
                complete=True,
                want_digest=True,
            )

        request = httpx_mock.get_requests()[0]
        assert "want-repr-digest" in {k.lower() for k in request.headers}
        assert "sha-256" in request.headers["want-repr-digest"].lower()

    def test_want_digest_not_sent_for_incomplete(self, httpx_mock: HTTPXMock):
        """When want_digest is set but complete=False, Want-Repr-Digest is not sent."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=201,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?0",
                "Upload-Offset": "11",
            },
        )

        with RufhClient() as client:
            client.create_upload(
                TARGET_URL,
                b"hello world",
                complete=False,
                want_digest=True,
            )

        request = httpx_mock.get_requests()[0]
        assert "want-repr-digest" not in {k.lower() for k in request.headers}

    def test_explicit_content_digest_header(self, httpx_mock: HTTPXMock):
        """Explicit content_digest parameter takes precedence."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=200,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?1",
                "Upload-Offset": "11",
            },
        )

        from pyrufh.headers import compute_digest

        explicit_digest = {"sha-512": compute_digest("sha-512", b"hello world")}

        with RufhClient() as client:
            client.create_upload(
                TARGET_URL,
                b"hello world",
                complete=True,
                content_digest=explicit_digest,
            )

        request = httpx_mock.get_requests()[0]
        assert "sha-512" in request.headers["content-digest"].lower()


class TestDigestAppend:
    def test_append_want_digest(self, httpx_mock: HTTPXMock):
        """append() supports want_digest for automatic Content-Digest."""
        httpx_mock.add_response(
            method="PATCH",
            url=UPLOAD_RESOURCE_URI,
            status_code=204,
            headers={
                "Upload-Complete": "?0",
                "Upload-Offset": "100",
            },
        )

        with RufhClient() as client:
            from pyrufh import UploadResource

            resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=50)
            client.append(
                resource,
                b"x" * 50,
                complete=False,
                want_digest=True,
            )

        request = httpx_mock.get_requests()[0]
        assert "content-digest" in {k.lower() for k in request.headers}


class TestDigestUpload:
    def test_upload_with_want_digest(self, httpx_mock: HTTPXMock):
        """upload() supports want_digest for automatic Content-Digest."""
        httpx_mock.add_response(
            method="POST",
            url=TARGET_URL,
            status_code=200,
            headers={
                "Location": UPLOAD_RESOURCE_URI,
                "Upload-Complete": "?1",
                "Upload-Offset": "100",
            },
        )

        with RufhClient() as client:
            client.upload(
                TARGET_URL,
                b"x" * 100,
                want_digest=True,
            )

        request = httpx_mock.get_requests()[0]
        assert "content-digest" in {k.lower() for k in request.headers}
        assert "want-repr-digest" in {k.lower() for k in request.headers}
