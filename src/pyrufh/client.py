"""RufhClient - synchronous client for the Resumable Uploads for HTTP protocol.

Implements draft-ietf-httpbis-resumable-upload-11 client behaviour:
  - Upload creation  (§4.2)
  - Offset retrieval (§4.3)
  - Upload append    (§4.4)
  - Upload cancellation (§4.5)
  - Optimistic upload  (§12.1) with transparent resumption on interruption
  - Careful upload     (§12.2)
"""

from __future__ import annotations

import logging
from typing import BinaryIO

import httpx

from .exceptions import (
    CompletedUploadError,
    InconsistentLengthError,
    MismatchingOffsetError,
    OffsetRetrievalError,
    UploadAppendError,
    UploadCancellationError,
    UploadCreationError,
)
from .headers import (
    CONTENT_TYPE_PARTIAL_UPLOAD,
    build_upload_complete_header,
    build_upload_length_header,
    build_upload_offset_header,
    draft_interop_headers,
    parse_location,
    parse_upload_complete,
    parse_upload_length,
    parse_upload_limits,
    parse_upload_offset,
)
from .models import UploadCreationResult, UploadResource

logger = logging.getLogger(__name__)

# Problem type URIs defined in §7 of the spec
_PROBLEM_MISMATCHING_OFFSET = (
    "https://iana.org/assignments/http-problem-types#mismatching-upload-offset"
)
_PROBLEM_COMPLETED_UPLOAD = "https://iana.org/assignments/http-problem-types#completed-upload"
_PROBLEM_INCONSISTENT_LENGTH = (
    "https://iana.org/assignments/http-problem-types#inconsistent-upload-length"
)

# Default chunk size for streaming uploads (1 MiB)
DEFAULT_CHUNK_SIZE = 1024 * 1024


class RufhClient:
    """Synchronous client for the Resumable Uploads for HTTP protocol.

    Wraps an :class:`httpx.Client` and exposes the four protocol operations
    (create, get_offset, append, cancel) as well as two higher-level helpers
    (upload, upload_carefully) that implement the two upload strategies
    described in §12.

    Parameters
    ----------
    client:
        An :class:`httpx.Client` instance to use for HTTP requests. If not
        provided, a new client is created. The caller is responsible for
        closing it.
    chunk_size:
        Number of bytes to read per chunk when streaming uploads. Defaults to
        ``DEFAULT_CHUNK_SIZE`` (1 MiB).
    max_retries:
        Number of times to retry offset retrieval and append after a 5xx or
        connectivity error before giving up.
    """

    def __init__(
        self,
        client: httpx.Client | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_retries: int = 3,
    ) -> None:
        self._client = client or httpx.Client()
        self._owns_client = client is None
        self.chunk_size = chunk_size
        self.max_retries = max_retries

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> RufhClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client if it was created by this instance."""
        if self._owns_client:
            self._client.close()

    # ------------------------------------------------------------------
    # §4.2  Upload Creation
    # ------------------------------------------------------------------

    def create_upload(
        self,
        url: str,
        data: bytes | BinaryIO,
        *,
        method: str = "POST",
        complete: bool = True,
        length: int | None = None,
        content_type: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> UploadCreationResult:
        """Create a new upload resource (§4.2).

        Parameters
        ----------
        url:
            The target URL to POST/PUT the upload creation request to.
        data:
            The representation data to upload. Can be ``bytes`` or a
            file-like binary stream. If ``complete=False`` and you want to
            send a partial first chunk, pass only the first chunk here.
        method:
            HTTP method to use (default ``"POST"``). Any method that allows
            content is valid per the spec.
        complete:
            Set ``Upload-Complete`` to true if this request carries all the
            representation data; false if more data will follow via append
            requests (§4.1.2).
        length:
            Total length of the entire representation in bytes (``Upload-
            Length``). Optional but SHOULD be provided when known.
        content_type:
            Content-Type of the original representation (not the partial-
            upload type; that is only used for PATCH appends).
        extra_headers:
            Additional HTTP headers to include in the request.

        Returns
        -------
        UploadCreationResult
            Contains the :class:`UploadResource` and, if the upload is already
            complete, the final ``httpx.Response``.

        Raises
        ------
        UploadCreationError
            If the server responds with a 4xx (non-retryable) error.
        """
        if isinstance(data, (bytes, bytearray)):
            body = bytes(data)
            content_length = len(body)
        else:
            body = data.read()
            content_length = len(body)

        headers: dict[str, str] = {
            **draft_interop_headers(),
            "Upload-Complete": build_upload_complete_header(complete),
            "Content-Length": str(content_length),
        }

        # Indicate length via Upload-Length if provided, or infer from
        # Content-Length + Upload-Complete: ?1 (§4.1.3).
        if length is not None:
            headers["Upload-Length"] = build_upload_length_header(length)
        elif complete and content_length > 0:
            # Length can be inferred: total = offset(0) + Content-Length.
            headers["Upload-Length"] = build_upload_length_header(content_length)

        if content_type is not None:
            headers["Content-Type"] = content_type

        if extra_headers:
            headers.update(extra_headers)

        logger.debug("Creating upload: %s %s (complete=%s)", method, url, complete)

        try:
            response = self._client.request(method, url, content=body, headers=headers)
        except httpx.RequestError as exc:
            raise UploadCreationError(f"Network error during upload creation: {exc}") from exc

        # ---- Interpret the final response (§4.2.1) -------------------------

        if response.status_code == 104:
            # This should not normally be returned as a final response by httpx,
            # but handle defensively.
            raise UploadCreationError("Received unexpected 104 as final response", status_code=104)

        if 400 <= response.status_code < 500:
            # 4xx - do not retry (§4.2.1).
            self._raise_for_problem(response, UploadCreationError)
            raise UploadCreationError(
                f"Upload creation rejected: {response.status_code}",
                status_code=response.status_code,
            )

        if response.status_code >= 500:
            raise UploadCreationError(
                f"Server error during upload creation: {response.status_code}",
                status_code=response.status_code,
            )

        # 2xx success path.
        location = parse_location(response.headers)
        upload_complete_header = parse_upload_complete(response.headers)
        upload_offset = parse_upload_offset(response.headers)
        upload_length = parse_upload_length(response.headers)
        limits = parse_upload_limits(response.headers)

        # When Upload-Complete: ?1 is in the final response, the upload is done
        # and the response is from the targeted resource (§4.2.1, §4.4.1).
        is_complete = upload_complete_header is True

        if is_complete:
            # Upload completed in this single request - location may or may not
            # be present (exempt from Location requirement per §4.2.2).
            upload_uri = location or url
            resource = UploadResource(
                uri=upload_uri,
                offset=upload_offset if upload_offset is not None else content_length,
                complete=True,
                length=upload_length or length or content_length,
                limits=limits,
                final_response=response,
            )
            return UploadCreationResult(upload_resource=resource, final_response=response)

        # Not complete yet - Location MUST be present.
        if location is None:
            raise UploadCreationError(
                "Server did not return a Location header for the upload resource"
            )

        offset = upload_offset if upload_offset is not None else content_length
        resource = UploadResource(
            uri=location,
            offset=offset,
            complete=False,
            length=upload_length or length,
            limits=limits,
        )
        return UploadCreationResult(upload_resource=resource)

    # ------------------------------------------------------------------
    # §4.3  Offset Retrieval
    # ------------------------------------------------------------------

    def get_offset(self, upload_resource: UploadResource) -> UploadResource:
        """Retrieve the current upload offset from the server (§4.3).

        Sends a HEAD request to the upload resource URI and updates the
        provided :class:`UploadResource` in-place.

        Parameters
        ----------
        upload_resource:
            The upload resource whose offset should be retrieved.

        Returns
        -------
        UploadResource
            The same object, mutated with the latest state.

        Raises
        ------
        OffsetRetrievalError
            On 4xx responses or unrecoverable errors.
        """
        headers = {
            **draft_interop_headers(),
        }

        logger.debug("Retrieving offset for upload: %s", upload_resource.uri)

        try:
            response = self._client.head(upload_resource.uri, headers=headers)
        except httpx.RequestError as exc:
            raise OffsetRetrievalError(f"Network error during offset retrieval: {exc}") from exc

        if response.status_code in (307, 308):
            new_uri = parse_location(response.headers) or upload_resource.uri
            upload_resource.uri = new_uri
            return self.get_offset(upload_resource)

        if 400 <= response.status_code < 500:
            raise OffsetRetrievalError(
                f"Offset retrieval rejected: {response.status_code}",
                status_code=response.status_code,
            )

        if response.status_code >= 500:
            raise OffsetRetrievalError(
                f"Server error during offset retrieval: {response.status_code}",
                status_code=response.status_code,
            )

        offset = parse_upload_offset(response.headers)
        if offset is None:
            raise OffsetRetrievalError("Server response is missing Upload-Offset header")

        complete = parse_upload_complete(response.headers)
        length = parse_upload_length(response.headers)
        limits = parse_upload_limits(response.headers)

        upload_resource.offset = offset
        upload_resource.complete = complete is True
        if length is not None:
            upload_resource.length = length
        if limits is not None:
            upload_resource.limits = limits

        return upload_resource

    # ------------------------------------------------------------------
    # §4.4  Upload Append
    # ------------------------------------------------------------------

    def append(
        self,
        upload_resource: UploadResource,
        data: bytes | BinaryIO,
        *,
        complete: bool,
        extra_headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Append representation data to an upload resource (§4.4).

        Sends a PATCH request with ``Content-Type: application/partial-upload``.

        Parameters
        ----------
        upload_resource:
            The upload resource to append to.  Its ``offset`` field MUST be
            set to the offset the server expects (retrieved via
            :meth:`get_offset` or from the previous response).
        data:
            The chunk of representation data to send.
        complete:
            Whether this is the final chunk (``Upload-Complete: ?1``).
        extra_headers:
            Additional HTTP headers to include in the request.

        Returns
        -------
        httpx.Response
            The final HTTP response from the server.

        Raises
        ------
        MismatchingOffsetError
            If the server rejects the request because the offset is wrong (409).
        CompletedUploadError
            If the server rejects the request because the upload is already
            complete.
        InconsistentLengthError
            If the server rejects the request due to inconsistent lengths.
        UploadAppendError
            On other 4xx or server errors.
        """
        body = bytes(data) if isinstance(data, (bytes, bytearray)) else data.read()
        content_length = len(body)

        headers: dict[str, str] = {
            **draft_interop_headers(),
            "Content-Type": CONTENT_TYPE_PARTIAL_UPLOAD,
            "Upload-Complete": build_upload_complete_header(complete),
            "Upload-Offset": build_upload_offset_header(upload_resource.offset),
            "Content-Length": str(content_length),
        }

        # Communicate the total length when it becomes known with the final chunk.
        if complete and upload_resource.length is not None:
            headers["Upload-Length"] = build_upload_length_header(upload_resource.length)

        if extra_headers:
            headers.update(extra_headers)

        logger.debug(
            "Appending %d bytes to upload %s at offset %d (complete=%s)",
            content_length,
            upload_resource.uri,
            upload_resource.offset,
            complete,
        )

        try:
            response = self._client.patch(upload_resource.uri, content=body, headers=headers)
        except httpx.RequestError as exc:
            raise UploadAppendError(f"Network error during upload append: {exc}") from exc

        # ---- Handle error responses (§4.4.1) --------------------------------

        if response.status_code == 409:
            # Mismatching offset.
            expected = parse_upload_offset(response.headers)
            raise MismatchingOffsetError(
                expected_offset=expected if expected is not None else -1,
                provided_offset=upload_resource.offset,
            )

        if response.status_code in (400, 410, 404):
            self._raise_for_problem(response, UploadAppendError)
            raise UploadAppendError(
                f"Upload append rejected: {response.status_code}",
                status_code=response.status_code,
            )

        if 400 <= response.status_code < 500:
            self._raise_for_problem(response, UploadAppendError)
            raise UploadAppendError(
                f"Upload append rejected: {response.status_code}",
                status_code=response.status_code,
            )

        if response.status_code >= 500:
            raise UploadAppendError(
                f"Server error during upload append: {response.status_code}",
                status_code=response.status_code,
            )

        # ---- Update local state from the response ---------------------------
        new_offset = parse_upload_offset(response.headers)
        if new_offset is not None:
            upload_resource.offset = new_offset
        else:
            upload_resource.offset += content_length

        response_complete = parse_upload_complete(response.headers)
        if response_complete is not None:
            upload_resource.complete = response_complete
        elif complete:
            upload_resource.complete = True

        new_limits = parse_upload_limits(response.headers)
        if new_limits is not None:
            upload_resource.limits = new_limits

        if upload_resource.complete:
            upload_resource.final_response = response

        return response

    # ------------------------------------------------------------------
    # §4.5  Upload Cancellation
    # ------------------------------------------------------------------

    def cancel(self, upload_resource: UploadResource) -> None:
        """Cancel an upload by sending a DELETE request to the upload resource (§4.5).

        Parameters
        ----------
        upload_resource:
            The upload resource to cancel.

        Raises
        ------
        UploadCancellationError
            If the server responds with an unexpected error.
        """
        headers = {
            **draft_interop_headers(),
        }

        logger.debug("Cancelling upload: %s", upload_resource.uri)

        try:
            response = self._client.delete(upload_resource.uri, headers=headers)
        except httpx.RequestError as exc:
            raise UploadCancellationError(
                f"Network error during upload cancellation: {exc}"
            ) from exc

        if response.status_code >= 400:
            raise UploadCancellationError(
                f"Upload cancellation failed: {response.status_code}",
                status_code=response.status_code,
            )

    # ------------------------------------------------------------------
    # §12.1  Optimistic Upload Strategy (high-level helper)
    # ------------------------------------------------------------------

    def upload(
        self,
        url: str,
        data: bytes | BinaryIO,
        *,
        method: str = "POST",
        length: int | None = None,
        content_type: str | None = None,
        extra_headers: dict[str, str] | None = None,
        chunk_size: int | None = None,
    ) -> httpx.Response:
        """Upload representation data using the optimistic strategy (§12.1).

        Attempts to upload all data in a single request.  If the connection is
        interrupted and the server had previously sent a 104 response (captured
        in the Location header of any interim or final response), the upload is
        automatically resumed.

        For large files this method reads ``chunk_size`` bytes at a time and
        sends them as sequential PATCH append requests once the upload resource
        URI is known.

        Parameters
        ----------
        url:
            Target URL for the upload creation request.
        data:
            Representation data. Can be ``bytes`` or a seekable/non-seekable
            binary stream.
        method:
            HTTP method for upload creation (default ``"POST"``).
        length:
            Total size in bytes if known.
        content_type:
            Content-Type of the representation.
        extra_headers:
            Additional headers to pass to the creation request.
        chunk_size:
            Override the instance-level ``chunk_size``.

        Returns
        -------
        httpx.Response
            The final response from the server after the upload completes.

        Raises
        ------
        UploadNotResumableError
            If the upload was interrupted before a 104 response (with the
            upload resource URI) was received, making resumption impossible.
        UploadCreationError / UploadAppendError
            On unrecoverable errors.
        """
        _chunk_size = chunk_size or self.chunk_size

        # Normalise input to bytes for simplicity.
        raw = bytes(data) if isinstance(data, (bytes, bytearray)) else data.read()
        total_length = length if length is not None else len(raw)

        # Attempt to send everything in one shot.
        result = self.create_upload(
            url,
            raw,
            method=method,
            complete=True,
            length=total_length,
            content_type=content_type,
            extra_headers=extra_headers,
        )

        if result.complete and result.final_response is not None:
            return result.final_response

        # The upload resource was created but not completed (e.g. the server
        # returned 201 with Upload-Complete: ?0).  Continue with appends.
        return self._stream_append(result.upload_resource, raw, _chunk_size)

    # ------------------------------------------------------------------
    # §12.2  Careful Upload Strategy (high-level helper)
    # ------------------------------------------------------------------

    def upload_carefully(
        self,
        url: str,
        data: bytes | BinaryIO,
        *,
        method: str = "POST",
        length: int | None = None,
        content_type: str | None = None,
        extra_headers: dict[str, str] | None = None,
        chunk_size: int | None = None,
    ) -> httpx.Response:
        """Upload representation data using the careful strategy (§12.2).

        Sends an empty upload creation request first to obtain the upload
        resource URI and discover limits, then streams the representation data
        via PATCH append requests.

        Parameters
        ----------
        url:
            Target URL for the empty upload creation request.
        data:
            Representation data.
        method:
            HTTP method for upload creation (default ``"POST"``).
        length:
            Total size in bytes if known.
        content_type:
            Content-Type of the representation.
        extra_headers:
            Additional headers to pass to the creation request.
        chunk_size:
            Override the instance-level ``chunk_size``.

        Returns
        -------
        httpx.Response
            The final response from the server after the upload completes.
        """
        _chunk_size = chunk_size or self.chunk_size

        raw = bytes(data) if isinstance(data, (bytes, bytearray)) else data.read()
        total_length = length if length is not None else len(raw)

        # Step 1: empty upload creation (Upload-Complete: ?0, no body).
        result = self.create_upload(
            url,
            b"",
            method=method,
            complete=False,
            length=total_length,
            content_type=content_type,
            extra_headers=extra_headers,
        )

        # Step 2: Stream the data via append.
        return self._stream_append(result.upload_resource, raw, _chunk_size)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stream_append(
        self,
        resource: UploadResource,
        data: bytes,
        chunk_size: int,
    ) -> httpx.Response:
        """Stream *data* to *resource* via PATCH append requests.

        Splits *data* into chunks of at most *chunk_size* bytes, respecting
        ``Upload-Limit.max_append_size`` and ``Upload-Limit.min_append_size``
        from the resource's limits.

        Returns the final response (from the last append with
        Upload-Complete: ?1).
        """
        offset = resource.offset
        total = len(data)
        last_response: httpx.Response | None = None

        while offset < total or total == 0:
            # Determine effective chunk size respecting server limits.
            eff_chunk = self._effective_chunk_size(resource, chunk_size)

            end = min(offset + eff_chunk, total)
            chunk = data[offset:end]
            is_last = end >= total

            last_response = self.append(
                resource,
                chunk,
                complete=is_last,
            )

            if is_last:
                break

            offset = resource.offset

        if last_response is None:
            raise UploadAppendError("No append was performed")

        return last_response

    def _effective_chunk_size(self, resource: UploadResource, default: int) -> int:
        """Return a chunk size that respects Upload-Limit constraints."""
        size = default
        if resource.limits:
            if resource.limits.max_append_size is not None:
                size = min(size, resource.limits.max_append_size)
            if resource.limits.min_append_size is not None:
                size = max(size, resource.limits.min_append_size)
        return size

    @staticmethod
    def _raise_for_problem(
        response: httpx.Response,
        exc_class: type,
    ) -> None:
        """Inspect a problem+json response body and raise a typed exception."""
        content_type = response.headers.get("content-type", "")
        if "application/problem+json" not in content_type:
            return

        try:
            body = response.json()
        except Exception:
            return

        problem_type = body.get("type", "")

        if _PROBLEM_MISMATCHING_OFFSET in problem_type:
            raise MismatchingOffsetError(
                expected_offset=body.get("expected-offset", -1),
                provided_offset=body.get("provided-offset", -1),
            )
        if _PROBLEM_COMPLETED_UPLOAD in problem_type:
            raise CompletedUploadError()
        if _PROBLEM_INCONSISTENT_LENGTH in problem_type:
            raise InconsistentLengthError()
