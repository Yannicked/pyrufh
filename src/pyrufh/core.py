"""Core server-side implementation for the Resumable Uploads for HTTP protocol.

Implements draft-ietf-httpbis-resumable-upload-11 server behaviour:
  - Upload creation  (§4.2)
  - Offset retrieval (§4.3)
  - Upload append    (§4.4)
  - Upload cancellation (§4.5)
  - 104 Upload Resumption Supported interim response handling (§5)
  - Integrity Digests (§10) via RFC 9530 Digest Fields
"""

from __future__ import annotations

import logging
import secrets
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    from .headers import UploadLimits

logger = logging.getLogger(__name__)


class UploadNotFoundError(Exception):
    """Raised when an upload resource is not found."""

    def __init__(self, upload_id: str) -> None:
        super().__init__(f"Upload not found: {upload_id}")
        self.upload_id = upload_id


class UploadOffsetMismatchError(Exception):
    """Raised when the client's Upload-Offset doesn't match the server's."""

    def __init__(self, expected: int, provided: int) -> None:
        super().__init__(f"Offset mismatch: server has {expected}, client sent {provided}")
        self.expected_offset = expected
        self.provided_offset = provided


class UploadAlreadyCompleteError(Exception):
    """Raised when trying to append to an already completed upload."""


class UploadLengthMismatchError(Exception):
    """Raised when the Upload-Length doesn't match the provided data."""


class DigestMismatchError(Exception):
    """Raised when a digest header doesn't match the computed digest."""

    def __init__(
        self,
        header_name: str,
        algorithm: str,
        expected: bytes,
        actual: bytes,
    ) -> None:
        import base64

        super().__init__(
            f"{header_name} mismatch for {algorithm}: "
            f"expected {base64.b64encode(expected).decode()}, "
            f"got {base64.b64encode(actual).decode()}"
        )
        self.header_name = header_name
        self.algorithm = algorithm
        self.expected = expected
        self.actual = actual


@dataclass
class Upload:
    """Represents a server-side upload resource."""

    upload_id: str
    uri: str
    data: bytearray = field(default_factory=bytearray)
    offset: int = 0
    complete: bool = False
    length: int | None = None
    limits: UploadLimits | None = None
    max_age: int | None = None
    content_digest: dict[str, bytes] | None = None
    repr_digest: dict[str, bytes] | None = None

    def append(self, chunk: bytes, expected_offset: int) -> None:
        if self.complete:
            raise UploadAlreadyCompleteError()
        if expected_offset != self.offset:
            raise UploadOffsetMismatchError(expected=self.offset, provided=expected_offset)
        self.data.extend(chunk)
        self.offset += len(chunk)

    def finish(self, length: int | None = None) -> None:
        if length is not None and self.length is not None and length != self.length:
            raise UploadLengthMismatchError()
        if length is not None:
            self.length = length
        if self.length is not None and self.offset != self.length:
            raise UploadLengthMismatchError()
        self.complete = True


class RufhServer(ABC):
    """Abstract base class for RUFH server implementations.

    Subclass this to provide storage backends (filesystem, S3, memory, etc.)
    and integrate with your web framework of choice.
    """

    def __init__(
        self,
        *,
        limits: UploadLimits | None = None,
        base_url: str = "http://localhost",
    ) -> None:
        self._limits = limits
        self._base_url = base_url.rstrip("/")
        self._uploads: dict[str, Upload] = {}
        self._lock = threading.Lock()

    def _generate_upload_id(self) -> str:
        return secrets.token_urlsafe(16)

    def _build_uri(self, upload_id: str) -> str:
        return f"{self._base_url}/uploads/{upload_id}"

    def _get_upload(self, uri: str) -> Upload | None:
        for upload in self._uploads.values():
            if upload.uri == uri:
                return upload
        upload_id = uri.rsplit("/", 1)[-1]
        return self._uploads.get(upload_id)

    @abstractmethod
    def _store_upload(self, upload: Upload) -> None:
        """Persist an upload (called after mutations)."""
        pass

    @abstractmethod
    def _delete_upload(self, upload_id: str) -> None:
        """Remove an upload from storage."""
        pass

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
        """Create a new upload resource (§4.2).

        Parameters
        ----------
        uri:
            Optional URI for the upload. If not provided, one is auto-generated.
            When provided, it is used as both the upload ID (last path segment)
            and the full URI.
        content_digest:
            Content-Digest header value (RFC 9530 §2) for verifying request content.
        repr_digest:
            Repr-Digest header value (RFC 9530 §3) for verifying full representation.
        want_repr_digest:
            Want-Repr-Digest header value (RFC 9530 §4) specifying preferred algorithms.
        want_content_digest:
            Want-Content-Digest header value (RFC 9530 §4) specifying preferred algorithms.

        Returns
        -------
        tuple[Upload, int]
            The created Upload and the response status code.
        """
        from .headers import compute_digest

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

        content_length = len(body)
        inferred_length = length

        if complete and inferred_length is None and content_length > 0:
            inferred_length = content_length

        if uri is not None:
            upload_id = uri.rsplit("/", 1)[-1]
        else:
            upload_id = self._generate_upload_id()
            uri = self._build_uri(upload_id)

        computed_repr_digest: dict[str, bytes] | None = None
        if want_repr_digest and complete and len(body) > 0:
            computed_repr_digest = {}
            for alg in sorted(want_repr_digest.keys()):
                if want_repr_digest[alg] > 0:
                    computed_repr_digest[alg] = compute_digest(alg, body)

        upload = Upload(
            upload_id=upload_id,
            uri=uri,
            data=bytearray(body),
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

        self._store_upload(upload)

        status = 200 if complete and upload.offset == upload.length else 201

        return upload, status

    def get_offset(self, uri: str) -> Upload:
        """Retrieve the current upload offset (§4.3).

        Returns
        -------
        Upload
            The upload with updated offset.

        Raises
        ------
        UploadNotFoundError
            If the upload resource doesn't exist.
        """
        upload = self._get_upload(uri)
        if upload is None:
            raise UploadNotFoundError(uri)

        self._store_upload(upload)
        return upload

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

        Parameters
        ----------
        content_digest:
            Content-Digest header value (RFC 9530 §2) for verifying request content.
        want_repr_digest:
            Want-Repr-Digest header value (RFC 9530 §4) for computing representation digest.

        Returns
        -------
        Upload
            The updated upload.

        Raises
        ------
        UploadNotFoundError
            If the upload resource doesn't exist.
        UploadOffsetMismatchError
            If the provided offset doesn't match.
        UploadAlreadyCompleteError
            If the upload is already complete.
        """
        from .headers import compute_digest

        upload = self._get_upload(uri)
        if upload is None:
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

        upload.append(body, upload_offset)

        if complete:
            upload.finish(upload_length)
            if want_repr_digest and len(upload.data) > 0:
                computed_repr_digest = {}
                for alg in sorted(want_repr_digest.keys()):
                    if want_repr_digest[alg] > 0:
                        computed_repr_digest[alg] = compute_digest(alg, bytes(upload.data))
                upload.repr_digest = computed_repr_digest

        with self._lock:
            self._uploads[upload.upload_id] = upload

        self._store_upload(upload)
        return upload

    def cancel(self, uri: str) -> None:
        """Cancel an upload (§4.5).

        Raises
        ------
        UploadNotFoundError
            If the upload resource doesn't exist.
        """
        upload = self._get_upload(uri)
        if upload is None:
            raise UploadNotFoundError(uri)

        upload_id = upload.upload_id
        with self._lock:
            del self._uploads[upload_id]

        self._delete_upload(upload_id)

    def get_upload_info(self, uri: str) -> Upload | None:
        """Get upload info for a URI."""
        return self._get_upload(uri)


class InMemoryRufhServer(RufhServer):
    """In-memory RUFH server implementation for testing and development."""

    def _store_upload(self, upload: Upload) -> None:
        pass

    def _delete_upload(self, upload_id: str) -> None:
        pass
