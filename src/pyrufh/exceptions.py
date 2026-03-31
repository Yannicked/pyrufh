"""Exceptions for the pyrufh resumable upload client."""

from __future__ import annotations


class RufhError(Exception):
    """Base exception for all pyrufh errors."""


class UploadError(RufhError):
    """Raised when an upload operation fails."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class UploadCreationError(UploadError):
    """Raised when the initial upload creation request fails."""


class OffsetRetrievalError(UploadError):
    """Raised when retrieving the upload offset fails."""


class UploadAppendError(UploadError):
    """Raised when appending data to an upload fails."""


class UploadCancellationError(UploadError):
    """Raised when cancelling an upload fails."""


class MismatchingOffsetError(UploadAppendError):
    """Raised when the Upload-Offset does not match the server's expected offset.

    Corresponds to problem type:
    https://iana.org/assignments/http-problem-types#mismatching-upload-offset
    """

    def __init__(
        self,
        expected_offset: int,
        provided_offset: int,
    ) -> None:
        super().__init__(
            f"Upload offset mismatch: server expected {expected_offset}, "
            f"client provided {provided_offset}",
            status_code=409,
        )
        self.expected_offset = expected_offset
        self.provided_offset = provided_offset


class CompletedUploadError(UploadAppendError):
    """Raised when trying to append to an already-completed upload.

    Corresponds to problem type:
    https://iana.org/assignments/http-problem-types#completed-upload
    """

    def __init__(self) -> None:
        super().__init__("Upload is already completed", status_code=400)


class InconsistentLengthError(UploadError):
    """Raised when upload length values are inconsistent.

    Corresponds to problem type:
    https://iana.org/assignments/http-problem-types#inconsistent-upload-length
    """

    def __init__(self) -> None:
        super().__init__("Inconsistent upload length values", status_code=400)


class UploadLimitExceededError(RufhError):
    """Raised when an upload limit would be exceeded."""


class UploadInterruptedError(RufhError):
    """Raised when an upload is interrupted and cannot be resumed."""


class UploadNotResumableError(RufhError):
    """Raised when an interrupted upload cannot be resumed (no upload resource URI available)."""
