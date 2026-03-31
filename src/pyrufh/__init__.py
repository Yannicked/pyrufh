"""pyrufh - Python client for the Resumable Uploads for HTTP protocol.

Implements draft-ietf-httpbis-resumable-upload-11.

Basic usage::

    from pyrufh import RufhClient

    with RufhClient() as client:
        response = client.upload("https://example.com/upload", data=open("file.bin", "rb"))
        print(response.status_code)

For chunked / careful upload::

    with RufhClient() as client:
        response = client.upload_carefully(
            "https://example.com/upload",
            data=open("large_file.bin", "rb"),
            chunk_size=5 * 1024 * 1024,  # 5 MiB chunks
        )
"""

from .client import DEFAULT_CHUNK_SIZE, RufhClient
from .exceptions import (
    CompletedUploadError,
    InconsistentLengthError,
    MismatchingOffsetError,
    OffsetRetrievalError,
    RufhError,
    UploadAppendError,
    UploadCancellationError,
    UploadCreationError,
    UploadError,
    UploadInterruptedError,
    UploadLimitExceededError,
    UploadNotResumableError,
)
from .headers import (
    CONTENT_TYPE_PARTIAL_UPLOAD,
    DRAFT_INTEROP_VERSION,
    UploadLimits,
)
from .models import UploadCreationResult, UploadResource

__all__ = [
    "CONTENT_TYPE_PARTIAL_UPLOAD",
    "DEFAULT_CHUNK_SIZE",
    "DRAFT_INTEROP_VERSION",
    "CompletedUploadError",
    "InconsistentLengthError",
    "MismatchingOffsetError",
    "OffsetRetrievalError",
    "RufhClient",
    "RufhError",
    "UploadAppendError",
    "UploadCancellationError",
    "UploadCreationError",
    "UploadCreationResult",
    "UploadError",
    "UploadInterruptedError",
    "UploadLimitExceededError",
    "UploadLimits",
    "UploadNotResumableError",
    "UploadResource",
]
