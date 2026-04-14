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

To receive 104 (Upload Resumption Supported) interim responses use
:class:`~pyrufh.transport.InterimCapturingTransport`::

    from pyrufh import RufhClient, InterimCapturingTransport, InterimResponse

    received: list[InterimResponse] = []
    transport = InterimCapturingTransport(on_interim=received.append)
    import httpx

    with RufhClient(client=httpx.Client(transport=transport)) as client:
        response = client.upload("https://example.com/upload", b"data")
"""

from __future__ import annotations

import importlib.util

from .client import DEFAULT_CHUNK_SIZE, RufhClient
from .exceptions import (
    CompletedUploadError,
    ContentDigestMismatchError,
    InconsistentLengthError,
    MismatchingOffsetError,
    OffsetRetrievalError,
    RepresentationDigestMismatchError,
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
    DIGEST_ALGORITHMS,
    DRAFT_INTEROP_VERSION,
    UploadLimits,
    build_content_digest_header,
    build_repr_digest_header,
    build_want_content_digest_header,
    build_want_repr_digest_header,
    compute_digest,
    parse_content_digest,
    parse_repr_digest,
    parse_want_content_digest,
    parse_want_repr_digest,
)
from .models import UploadCreationResult, UploadResource
from .server import (
    InMemoryRufhServer,
    RufhServer,
    UploadAlreadyCompleteError,
    UploadLengthMismatchError,
    UploadNotFoundError,
    UploadOffsetMismatchError,
)
from .transport import InterimCapturingTransport, InterimResponse

__all__ = [
    "CONTENT_TYPE_PARTIAL_UPLOAD",
    "DEFAULT_CHUNK_SIZE",
    "DIGEST_ALGORITHMS",
    "DRAFT_INTEROP_VERSION",
    "CompletedUploadError",
    "ContentDigestMismatchError",
    "InMemoryRufhServer",
    "InconsistentLengthError",
    "InterimCapturingTransport",
    "InterimResponse",
    "MismatchingOffsetError",
    "OffsetRetrievalError",
    "RepresentationDigestMismatchError",
    "RufhClient",
    "RufhError",
    "RufhServer",
    "UploadAlreadyCompleteError",
    "UploadAppendError",
    "UploadCancellationError",
    "UploadCreationError",
    "UploadCreationResult",
    "UploadError",
    "UploadInterruptedError",
    "UploadLengthMismatchError",
    "UploadLimitExceededError",
    "UploadLimits",
    "UploadNotFoundError",
    "UploadNotResumableError",
    "UploadOffsetMismatchError",
    "UploadResource",
    "build_content_digest_header",
    "build_repr_digest_header",
    "build_want_content_digest_header",
    "build_want_repr_digest_header",
    "compute_digest",
    "parse_content_digest",
    "parse_repr_digest",
    "parse_want_content_digest",
    "parse_want_repr_digest",
]

if importlib.util.find_spec("fastapi") is not None:
    from .server.fastapi import make_fastapi_app, setup_fastapi_routes  # noqa: F401

    __all__.extend(["make_fastapi_app", "setup_fastapi_routes"])

if importlib.util.find_spec("flask") is not None:
    from .server.flask import make_flask_app, setup_flask_routes  # noqa: F401

    __all__.extend(["make_flask_app", "setup_flask_routes"])
