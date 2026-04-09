"""FastAPI integration for the RUFH server.

Example usage::

    from fastapi import FastAPI
    from pyrufh.server import InMemoryRufhServer
    from pyrufh.server.fastapi import setup_fastapi_routes

    app = FastAPI()
    server = InMemoryRufhServer(base_url="http://localhost:8000")
    setup_fastapi_routes(app, server)

Or use the standalone ASGI app::

    from pyrufh.server.fastapi import make_fastapi_app

    app = make_fastapi_app()
"""

from __future__ import annotations

import logging

try:
    from fastapi import FastAPI, Request, Response
except ImportError as exc:
    raise ImportError(
        "FastAPI is required for FastAPI integration. Install with: pip install pyrufh[fastapi]"
    ) from exc

from ..core import (
    DigestMismatchError,
    InMemoryRufhServer,
    RufhServer,
    UploadAlreadyCompleteError,
    UploadLengthMismatchError,
    UploadNotFoundError,
    UploadOffsetMismatchError,
)
from ..headers import (
    CONTENT_TYPE_PARTIAL_UPLOAD,
    UploadLimits,
    build_repr_digest_header,
    build_upload_complete_header,
    build_upload_length_header,
    build_upload_offset_header,
    draft_interop_headers,
    parse_content_digest,
    parse_repr_digest,
    parse_upload_complete,
    parse_upload_length,
    parse_upload_offset,
    parse_want_content_digest,
    parse_want_repr_digest,
)

logger = logging.getLogger(__name__)


async def create_upload(request: Request, upload_uri: str) -> Response:
    """Handle upload creation (POST/PUT to /uploads/{upload_uri})."""
    body = await request.body()
    complete_header = parse_upload_complete(request.headers)
    length_header = parse_upload_length(request.headers)
    content_digest = parse_content_digest(request.headers)
    repr_digest = parse_repr_digest(request.headers)
    want_repr_digest = parse_want_repr_digest(request.headers)
    want_content_digest = parse_want_content_digest(request.headers)

    complete = complete_header if complete_header is not None else False

    uri = f"{request.state.base_url}/uploads/{upload_uri}"
    try:
        upload, status = request.state.server.create_upload(
            body,
            method=request.method,
            complete=complete,
            length=length_header,
            content_type=request.headers.get("content-type"),
            uri=uri,
            content_digest=content_digest,
            repr_digest=repr_digest,
            want_repr_digest=want_repr_digest,
            want_content_digest=want_content_digest,
        )
    except DigestMismatchError as e:
        import base64

        body = (
            f'{{"type":"https://iana.org/assignments/http-problem-types#digest-mismatch",'
            f'"title":"Digest mismatch","algorithm":"{e.algorithm}",'
            f'"expected":"{base64.b64encode(e.expected).decode()}",'
            f'"actual":"{base64.b64encode(e.actual).decode()}"}}'
        ).encode()
        return Response(content=body, status_code=400, media_type="application/problem+json")

    headers = {
        **draft_interop_headers(),
        "Upload-Complete": build_upload_complete_header(upload.complete),
        "Upload-Offset": build_upload_offset_header(upload.offset),
        "Location": upload.uri,
    }

    if upload.length is not None:
        headers["Upload-Length"] = build_upload_length_header(upload.length)

    if upload.repr_digest is not None:
        headers["Repr-Digest"] = build_repr_digest_header(upload.repr_digest)

    if upload.limits is not None:
        limit_parts = []
        if upload.limits.max_size is not None:
            limit_parts.append(f"max-size={upload.limits.max_size}")
        if upload.limits.min_size is not None:
            limit_parts.append(f"min-size={upload.limits.min_size}")
        if upload.limits.max_append_size is not None:
            limit_parts.append(f"max-append-size={upload.limits.max_append_size}")
        if upload.limits.min_append_size is not None:
            limit_parts.append(f"min-append-size={upload.limits.min_append_size}")
        if upload.limits.max_age is not None:
            limit_parts.append(f"max-age={upload.limits.max_age}")
        if limit_parts:
            headers["Upload-Limit"] = ", ".join(limit_parts)

    return Response(content=b"", status_code=status, headers=headers)


async def get_offset(request: Request, upload_uri: str) -> Response:
    """Handle offset retrieval (HEAD to /uploads/{upload_uri})."""
    try:
        upload = request.state.server.get_offset(upload_uri)
    except UploadNotFoundError:
        return Response(
            content=b"",
            status_code=404,
            media_type="application/problem+json",
        )

    headers = {
        **draft_interop_headers(),
        "Upload-Complete": build_upload_complete_header(upload.complete),
        "Upload-Offset": build_upload_offset_header(upload.offset),
    }

    if upload.length is not None:
        headers["Upload-Length"] = build_upload_length_header(upload.length)

    if upload.limits is not None:
        limit_parts = []
        if upload.limits.max_size is not None:
            limit_parts.append(f"max-size={upload.limits.max_size}")
        if upload.limits.min_size is not None:
            limit_parts.append(f"min-size={upload.limits.min_size}")
        if upload.limits.max_append_size is not None:
            limit_parts.append(f"max-append-size={upload.limits.max_append_size}")
        if upload.limits.min_append_size is not None:
            limit_parts.append(f"min-append-size={upload.limits.min_append_size}")
        if upload.limits.max_age is not None:
            limit_parts.append(f"max-age={upload.limits.max_age}")
        if limit_parts:
            headers["Upload-Limit"] = ", ".join(limit_parts)

    return Response(content=b"", status_code=200, headers=headers)


async def append_upload(request: Request, upload_uri: str) -> Response:
    """Handle upload append (PATCH to /uploads/{upload_uri})."""
    content_type = request.headers.get("Content-Type", "")
    if content_type != CONTENT_TYPE_PARTIAL_UPLOAD:
        return Response(
            content=b"",
            status_code=415,
            media_type="application/problem+json",
        )

    body = await request.body()
    offset_header = parse_upload_offset(request.headers)
    complete_header = parse_upload_complete(request.headers)
    length_header = parse_upload_length(request.headers)
    content_digest = parse_content_digest(request.headers)
    want_repr_digest = parse_want_repr_digest(request.headers)

    if offset_header is None:
        return Response(
            content=b'{"type":"https://iana.org/assignments/http-problem-types#upload-offset-required","title":"Upload-Offset header required"}',
            status_code=400,
            media_type="application/problem+json",
        )

    try:
        complete = complete_header if complete_header is not None else False
        upload = request.state.server.append(
            upload_uri,
            body,
            upload_offset=offset_header,
            complete=complete,
            upload_length=length_header,
            content_digest=content_digest,
            want_repr_digest=want_repr_digest,
        )
    except UploadNotFoundError:
        return Response(
            content=b"",
            status_code=404,
            media_type="application/problem+json",
        )
    except UploadOffsetMismatchError as e:
        body = (
            f'{{"type":"https://iana.org/assignments/http-problem-types#mismatching-upload-offset",'
            f'"title":"Offset mismatch","expected-offset":{e.expected_offset},'
            f'"provided-offset":{e.provided_offset}}}'
        ).encode()
        return Response(content=body, status_code=409, media_type="application/problem+json")
    except UploadAlreadyCompleteError:
        return Response(
            content=b'{"type":"https://iana.org/assignments/http-problem-types#completed-upload","title":"Upload already complete"}',
            status_code=400,
            media_type="application/problem+json",
        )
    except UploadLengthMismatchError:
        return Response(
            content=b'{"type":"https://iana.org/assignments/http-problem-types#inconsistent-upload-length","title":"Inconsistent upload length"}',
            status_code=400,
            media_type="application/problem+json",
        )
    except DigestMismatchError as e:
        import base64

        body = (
            f'{{"type":"https://iana.org/assignments/http-problem-types#digest-mismatch",'
            f'"title":"Digest mismatch","algorithm":"{e.algorithm}",'
            f'"expected":"{base64.b64encode(e.expected).decode()}",'
            f'"actual":"{base64.b64encode(e.actual).decode()}"}}'
        ).encode()
        return Response(content=body, status_code=400, media_type="application/problem+json")

    headers = {
        **draft_interop_headers(),
        "Upload-Complete": build_upload_complete_header(upload.complete),
        "Upload-Offset": build_upload_offset_header(upload.offset),
    }

    if upload.length is not None:
        headers["Upload-Length"] = build_upload_length_header(upload.length)

    if upload.repr_digest is not None:
        headers["Repr-Digest"] = build_repr_digest_header(upload.repr_digest)

    return Response(content=b"", status_code=200, headers=headers)


async def cancel_upload(request: Request, upload_uri: str) -> Response:
    """Handle upload cancellation (DELETE to /uploads/{upload_uri})."""
    try:
        request.state.server.cancel(upload_uri)
    except UploadNotFoundError:
        return Response(
            content=b"",
            status_code=404,
            media_type="application/problem+json",
        )

    return Response(
        content=b"",
        status_code=204,
        headers=draft_interop_headers(),
    )


def setup_fastapi_routes(app: FastAPI, server: RufhServer) -> None:
    """Register RUFH routes on a FastAPI application.

    Registers these routes:
      - POST/PUT  /uploads/{upload_uri}  - Upload creation
      - HEAD      /uploads/{upload_uri}  - Offset retrieval
      - PATCH     /uploads/{upload_uri}  - Upload append
      - DELETE    /uploads/{upload_uri}  - Upload cancellation

    Parameters
    ----------
    app:
        The FastAPI application to register routes on.
    server:
        The RUFH server instance to use.
    """

    @app.middleware("http")
    async def attach_server(request: Request, call_next):
        request.state.server = server
        request.state.base_url = server._base_url
        return await call_next(request)

    app.add_api_route(
        "/uploads/{upload_uri:path}",
        create_upload,
        methods=["POST", "PUT"],
        name="create_upload",
    )
    app.add_api_route(
        "/uploads/{upload_uri:path}",
        get_offset,
        methods=["HEAD"],
        name="get_offset",
    )
    app.add_api_route(
        "/uploads/{upload_uri:path}",
        append_upload,
        methods=["PATCH"],
        name="append_upload",
    )
    app.add_api_route(
        "/uploads/{upload_uri:path}",
        cancel_upload,
        methods=["DELETE"],
        name="cancel_upload",
    )


def make_fastapi_app(
    server: RufhServer | None = None,
    *,
    limits: UploadLimits | None = None,
    base_url: str = "http://localhost:8000",
) -> FastAPI:
    """Create a FastAPI application with RUFH routes configured.

    Parameters
    ----------
    server:
        The RUFH server instance. If None, creates an InMemoryRufhServer.
    limits:
        Optional upload limits to apply.
    base_url:
        Base URL for the server.

    Returns
    -------
    FastAPI
        Configured FastAPI application.
    """
    if server is None:
        server = InMemoryRufhServer(limits=limits, base_url=base_url)

    app = FastAPI()
    setup_fastapi_routes(app, server)
    return app
