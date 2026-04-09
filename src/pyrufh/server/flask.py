"""Flask integration for the RUFH server.

Example usage::

    from flask import Flask
    from pyrufh.server import InMemoryRufhServer
    from pyrufh.server.flask import make_flask_app

    app = Flask(__name__)
    server = InMemoryRufhServer(base_url="http://localhost:5000")
    setup_flask_routes(app, server)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

try:
    import flask  # ty:ignore[unresolved-import]
except ImportError as exc:
    raise ImportError(
        "Flask is required for Flask integration. Install with: pip install pyrufh[flask]"
    ) from exc

from ..core import (
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
    build_upload_complete_header,
    build_upload_length_header,
    build_upload_offset_header,
    draft_interop_headers,
    parse_upload_complete,
    parse_upload_length,
    parse_upload_offset,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)


def setup_flask_routes(app: flask.Flask, server: RufhServer) -> None:
    """Register RUFH routes on a Flask application.

    Registers these routes:
      - POST/PUT  /uploads/*  - Upload creation
      - HEAD      /uploads/*  - Offset retrieval
      - PATCH     /uploads/*  - Upload append
      - DELETE    /uploads/*  - Upload cancellation

    Parameters
    ----------
    app:
        The Flask application to register routes on.
    server:
        The RUFH server instance to use.
    """

    @app.route("/uploads/<path:upload_uri>", methods=["POST", "PUT"])
    def create_upload(upload_uri: str):
        data = flask.request.get_data()
        headers = cast("Mapping[str, str]", flask.request.headers)
        complete_header = parse_upload_complete(headers)
        length_header = parse_upload_length(headers)

        complete = complete_header if complete_header is not None else False

        uri = f"{server._base_url}/uploads/{upload_uri}"
        upload, status = server.create_upload(
            data,
            method=flask.request.method,
            complete=complete,
            length=length_header,
            content_type=flask.request.content_type,
            uri=uri,
        )

        headers = {
            **draft_interop_headers(),
            "Upload-Complete": build_upload_complete_header(upload.complete),
            "Upload-Offset": build_upload_offset_header(upload.offset),
            "Location": upload.uri,
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

        return flask.make_response(("", status, headers))

    @app.route("/uploads/<path:upload_uri>", methods=["HEAD"])
    def get_offset(upload_uri: str):
        try:
            upload = server.get_offset(upload_uri)
        except UploadNotFoundError:
            return flask.make_response(
                (
                    "",
                    404,
                    {"Content-Type": "application/problem+json"},
                )
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

        return flask.make_response(("", 200, headers))

    @app.route("/uploads/<path:upload_uri>", methods=["PATCH"])
    def append_upload(upload_uri: str):
        content_type = flask.request.headers.get("Content-Type", "")
        if content_type != CONTENT_TYPE_PARTIAL_UPLOAD:
            return flask.make_response(
                (
                    "",
                    415,
                    {"Content-Type": "application/problem+json"},
                )
            )

        data = flask.request.get_data()
        headers = cast("Mapping[str, str]", flask.request.headers)
        offset_header = parse_upload_offset(headers)
        complete_header = parse_upload_complete(headers)
        length_header = parse_upload_length(headers)

        if offset_header is None:
            return flask.make_response(
                (
                    '{"type":"https://iana.org/assignments/http-problem-types#upload-offset-required","title":"Upload-Offset header required"}',
                    400,
                    {"Content-Type": "application/problem+json"},
                )
            )

        try:
            complete = complete_header if complete_header is not None else False
            upload = server.append(
                upload_uri,
                data,
                upload_offset=offset_header,
                complete=complete,
                upload_length=length_header,
            )
        except UploadNotFoundError:
            return flask.make_response(
                (
                    "",
                    404,
                    {"Content-Type": "application/problem+json"},
                )
            )
        except UploadOffsetMismatchError as e:
            return flask.make_response(
                (
                    f'{{"type":"https://iana.org/assignments/http-problem-types#mismatching-upload-offset","title":"Offset mismatch","expected-offset":{e.expected_offset},"provided-offset":{e.provided_offset}}}',
                    409,
                    {"Content-Type": "application/problem+json"},
                )
            )
        except UploadAlreadyCompleteError:
            return flask.make_response(
                (
                    '{"type":"https://iana.org/assignments/http-problem-types#completed-upload","title":"Upload already complete"}',
                    400,
                    {"Content-Type": "application/problem+json"},
                )
            )
        except UploadLengthMismatchError:
            return flask.make_response(
                (
                    '{"type":"https://iana.org/assignments/http-problem-types#inconsistent-upload-length","title":"Inconsistent upload length"}',
                    400,
                    {"Content-Type": "application/problem+json"},
                )
            )

        headers = {
            **draft_interop_headers(),
            "Upload-Complete": build_upload_complete_header(upload.complete),
            "Upload-Offset": build_upload_offset_header(upload.offset),
        }

        if upload.length is not None:
            headers["Upload-Length"] = build_upload_length_header(upload.length)

        return flask.make_response(("", 200, headers))

    @app.route("/uploads/<path:upload_uri>", methods=["DELETE"])
    def cancel_upload(upload_uri: str):
        try:
            server.cancel(upload_uri)
        except UploadNotFoundError:
            return flask.make_response(
                (
                    "",
                    404,
                    {"Content-Type": "application/problem+json"},
                )
            )

        return flask.make_response(("", 204, draft_interop_headers()))


def make_flask_app(
    server: RufhServer | None = None,
    *,
    limits: UploadLimits | None = None,
    base_url: str = "http://localhost:5000",
) -> flask.Flask:
    """Create a Flask application with RUFH routes configured.

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
    flask.Flask
        Configured Flask application.
    """
    if server is None:
        server = InMemoryRufhServer(limits=limits, base_url=base_url)

    app = flask.Flask(__name__)
    setup_flask_routes(app, server)
    return app
