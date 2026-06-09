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
    import flask  # ty: ignore
except ImportError as exc:
    raise ImportError(
        "Flask is required for Flask integration. Install with: pip install pyrufh[flask]"
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

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

logger = logging.getLogger(__name__)


def _handle_create_upload(
    server: RufhServer, upload_uri: str | None, require_authentication: Callable[[flask.Request], None] | None
) -> flask.Response:
    if require_authentication is not None:
        require_authentication(flask.request)

    data = flask.request.get_data()
    headers = cast("Mapping[str, str]", flask.request.headers)
    complete_header = parse_upload_complete(headers)
    length_header = parse_upload_length(headers)
    content_digest = parse_content_digest(headers)
    repr_digest = parse_repr_digest(headers)
    want_repr_digest = parse_want_repr_digest(headers)
    want_content_digest = parse_want_content_digest(headers)

    complete = complete_header if complete_header is not None else False

    uri = f"{server._base_url}/uploads"
    if upload_uri:
        uri = f"{uri}/{upload_uri}"

    try:
        upload, status = server.create_upload(
            data,
            method=flask.request.method,
            complete=complete,
            length=length_header,
            content_type=flask.request.content_type,
            uri=uri,
            content_digest=content_digest,
            repr_digest=repr_digest,
            want_repr_digest=want_repr_digest,
            want_content_digest=want_content_digest,
        )
    except DigestMismatchError as e:
        import base64

        return flask.make_response(
            (
                f'{{"type":"https://iana.org/assignments/http-problem-types#digest-mismatch","title":"Digest mismatch","algorithm":"{e.algorithm}","expected":"{base64.b64encode(e.expected).decode()}","actual":"{base64.b64encode(e.actual).decode()}"}}',
                400,
                {"Content-Type": "application/problem+json"},
            )
        )

    response_headers = {
        **draft_interop_headers(),
        "Upload-Complete": build_upload_complete_header(upload.complete),
        "Upload-Offset": build_upload_offset_header(upload.offset),
        "Location": upload.uri,
    }

    if upload.length is not None:
        response_headers["Upload-Length"] = build_upload_length_header(upload.length)

    if upload.repr_digest is not None:
        response_headers["Repr-Digest"] = build_repr_digest_header(upload.repr_digest)

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
            response_headers["Upload-Limit"] = ", ".join(limit_parts)

    return flask.make_response(("", status, response_headers))


def _handle_get_offset(
    server: RufhServer, upload_uri: str | None, require_authentication: Callable[[flask.Request], None] | None
) -> flask.Response:
    if require_authentication is not None:
        require_authentication(flask.request)

    if upload_uri is None:
        return flask.make_response(
            (
                "",
                404,
                {"Content-Type": "application/problem+json"},
            )
        )

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


def _handle_append_upload(
    server: RufhServer, upload_uri: str | None, require_authentication: Callable[[flask.Request], None] | None
) -> flask.Response:
    if require_authentication is not None:
        require_authentication(flask.request)

    if upload_uri is None:
        return flask.make_response(
            (
                "",
                404,
                {"Content-Type": "application/problem+json"},
            )
        )

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
    content_digest = parse_content_digest(headers)
    want_repr_digest = parse_want_repr_digest(headers)

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
            content_digest=content_digest,
            want_repr_digest=want_repr_digest,
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
    except DigestMismatchError as e:
        import base64

        return flask.make_response(
            (
                f'{{"type":"https://iana.org/assignments/http-problem-types#digest-mismatch","title":"Digest mismatch","algorithm":"{e.algorithm}","expected":"{base64.b64encode(e.expected).decode()}","actual":"{base64.b64encode(e.actual).decode()}"}}',
                400,
                {"Content-Type": "application/problem+json"},
            )
        )

    response_headers = {
        **draft_interop_headers(),
        "Upload-Complete": build_upload_complete_header(upload.complete),
        "Upload-Offset": build_upload_offset_header(upload.offset),
    }

    if upload.length is not None:
        response_headers["Upload-Length"] = build_upload_length_header(upload.length)

    if upload.repr_digest is not None:
        response_headers["Repr-Digest"] = build_repr_digest_header(upload.repr_digest)

    return flask.make_response(("", 200, response_headers))


def _handle_cancel_upload(
    server: RufhServer, upload_uri: str | None, require_authentication: Callable[[flask.Request], None] | None
) -> flask.Response:
    if require_authentication is not None:
        require_authentication(flask.request)

    if upload_uri is None:
        return flask.make_response(
            (
                "",
                404,
                {"Content-Type": "application/problem+json"},
            )
        )

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


def setup_flask_routes(
    app: flask.Flask | flask.Blueprint,
    server: RufhServer,
    path: str = "/uploads",
    require_authentication: Callable[[flask.Request], None] | None = None,
) -> None:
    """Mount RUFH endpoints on a Flask app or Blueprint.

    Parameters
    ----------
    app:
        The Flask application or Blueprint to mount the routes on.
    server:
        The initialized RUFH server instance.
    path:
        The base path for the upload endpoints.
    require_authentication:
        An optional callable that takes the Flask request object and
        raises an exception (e.g. `werkzeug.exceptions.Unauthorized`)
        if the request is not authenticated. Called before any RUFH logic.
    """

    @app.route(f"{path}", methods=["POST", "PUT", "OPTIONS"])
    @app.route(f"{path}/<path:upload_id>", methods=["POST", "PUT", "PATCH", "HEAD", "DELETE", "OPTIONS"])
    def handle_rufh_request(upload_id: str | None = None) -> flask.Response:
        if flask.request.method == "OPTIONS":
            return flask.make_response(("", 204, {"Access-Control-Allow-Methods": "POST, PUT, PATCH, HEAD, DELETE, OPTIONS"}))
        elif flask.request.method in ("POST", "PUT"):
            return _handle_create_upload(server, upload_id, require_authentication)
        elif flask.request.method == "HEAD":
            return _handle_get_offset(server, upload_id, require_authentication)
        elif flask.request.method == "PATCH":
            return _handle_append_upload(server, upload_id, require_authentication)
        elif flask.request.method == "DELETE":
            return _handle_cancel_upload(server, upload_id, require_authentication)

        return flask.make_response(("", 405))


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
