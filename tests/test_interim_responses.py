"""Tests for 104 (Upload Resumption Supported) interim response handling.

These tests verify:
1. The _CapturingHTTP11Connection captures 104 responses and fires a callback.
2. RufhClient correctly processes captured 104 data (Location, Upload-Limit,
   Upload-Offset) into the returned results and resource state.
3. The InterimResponse helper class works correctly.
4. The on_interim_response callback on RufhClient is invoked.

Testing strategy
----------------
- For unit tests of the h11-level capture (_CapturingHTTP11Connection), we feed
  pre-built h11 events directly into the connection object.
- For RufhClient integration tests we use a custom httpx transport subclass that
  directly calls RufhClient._handle_interim_response() before returning the final
  response, accurately simulating what a real HTTP server would do.
"""

from __future__ import annotations

import typing

import h11
import httpcore
import httpx

from pyrufh import (
    InterimCapturingTransport,
    InterimResponse,
    RufhClient,
    UploadResource,
)
from pyrufh.transport import _CapturingHTTP11Connection

TARGET_URL = "https://example.com/upload"
UPLOAD_RESOURCE_URI = "https://example.com/uploads/abc123"


# ---------------------------------------------------------------------------
# Helper: build a RufhClient whose internal transport simulates 104 responses
# ---------------------------------------------------------------------------


def _make_rufh_with_simulated_interims(
    final_responses: list[httpx.Response],
    interims_per_request: list[list[InterimResponse]] | None = None,
    on_interim_response: object = None,
) -> RufhClient:
    """Return a RufhClient backed by a simulated transport.

    The transport fires *interims_per_request[i]* interim responses before
    returning *final_responses[i]*.  Both lists are consumed in order.

    The transport overrides ``handle_request`` on the pool-substituted
    InterimCapturingTransport.  It fires the interim callbacks via the pool's
    *_on_interim* slot, which was set to ``rufh._handle_interim_response`` by
    the RufhClient constructor.
    """
    resp_queue: list[httpx.Response] = list(final_responses)
    interim_queue: list[list[InterimResponse]] = list(interims_per_request or [])

    # We can't pass the on_interim at construction time to _TestTransport because
    # RufhClient sets it up during __init__ and we need to inject it into the pool.
    # Instead, we store a mutable reference and patch it after construction.
    _callback_ref: list[typing.Callable[[InterimResponse], None] | None] = [None]

    class _SimulatingTransport(InterimCapturingTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            # Fire any pending interim responses for this request.
            to_fire = interim_queue.pop(0) if interim_queue else []
            cb = _callback_ref[0]
            if cb is not None:
                for interim in to_fire:
                    cb(interim)  # type: ignore[operator]
            return resp_queue.pop(0)

    transport = _SimulatingTransport(on_interim=None)
    client_kwargs: dict = {}
    if on_interim_response is not None:
        client_kwargs["on_interim_response"] = on_interim_response

    rufh = RufhClient(
        client=httpx.Client(transport=transport),
        **client_kwargs,
    )

    # Wire up the callback: when transport fires interims they should go through
    # rufh._handle_interim_response so that _interim_responses is populated.
    _callback_ref[0] = rufh._handle_interim_response
    return rufh


# ---------------------------------------------------------------------------
# InterimResponse unit tests
# ---------------------------------------------------------------------------


class TestInterimResponse:
    def test_get_existing_header(self):
        interim = InterimResponse(headers=[(b"location", b"https://example.com/uploads/xyz")])
        assert interim.get("location") == "https://example.com/uploads/xyz"

    def test_get_case_insensitive(self):
        interim = InterimResponse(headers=[(b"Location", b"https://example.com/uploads/xyz")])
        assert interim.get("location") == "https://example.com/uploads/xyz"
        assert interim.get("LOCATION") == "https://example.com/uploads/xyz"

    def test_get_missing_returns_default(self):
        interim = InterimResponse(headers=[])
        assert interim.get("location") is None
        assert interim.get("location", "default") == "default"

    def test_get_upload_offset(self):
        interim = InterimResponse(headers=[(b"upload-offset", b"12345")])
        assert interim.get("upload-offset") == "12345"

    def test_get_upload_limit(self):
        interim = InterimResponse(headers=[(b"upload-limit", b"max-size=1000000")])
        assert interim.get("upload-limit") == "max-size=1000000"


# ---------------------------------------------------------------------------
# §5 / §4.2: 104 responses during upload creation
# ---------------------------------------------------------------------------


class TestInterimResponsesInCreation:
    def test_interim_responses_stored_in_result(self):
        """Captured 104 responses are available in UploadCreationResult."""
        interim = InterimResponse(
            headers=[
                (b"location", b"https://example.com/uploads/abc123"),
                (b"upload-limit", b"max-size=10000000"),
            ]
        )

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    201,
                    headers={
                        "Location": UPLOAD_RESOURCE_URI,
                        "Upload-Complete": "?0",
                    },
                )
            ],
            interims_per_request=[[interim]],
        )
        with rufh:
            result = rufh.create_upload(TARGET_URL, b"x" * 100, complete=False)

        assert len(result.interim_responses) == 1
        assert result.interim_responses[0].get("location") == "https://example.com/uploads/abc123"

    def test_location_from_104_used_when_final_response_omits_it(self):
        """When the final response is exempt from Location (Upload-Complete: ?1),
        the Location from the 104 interim response is used as the upload URI (§4.2.2)."""
        interim = InterimResponse(headers=[(b"location", b"https://example.com/uploads/from-104")])

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    200,
                    headers={
                        # No Location header - exempt per §4.2.2 when Upload-Complete: ?1
                        "Upload-Complete": "?1",
                        "Upload-Offset": "100",
                    },
                    content=b'{"ok": true}',
                )
            ],
            interims_per_request=[[interim]],
        )
        with rufh:
            result = rufh.create_upload(TARGET_URL, b"x" * 100, complete=True)

        assert result.complete is True
        assert result.upload_resource.uri == "https://example.com/uploads/from-104"
        assert len(result.interim_responses) == 1

    def test_limits_from_104_applied_when_final_response_omits_them(self):
        """Upload-Limit from 104 is applied when the final response doesn't include it."""
        interim = InterimResponse(
            headers=[
                (b"location", b"https://example.com/uploads/abc123"),
                (b"upload-limit", b"max-size=99999"),
            ]
        )

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    201,
                    headers={
                        "Location": UPLOAD_RESOURCE_URI,
                        "Upload-Complete": "?0",
                        # No Upload-Limit
                    },
                )
            ],
            interims_per_request=[[interim]],
        )
        with rufh:
            result = rufh.create_upload(TARGET_URL, b"x" * 50, complete=False)

        assert result.upload_resource.limits is not None
        assert result.upload_resource.limits.max_size == 99999

    def test_multiple_104_responses_all_captured(self):
        """Multiple 104 responses during creation are all stored on the result."""
        interim1 = InterimResponse(headers=[(b"location", b"https://example.com/uploads/abc")])
        interim2 = InterimResponse(headers=[(b"upload-offset", b"50000")])

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    200,
                    headers={
                        "Location": UPLOAD_RESOURCE_URI,
                        "Upload-Complete": "?1",
                        "Upload-Offset": "100",
                    },
                    content=b"ok",
                )
            ],
            interims_per_request=[[interim1, interim2]],
        )
        with rufh:
            result = rufh.create_upload(TARGET_URL, b"x" * 100, complete=True)

        assert len(result.interim_responses) == 2
        assert result.interim_responses[0].get("location") == "https://example.com/uploads/abc"
        assert result.interim_responses[1].get("upload-offset") == "50000"

    def test_no_interim_responses_when_server_sends_none(self):
        """When the server sends no 104, interim_responses is empty."""
        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    201,
                    headers={
                        "Location": UPLOAD_RESOURCE_URI,
                        "Upload-Complete": "?0",
                    },
                )
            ],
        )
        with rufh:
            result = rufh.create_upload(TARGET_URL, b"x" * 50, complete=False)

        assert result.interim_responses == []

    def test_final_response_location_takes_precedence_over_104(self):
        """When both the final response and 104 have Location, the final response wins."""
        interim = InterimResponse(headers=[(b"location", b"https://example.com/uploads/from-104")])

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    201,
                    headers={
                        "Location": UPLOAD_RESOURCE_URI,  # final response has Location
                        "Upload-Complete": "?0",
                    },
                )
            ],
            interims_per_request=[[interim]],
        )
        with rufh:
            result = rufh.create_upload(TARGET_URL, b"x" * 50, complete=False)

        # Final response's Location takes precedence
        assert result.upload_resource.uri == UPLOAD_RESOURCE_URI


# ---------------------------------------------------------------------------
# §5 / §4.4: 104 responses during upload append
# ---------------------------------------------------------------------------


class TestInterimResponsesInAppend:
    def test_offset_from_104_applied_during_append(self):
        """Upload-Offset from 104 during append updates the resource's offset."""
        interim = InterimResponse(headers=[(b"upload-offset", b"75000")])

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    204,
                    headers={"Upload-Complete": "?0"},
                    # No Upload-Offset in final response
                )
            ],
            interims_per_request=[[interim]],
        )
        resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=50000)
        with rufh:
            rufh.append(resource, b"x" * 25000, complete=False)

        # Offset should be the value from the 104 since final response omits it.
        assert resource.offset == 75000

    def test_final_response_offset_takes_precedence(self):
        """Final response Upload-Offset overrides 104 Upload-Offset."""
        interim = InterimResponse(headers=[(b"upload-offset", b"75000")])

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    204,
                    headers={"Upload-Complete": "?0", "Upload-Offset": "100000"},
                )
            ],
            interims_per_request=[[interim]],
        )
        resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=50000)
        with rufh:
            rufh.append(resource, b"x" * 50000, complete=False)

        # Final response's Upload-Offset (100000) takes precedence over 104 (75000).
        assert resource.offset == 100000

    def test_highest_104_offset_used_when_final_omits_offset(self):
        """When multiple 104s are received, the highest offset is applied."""
        interims = [
            InterimResponse(headers=[(b"upload-offset", b"60000")]),
            InterimResponse(headers=[(b"upload-offset", b"90000")]),
            InterimResponse(headers=[(b"upload-offset", b"75000")]),
        ]

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    204,
                    headers={"Upload-Complete": "?0"},
                    # No Upload-Offset in final response
                )
            ],
            interims_per_request=[interims],
        )
        resource = UploadResource(uri=UPLOAD_RESOURCE_URI, offset=50000)
        with rufh:
            rufh.append(resource, b"x" * 50000, complete=False)

        # Highest seen offset from 104s (90000) is applied.
        assert resource.offset == 90000


# ---------------------------------------------------------------------------
# on_interim_response callback on RufhClient
# ---------------------------------------------------------------------------


class TestOnInterimResponseCallback:
    def test_callback_invoked_for_each_104(self):
        """The on_interim_response callback is invoked for each captured 104."""
        received: list[InterimResponse] = []
        interims = [
            InterimResponse(headers=[(b"location", b"https://example.com/uploads/x")]),
            InterimResponse(headers=[(b"upload-offset", b"12345")]),
        ]

        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    201,
                    headers={"Location": UPLOAD_RESOURCE_URI, "Upload-Complete": "?0"},
                )
            ],
            interims_per_request=[interims],
            on_interim_response=received.append,
        )
        with rufh:
            rufh.create_upload(TARGET_URL, b"x" * 50, complete=False)

        assert len(received) == 2
        assert received[0].get("location") == "https://example.com/uploads/x"
        assert received[1].get("upload-offset") == "12345"

    def test_callback_not_required(self):
        """When no callback is set, interim responses are silently stored."""
        rufh = _make_rufh_with_simulated_interims(
            final_responses=[
                httpx.Response(
                    201,
                    headers={"Location": UPLOAD_RESOURCE_URI, "Upload-Complete": "?0"},
                )
            ],
            interims_per_request=[[InterimResponse(headers=[(b"upload-offset", b"100")])]],
        )
        with rufh:
            result = rufh.create_upload(TARGET_URL, b"x" * 50, complete=False)

        assert len(result.interim_responses) == 1


# ---------------------------------------------------------------------------
# InterimCapturingTransport: direct h11-level simulation
# ---------------------------------------------------------------------------


class TestCapturingHTTP11Connection:
    """Unit-test the _CapturingHTTP11Connection against a simulated h11 stream."""

    def _make_conn_with_events(
        self,
        events: list[object],
    ) -> tuple[_CapturingHTTP11Connection, list[InterimResponse]]:
        """Build a _CapturingHTTP11Connection with pre-seeded h11 events."""
        from unittest.mock import MagicMock

        from httpcore._models import Origin

        stream = MagicMock()
        stream.read = MagicMock(return_value=b"")

        captured: list[InterimResponse] = []
        origin = Origin(scheme=b"https", host=b"example.com", port=443)
        conn = _CapturingHTTP11Connection(
            origin=origin,
            stream=stream,
            on_interim=captured.append,
        )

        _events = list(events)

        def _fake_next_event() -> object:
            if _events:
                return _events.pop(0)
            return h11.NEED_DATA

        conn._h11_state.next_event = _fake_next_event  # type: ignore
        return conn, captured

    def _make_request(self) -> httpcore.Request:
        return httpcore.Request(
            method=b"POST",
            url="https://example.com/upload",
            headers=[],
        )

    def test_104_fires_callback(self):
        """_receive_response_headers fires the callback for a 104 event."""
        interim_event = h11.InformationalResponse(
            status_code=104,
            headers=[(b"location", b"https://example.com/uploads/abc")],
        )
        final_event = h11.Response(status_code=200, headers=[(b"content-length", b"0")])

        conn, captured = self._make_conn_with_events([interim_event, final_event])
        result = conn._receive_response_headers(self._make_request())

        assert result[1] == 200
        assert len(captured) == 1
        assert captured[0].get("location") == "https://example.com/uploads/abc"

    def test_non_104_informational_discarded(self):
        """Non-104 1xx responses are discarded without firing the callback."""
        info_100 = h11.InformationalResponse(status_code=100, headers=[])
        final_event = h11.Response(status_code=200, headers=[(b"content-length", b"0")])

        conn, captured = self._make_conn_with_events([info_100, final_event])
        result = conn._receive_response_headers(self._make_request())

        assert result[1] == 200
        assert len(captured) == 0  # 100 Continue does NOT fire the callback

    def test_multiple_104_all_captured(self):
        """Multiple 104 responses all fire the callback."""
        interim1 = h11.InformationalResponse(
            status_code=104,
            headers=[(b"location", b"https://example.com/uploads/abc")],
        )
        interim2 = h11.InformationalResponse(
            status_code=104,
            headers=[(b"upload-offset", b"50000")],
        )
        final_event = h11.Response(status_code=200, headers=[(b"content-length", b"0")])

        conn, captured = self._make_conn_with_events([interim1, interim2, final_event])
        conn._receive_response_headers(self._make_request())

        assert len(captured) == 2
        assert captured[0].get("location") == "https://example.com/uploads/abc"
        assert captured[1].get("upload-offset") == "50000"

    def test_104_with_upload_limit_headers(self):
        """104 responses with Upload-Limit header are captured with full header set."""
        interim_event = h11.InformationalResponse(
            status_code=104,
            headers=[
                (b"location", b"https://example.com/uploads/abc"),
                (b"upload-limit", b"max-size=1234567890"),
            ],
        )
        final_event = h11.Response(status_code=201, headers=[(b"content-length", b"0")])

        conn, captured = self._make_conn_with_events([interim_event, final_event])
        conn._receive_response_headers(self._make_request())

        assert len(captured) == 1
        assert captured[0].get("upload-limit") == "max-size=1234567890"
        assert captured[0].get("location") == "https://example.com/uploads/abc"

    def test_no_callback_set(self):
        """When no callback is set, 104 responses are discarded silently."""
        from unittest.mock import MagicMock

        from httpcore._models import Origin, Request

        stream = MagicMock()
        origin = Origin(scheme=b"https", host=b"example.com", port=443)
        conn = _CapturingHTTP11Connection(
            origin=origin,
            stream=stream,
            on_interim=None,  # no callback
        )

        _events = [
            h11.InformationalResponse(
                status_code=104,
                headers=[(b"location", b"https://example.com/uploads/abc")],
            ),
            h11.Response(status_code=200, headers=[(b"content-length", b"0")]),
        ]

        conn._h11_state.next_event = lambda: _events.pop(0) if _events else h11.NEED_DATA  # type: ignore

        request = Request(method=b"POST", url="https://example.com/upload", headers=[])
        result = conn._receive_response_headers(request)

        # Should not raise; final response is returned.
        assert result[1] == 200
