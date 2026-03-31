"""Custom httpx transport that captures 104 (Upload Resumption Supported) interim responses.

HTTP/1.1 connections route through h11, which sees ``InformationalResponse`` events for
1xx status codes.  The standard ``httpcore`` ``HTTP11Connection`` discards all 1xx responses
except 101 (WebSocket upgrade), so they never reach the ``httpx`` layer.

This module provides:

- :class:`InterimResponse` - lightweight dataclass holding the headers of a single 104
  interim response.
- :class:`InterimCapturingTransport` - an ``httpx.HTTPTransport`` subclass that captures
  every 104 response from the server and fires an ``on_interim`` callback for each one.

The capturing is implemented by subclassing ``httpcore``'s ``HTTP11Connection`` and
overriding ``_receive_response_headers`` to intercept h11 ``InformationalResponse``
events before the normal code discards them.  The custom connection class is injected
into the ``httpcore.ConnectionPool`` via a pool subclass that overrides
``create_connection``.

Because :class:`InterimCapturingTransport` extends ``httpx.HTTPTransport`` without
overriding ``handle_request``, test mocking libraries that patch
``httpx.HTTPTransport.handle_request`` (such as ``pytest-httpx``) continue to work
correctly.
"""

from __future__ import annotations

import logging
import typing
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ssl

import h11
import httpcore
import httpcore._sync.connection as _hc_conn_mod
import httpcore._sync.http11 as _hc_http11_mod
import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public data model
# ---------------------------------------------------------------------------


@dataclass
class InterimResponse:
    """A captured 104 (Upload Resumption Supported) interim response.

    Attributes
    ----------
    headers:
        The raw headers from the 104 interim response, as a list of
        ``(name_bytes, value_bytes)`` pairs (as returned by h11).
    """

    headers: list[tuple[bytes, bytes]] = field(default_factory=list)

    def get(self, name: str, default: str | None = None) -> str | None:
        """Return the first header value matching *name* (case-insensitive).

        Parameters
        ----------
        name:
            Header field name (case-insensitive), e.g. ``"location"``.
        default:
            Value to return when the header is absent.
        """
        target = name.lower().encode()
        for k, v in self.headers:
            if k.lower() == target:
                return v.decode(errors="replace")
        return default


# ---------------------------------------------------------------------------
# Internal - patched httpcore HTTP11Connection
# ---------------------------------------------------------------------------

#: Type alias for the 104-response callback.
_InterimCallback = typing.Callable[[InterimResponse], None]


class _CapturingHTTP11Connection(_hc_http11_mod.HTTP11Connection):
    """HTTP11Connection subclass that fires *on_interim* for every 104 response."""

    _on_interim: _InterimCallback | None

    def __init__(
        self,
        *args: typing.Any,
        on_interim: _InterimCallback | None = None,
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._on_interim = on_interim

    def _receive_response_headers(
        self,
        request: httpcore.Request,
    ) -> tuple[bytes, int, bytes, list[tuple[bytes, bytes]], bytes]:
        """Override to fire *on_interim* for every 104 response.

        The original httpcore implementation discards all 1xx responses except
        101.  We replicate the same logic while additionally calling the callback
        for 104 events.
        """
        timeouts = request.extensions.get("timeout", {})
        timeout = timeouts.get("read", None)

        while True:
            event = self._receive_event(timeout=timeout)
            if isinstance(event, h11.Response):
                break
            if isinstance(event, h11.InformationalResponse):
                if event.status_code == 101:
                    # WebSocket / HTTP upgrade - honour original behaviour.
                    break
                if event.status_code == 104 and self._on_interim is not None:
                    interim = InterimResponse(headers=list(event.headers.raw_items()))
                    try:
                        self._on_interim(interim)
                    except Exception:  # pragma: no cover
                        logger.debug("on_interim callback raised; ignoring", exc_info=True)
                # All other 1xx responses are silently discarded, matching
                # standard httpcore behaviour.

        http_version = b"HTTP/" + event.http_version
        headers = event.headers.raw_items()
        trailing_data, _ = self._h11_state.trailing_data
        return http_version, event.status_code, event.reason, headers, trailing_data


# ---------------------------------------------------------------------------
# Internal - patched httpcore HTTPConnection
# ---------------------------------------------------------------------------


class _CapturingHTTPConnection(_hc_conn_mod.HTTPConnection):
    """HTTPConnection subclass that injects *_CapturingHTTP11Connection* at connect time.

    The parent class creates an ``HTTP11Connection`` instance inside its own
    ``handle_request`` method (after the TCP handshake).  We temporarily replace
    the module-level ``HTTP11Connection`` name that the parent imports from so that
    our subclass is used instead.
    """

    def __init__(
        self,
        *args: typing.Any,
        on_interim: _InterimCallback | None = None,
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._on_interim = on_interim

    def handle_request(self, request: httpcore.Request) -> httpcore.Response:
        on_interim = self._on_interim
        original_cls = _hc_conn_mod.HTTP11Connection

        class _Patched(_CapturingHTTP11Connection):
            def __init__(self, *a: typing.Any, **kw: typing.Any) -> None:
                super().__init__(*a, on_interim=on_interim, **kw)

        try:
            _hc_conn_mod.HTTP11Connection = _Patched  # type: ignore[misc]  # ty: ignore[invalid-assignment]
            return super().handle_request(request)
        finally:
            _hc_conn_mod.HTTP11Connection = original_cls  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Internal - patched httpcore ConnectionPool
# ---------------------------------------------------------------------------


class _CapturingPool(httpcore.ConnectionPool):
    """ConnectionPool subclass that creates ``_CapturingHTTPConnection`` instances."""

    def __init__(
        self,
        *args: typing.Any,
        on_interim: _InterimCallback | None = None,
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._on_interim = on_interim

    def create_connection(  # type: ignore[override]
        self, origin: httpcore._models.Origin
    ) -> _CapturingHTTPConnection:
        """Return a capturing HTTPConnection for the given *origin*."""
        return _CapturingHTTPConnection(
            origin=origin,
            ssl_context=self._ssl_context,
            keepalive_expiry=self._keepalive_expiry,
            http1=self._http1,
            http2=self._http2,
            retries=self._retries,
            local_address=self._local_address,
            uds=self._uds,
            network_backend=self._network_backend,
            socket_options=self._socket_options,
            on_interim=self._on_interim,
        )


# ---------------------------------------------------------------------------
# Public - InterimCapturingTransport
# ---------------------------------------------------------------------------


class InterimCapturingTransport(httpx.HTTPTransport):
    """An ``httpx.HTTPTransport`` subclass that fires a callback for every 104 interim response.

    This transport subclasses :class:`httpx.HTTPTransport` without overriding
    ``handle_request``, so test mocking libraries that patch
    ``httpx.HTTPTransport.handle_request`` (e.g. ``pytest-httpx``) continue to work
    as expected.

    The 104-capturing logic is injected at the ``httpcore`` connection-pool level: a
    custom :class:`_CapturingPool` is substituted for the default
    ``httpcore.ConnectionPool`` stored in ``self._pool``.

    Parameters
    ----------
    on_interim:
        Callable invoked synchronously for every 104 response received during a
        request.  The callable receives a single :class:`InterimResponse` argument.
        When ``None``, 104 responses are discarded (same as the default transport).
    verify:
        SSL verification - forwarded to the parent ``__init__``.  Accepts a boolean,
        a path to a CA bundle, or an ``ssl.SSLContext`` instance.
    http1:
        Enable HTTP/1.1 (default ``True``).
    http2:
        Enable HTTP/2 (default ``False``).

    Examples
    --------
    Collecting all 104 responses into a list::

        received: list[InterimResponse] = []
        transport = InterimCapturingTransport(on_interim=received.append)
        with httpx.Client(transport=transport) as client:
            resp = client.post("https://example.com/upload", ...)
    """

    def __init__(
        self,
        *,
        on_interim: _InterimCallback | None = None,
        verify: ssl.SSLContext | str | bool = True,
        http1: bool = True,
        http2: bool = False,
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(verify=verify, http1=http1, http2=http2, **kwargs)

        # Replace the pool created by the parent with our capturing version.
        # We re-use the same ssl_context and settings that the parent already computed.
        # The parent class always creates a plain ConnectionPool when no proxy is set, so
        # casting to httpcore.ConnectionPool is safe here.
        import typing as _typing

        existing_pool = _typing.cast("httpcore.ConnectionPool", self._pool)
        self._pool = _CapturingPool(
            ssl_context=existing_pool._ssl_context,
            max_connections=existing_pool._max_connections,
            max_keepalive_connections=existing_pool._max_keepalive_connections,
            keepalive_expiry=existing_pool._keepalive_expiry,
            http1=existing_pool._http1,
            http2=existing_pool._http2,
            retries=existing_pool._retries,
            local_address=existing_pool._local_address,
            uds=existing_pool._uds,
            network_backend=existing_pool._network_backend,
            socket_options=existing_pool._socket_options,
            on_interim=on_interim,
        )
        # Close the unused pool to free resources.
        existing_pool.close()
