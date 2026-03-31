# pyrufh

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Python client for the [Resumable Uploads for HTTP](https://datatracker.ietf.org/doc/draft-ietf-httpbis-resumable-upload/) protocol (`draft-ietf-httpbis-resumable-upload-11`).

Resumable uploads allow clients to recover from interrupted HTTP uploads by resuming from the last acknowledged byte rather than retransmitting the entire representation from scratch.

## Requirements

- Python 3.12+
- [httpx](https://www.python-httpx.org/) ≥ 0.28

## Installation

```bash
pip install pyrufh
# or with uv:
uv add pyrufh
```

## Quick start

### Optimistic upload (§12.1)

The simplest approach. Sends all data in one request. If the server supports resumable uploads it will return a `Location` URI that can be used to resume after an interruption.

```python
from pyrufh import RufhClient

with RufhClient() as client:
    response = client.upload(
        "https://example.com/files",
        data=open("video.mp4", "rb"),
        content_type="video/mp4",
    )
    print(response.status_code, response.json())
```

### Careful upload (§12.2)

Sends an empty creation request first to obtain the upload resource URI and discover server limits, then streams the data in chunks. Useful when you cannot receive interim responses, or when the payload is so large that avoiding any retransmission is worth an extra round-trip.

```python
from pyrufh import RufhClient

with RufhClient(chunk_size=5 * 1024 * 1024) as client:  # 5 MiB chunks
    response = client.upload_carefully(
        "https://example.com/files",
        data=open("large_archive.tar.gz", "rb"),
        content_type="application/gzip",
    )
    print(response.status_code)
```

## Protocol operations

`RufhClient` exposes each of the four protocol operations directly, giving you full control over the upload lifecycle.

### Create an upload (§4.2)

```python
from pyrufh import RufhClient

with RufhClient() as client:
    result = client.create_upload(
        "https://example.com/files",
        data=b"hello, world",
        complete=True,                    # Upload-Complete: ?1
        content_type="text/plain",
    )

    if result.complete:
        # Server processed the upload in a single round-trip
        print("Done:", result.final_response.status_code)
    else:
        # Server created an upload resource; continue with append requests
        resource = result.upload_resource
        print("Upload resource URI:", resource.uri)
```

Set `complete=False` to create a multi-part upload without sending any data yet (careful strategy), or to send only the first chunk.

### Retrieve the current offset (§4.3)

Use this after an interruption to learn how much data the server already has.

```python
from pyrufh import RufhClient, UploadResource

resource = UploadResource(uri="https://example.com/uploads/abc")

with RufhClient() as client:
    client.get_offset(resource)

print(f"Server has {resource.offset} bytes, complete={resource.complete}")
```

### Append data (§4.4)

Send the next chunk of representation data to the upload resource. `Upload-Offset` is taken from `resource.offset`, which is updated automatically after each successful append.

```python
with RufhClient() as client:
    # Send an intermediate chunk
    client.append(resource, data=chunk, complete=False)

    # Send the final chunk
    response = client.append(resource, data=last_chunk, complete=True)
    print("Final response:", response.status_code)
```

### Cancel an upload (§4.5)

```python
with RufhClient() as client:
    client.cancel(resource)
```

## Providing your own `httpx.Client`

Pass a pre-configured `httpx.Client` to share connection pools, set default headers, configure authentication, timeouts, etc.

```python
import httpx
from pyrufh import RufhClient

http = httpx.Client(
    headers={"Authorization": "Bearer <token>"},
    timeout=httpx.Timeout(30.0),
)

with RufhClient(client=http) as client:
    response = client.upload("https://example.com/files", data=b"...")

http.close()
```

> **Note:** When you supply your own `httpx.Client`, `RufhClient` will not close it automatically. You are responsible for its lifecycle.

## Error handling

All exceptions inherit from `RufhError`.

| Exception | When raised |
|---|---|
| `UploadCreationError` | Server rejects the creation request (4xx/5xx) |
| `OffsetRetrievalError` | HEAD request to the upload resource fails |
| `UploadAppendError` | PATCH append request fails |
| `UploadCancellationError` | DELETE request fails |
| `MismatchingOffsetError` | Server returns 409 – `Upload-Offset` mismatch |
| `CompletedUploadError` | Server rejects append because upload is already complete |
| `InconsistentLengthError` | Server reports inconsistent length values |

```python
from pyrufh import RufhClient, MismatchingOffsetError, UploadCreationError

with RufhClient() as client:
    try:
        response = client.upload("https://example.com/files", data=payload)
    except MismatchingOffsetError as e:
        print(f"Offset mismatch: expected {e.expected_offset}, sent {e.provided_offset}")
    except UploadCreationError as e:
        print(f"Upload rejected with status {e.status_code}")
```

## Server limits

Servers may advertise limits via the `Upload-Limit` response header (§4.1.4). `RufhClient` reads these automatically and stores them on the `UploadResource`. The `upload()` and `upload_carefully()` helpers clamp the chunk size to `max_append_size` / `min_append_size` automatically.

```python
result = client.create_upload(url, b"", complete=False, length=total_size)
limits = result.upload_resource.limits

if limits:
    print(f"max upload size : {limits.max_size}")
    print(f"max append size : {limits.max_append_size}")
    print(f"resource max-age: {limits.max_age}s")
```

## Draft interop version

This library implements **interop version 8** (Appendix B of the draft). It sends `Upload-Draft-Interop-Version: 8` on every request and ignores `104` responses that carry a different or missing interop version, as required by the spec.

## Development

```bash
git clone https://github.com/your-org/pyrufh
cd pyrufh
uv sync --all-groups
```

Run the checks:

```bash
uv run ruff format --check src/ tests/   # formatting
uv run ruff check src/ tests/            # linting
uv run ty check src/                     # type checking
uv run pytest tests/ -v                  # tests
```

## CI

GitHub Actions runs three jobs on every push and pull request to `main`:

| Job | Tool | What it checks |
|---|---|---|
| `lint` | ruff | Formatting and linting |
| `typecheck` | ty | Static type correctness |
| `test` | pytest | Functional correctness on Python 3.12, 3.13, and 3.14 |

## Specification

- [draft-ietf-httpbis-resumable-upload-11](https://datatracker.ietf.org/doc/draft-ietf-httpbis-resumable-upload/)
- [IETF HTTP Working Group](https://httpwg.org/)

## License

MIT — see [LICENSE](LICENSE).
