"""Data models for the pyrufh resumable upload client."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx

    from .headers import UploadLimits
    from .transport import InterimResponse


@dataclass
class UploadResource:
    """Represents a server-side upload resource.

    This is the handle returned after a successful upload creation request.
    It contains all the information needed to resume, append to, or cancel an
    ongoing upload.
    """

    #: The URI of the upload resource (from the Location header).
    uri: str

    #: The number of bytes the server has received so far.
    offset: int = 0

    #: Whether the upload has been marked as complete.
    complete: bool = False

    #: The total length of the representation, if known.
    length: int | None = None

    #: Server-imposed limits for this upload resource.
    limits: UploadLimits | None = None

    #: The final response received when the upload was completed with
    #: Upload-Complete: ?1.  Only set after the upload finishes.
    final_response: httpx.Response | None = field(default=None, repr=False)


@dataclass
class UploadCreationResult:
    """Result of an upload creation request.

    Depending on the outcome:
    - If the entire representation was transferred and the server returned a
      2xx response with Upload-Complete: ?1, the upload is already finished.
      ``upload_resource`` and ``final_response`` will both be set.
    - If the upload was created but not yet completed (Upload-Complete: ?0 or
      the full data was not sent), ``upload_resource`` will be set and
      ``final_response`` will be None until the upload is completed via append.
    """

    #: The upload resource (always set on success).
    upload_resource: UploadResource

    #: Set when the upload was completed in the creation request itself.
    final_response: httpx.Response | None = None

    #: 104 (Upload Resumption Supported) interim responses received during
    #: the creation request, in the order they arrived.  Empty when the
    #: underlying transport does not support interim response capture or when
    #: the server did not send any.
    interim_responses: list[InterimResponse] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        """True if the upload is already complete."""
        return self.upload_resource.complete
