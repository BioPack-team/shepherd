"""HMAC signature verification for inbound ARS callbacks (Relay parity).

When ``settings.ars_signature_verify`` is on, a callback must carry an
``X-ARS-Signature`` header equal to the hex HMAC-SHA256 of the raw body under
``settings.ars_signature_secret``. ``verify_signature`` is pure (takes the
header + body bytes) so it is trivially unit-testable.
"""

import hashlib
import hmac

from shepherd_utils.config import settings

SIGNATURE_HEADER = "x-ars-signature"


def compute_signature(body: bytes, secret: str) -> str:
    """Return the hex HMAC-SHA256 of ``body`` under ``secret``."""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify_signature(signature: str, body: bytes, secret: str) -> bool:
    """Constant-time check that ``signature`` matches ``body`` under ``secret``."""
    if not signature or not secret:
        return False
    return hmac.compare_digest(signature, compute_signature(body, secret))


def is_signature_valid(headers, body: bytes) -> bool:
    """Verify a request's signature using configured settings.

    Returns True when verification is disabled (so callers can gate on a single
    call). ``headers`` is anything with case-insensitive ``.get`` (e.g. a
    Starlette ``Headers``).
    """
    if not settings.ars_signature_verify:
        return True
    signature = headers.get(SIGNATURE_HEADER, "")
    return verify_signature(signature, body, settings.ars_signature_secret)
