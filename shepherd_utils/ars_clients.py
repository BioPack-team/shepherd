"""Subscriber client signature verification (ARS event-subscribe parity).

Ported from Relay ``verify_signature`` / ``canonize_url`` / ``decrypt_secret``.
A subscriber signs requests with an HMAC-SHA256 of the raw POST body (or the
canonicalized GET url) under a per-client secret; the secret is stored AES-CBC
encrypted and decrypted with ``settings.aes_master_key``.
"""

import base64
import hashlib
import hmac
import logging
from typing import Any, Dict
from urllib.parse import parse_qsl, unquote, urlparse

from shepherd_utils.config import settings
from shepherd_utils.db import get_client

EVENT_SIGNATURE_HEADER = "x-event-signature"


def canonize_url(url_str: str) -> str:
    """Canonical signature string for a GET url (Relay ``canonize_url``)."""
    parsed = urlparse(url_str)
    sorted_query = "|".join(
        f"{unquote(k)}|{unquote(v)}"
        for k, v in sorted(parse_qsl(parsed.query, keep_blank_values=True))
    )
    return "|".join([parsed.scheme, parsed.netloc, parsed.path, sorted_query])


def decrypt_secret(encrypted_secret: str, master_key: bytes) -> str:
    """Decrypt an AES-CBC (IV-prefixed, PKCS7-padded) base64 secret.

    Imported lazily so the module loads even where pycryptodome is absent.
    """
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad

    encrypted_data = base64.b64decode(encrypted_secret)
    iv = encrypted_data[: AES.block_size]
    cipher = AES.new(master_key, AES.MODE_CBC, iv)
    decrypted = unpad(cipher.decrypt(encrypted_data[AES.block_size :]), AES.block_size)
    return decrypted.decode()


def _master_key() -> bytes:
    return base64.b64decode(settings.aes_master_key)


async def _client_secret(client_id: str, logger: logging.Logger):
    """Return the decrypted client secret + subscriptions, or (None, None)."""
    client = await get_client(client_id, logger)
    if client is None or not client.get("client_secret"):
        return None, None
    try:
        secret = decrypt_secret(client["client_secret"], _master_key())
    except Exception as e:
        logger.error(f"Failed to decrypt client secret for {client_id}: {e}")
        return None, client.get("subscriptions") or []
    return secret, client.get("subscriptions") or []


async def verify_post_signature(
    signature: str,
    body: bytes,
    client_id: str,
    logger: logging.Logger,
) -> bool:
    """Verify an HMAC-SHA256 over the raw POST body under the client secret."""
    if not signature or not settings.aes_master_key:
        return False
    secret, _ = await _client_secret(client_id, logger)
    if secret is None:
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


async def verify_get_signature(
    signature: str,
    url: str,
    client_id: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Verify a GET signature over the canonicalized url; return verified +
    the client's current subscriptions."""
    result = {"verified": False, "pks": [], "client_id": client_id}
    if not signature or not settings.aes_master_key:
        return result
    secret, subscriptions = await _client_secret(client_id, logger)
    result["pks"] = subscriptions or []
    if secret is None:
        return result
    expected = hmac.new(
        secret.encode("utf-8"), canonize_url(url).encode("utf-8"), hashlib.sha256
    ).hexdigest()
    result["verified"] = hmac.compare_digest(expected, signature)
    return result
