"""Client-secret crypto + notification signing.

Ported from NCATSTranslator/Relay @ dd1e71b:
  - api.py decrypt_secret (AES-CBC, IV-prefixed, PKCS7-unpadded, base64)
  - api.py canonize_url (GET-signature canonical string)
  - tasks.py notify_one_client_task (HMAC-SHA256 over compact sorted JSON)
"""

import base64
import hashlib
import hmac
import json
from typing import Any, Dict
from urllib.parse import parse_qsl, unquote, urlparse

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from shepherd_utils.config import settings


def master_key() -> bytes:
    """The AES master key from settings (upstream env AES_MASTER_KEY)."""
    if not settings.aes_master_key:
        raise RuntimeError("AES_MASTER_KEY is not set")
    return base64.b64decode(settings.aes_master_key)


def decrypt_secret(encrypted_secret: str, key: bytes) -> str:
    encrypted_data = base64.b64decode(encrypted_secret)
    iv = encrypted_data[: AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted_secret = unpad(
        cipher.decrypt(encrypted_data[AES.block_size:]), AES.block_size
    )
    return decrypted_secret.decode()


def encrypt_secret(secret: str, key: bytes, iv: bytes) -> str:
    """Inverse of decrypt_secret; used by tests and client onboarding."""
    cipher = AES.new(key, AES.MODE_CBC, iv)
    payload = iv + cipher.encrypt(pad(secret.encode(), AES.block_size))
    return base64.b64encode(payload).decode()


def canonize_url(url_str: str) -> str:
    parsed = urlparse(url_str)
    sorted_query = "|".join(
        f"{unquote(k)}|{unquote(v)}"
        for k, v in sorted(parse_qsl(parsed.query, keep_blank_values=True))
    )
    return "|".join([parsed.scheme, parsed.netloc, parsed.path, sorted_query])


def notification_body_and_signature(
    notification: Dict[str, Any], client_secret: str
) -> tuple[bytes, str]:
    """The exact bytes + x-event-signature the notify worker POSTs."""
    data_json = json.dumps(
        notification, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    digest = hmac.new(
        client_secret.encode("utf-8"), data_json, hashlib.sha256
    ).hexdigest()
    return data_json, digest


def verify_body_signature(body: bytes, client_secret: str, signature: str) -> bool:
    expected = hmac.new(
        client_secret.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def verify_url_signature(url: str, client_secret: str, signature: str) -> bool:
    expected = hmac.new(
        client_secret.encode("utf-8"),
        canonize_url(url).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
