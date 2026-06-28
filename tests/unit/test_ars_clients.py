"""Tests for subscriber client signature verification (ARS parity)."""

import base64
import hashlib
import hmac
import logging

import pytest
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from shepherd_utils import ars_clients
from shepherd_utils.ars_clients import (
    canonize_url,
    decrypt_secret,
    verify_get_signature,
    verify_post_signature,
)

logger = logging.getLogger(__name__)


def _encrypt(secret: str, master_key: bytes) -> str:
    """AES-CBC encrypt like the ARS client provisioning side (IV-prefixed)."""
    iv = b"\x00" * AES.block_size
    cipher = AES.new(master_key, AES.MODE_CBC, iv)
    ct = cipher.encrypt(pad(secret.encode(), AES.block_size))
    return base64.b64encode(iv + ct).decode()


def test_canonize_url_sorts_query():
    url = "https://h.test/ars/api/sub?b=2&a=1"
    assert canonize_url(url) == "https|h.test|/ars/api/sub|a|1|b|2"


def test_decrypt_secret_roundtrip():
    master_key = b"0" * 32
    enc = _encrypt("super-secret", master_key)
    assert decrypt_secret(enc, master_key) == "super-secret"


@pytest.mark.asyncio
async def test_verify_post_signature_accepts_valid(mocker):
    master_key = b"0" * 32
    mocker.patch.object(
        ars_clients.settings, "aes_master_key", base64.b64encode(master_key).decode()
    )
    mocker.patch.object(
        ars_clients,
        "get_client",
        new_callable=mocker.AsyncMock,
        return_value={"client_secret": _encrypt("k", master_key), "subscriptions": []},
    )
    body = b'{"pks":["p1"],"client_id":"c1"}'
    sig = hmac.new(b"k", body, hashlib.sha256).hexdigest()
    assert await verify_post_signature(sig, body, "c1", logger) is True
    assert await verify_post_signature("bad", body, "c1", logger) is False


@pytest.mark.asyncio
async def test_verify_post_signature_disabled_without_master_key(mocker):
    mocker.patch.object(ars_clients.settings, "aes_master_key", "")
    assert await verify_post_signature("x", b"{}", "c1", logger) is False


@pytest.mark.asyncio
async def test_verify_get_signature_returns_subscriptions(mocker):
    master_key = b"0" * 32
    mocker.patch.object(
        ars_clients.settings, "aes_master_key", base64.b64encode(master_key).decode()
    )
    mocker.patch.object(
        ars_clients,
        "get_client",
        new_callable=mocker.AsyncMock,
        return_value={
            "client_secret": _encrypt("k", master_key),
            "subscriptions": ["p1", "p2"],
        },
    )
    url = "https://h.test/ars/query_event_subscribe?client_id=c1"
    sig = hmac.new(
        b"k", canonize_url(url).encode(), hashlib.sha256
    ).hexdigest()
    out = await verify_get_signature(sig, url, "c1", logger)
    assert out["verified"] is True
    assert out["pks"] == ["p1", "p2"]
