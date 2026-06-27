"""Tests for ARS callback signature verification."""

from shepherd_utils import signature
from shepherd_utils.signature import (
    compute_signature,
    is_signature_valid,
    verify_signature,
)


def test_verify_signature_roundtrip():
    body = b'{"hello":"world"}'
    secret = "s3cret"
    sig = compute_signature(body, secret)
    assert verify_signature(sig, body, secret) is True


def test_verify_signature_rejects_tampered_body():
    secret = "s3cret"
    sig = compute_signature(b"original", secret)
    assert verify_signature(sig, b"tampered", secret) is False


def test_verify_signature_rejects_empty():
    assert verify_signature("", b"x", "secret") is False
    assert verify_signature("abc", b"x", "") is False


class _Headers:
    def __init__(self, mapping):
        self._m = mapping

    def get(self, key, default=None):
        return self._m.get(key, default)


def test_is_signature_valid_disabled_passes(mocker):
    mocker.patch.object(signature.settings, "ars_signature_verify", False)
    assert is_signature_valid(_Headers({}), b"anything") is True


def test_is_signature_valid_enabled_checks(mocker):
    mocker.patch.object(signature.settings, "ars_signature_verify", True)
    mocker.patch.object(signature.settings, "ars_signature_secret", "k")
    body = b"payload"
    good = compute_signature(body, "k")
    assert is_signature_valid(_Headers({signature.SIGNATURE_HEADER: good}), body) is True
    assert is_signature_valid(_Headers({signature.SIGNATURE_HEADER: "bad"}), body) is False
    assert is_signature_valid(_Headers({}), body) is False
