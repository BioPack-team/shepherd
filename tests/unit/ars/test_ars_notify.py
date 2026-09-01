"""Parity tests for the ars_notify worker.

Upstream reference: NCATSTranslator/Relay @ dd1e71b tasks.py
notify_subscribers_task + notify_one_client_task: payload base
{pk, timestamp, code} + event fields, last_merged_completed forces code 200,
one POST per subscribed client with an HMAC-SHA256 x-event-signature over
the compact sorted-key JSON body, retried on failure.
"""

import base64
import json
import logging
import uuid
from unittest.mock import AsyncMock

import httpx
import pytest

import shepherd_utils.ars.db as ars_db
from shepherd_utils.ars import crypto
from shepherd_utils.config import settings
from workers.ars_notify import worker as notify_worker

LOGGER = logging.getLogger(__name__)

MASTER_KEY = base64.b64encode(b"0" * 32).decode()
SECRET = "hunter2-hunter2"
IV = b"1" * 16


@pytest.fixture
def env(mocker):
    mocker.patch.object(settings, "aes_master_key", MASTER_KEY)
    encrypted = crypto.encrypt_secret(SECRET, base64.b64decode(MASTER_KEY), IV)
    message_pk = uuid.uuid4()
    clients = [
        {
            "id": 1,
            "client_id": "ui",
            "client_secret": encrypted,
            "callback_url": "https://ui.example/notify",
            "active": True,
            "subscriptions": [str(message_pk)],
        },
    ]

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    return {
        "message_pk": message_pk,
        "get_subscribed_clients": _patch(
            "get_subscribed_clients", return_value=clients
        ),
    }


def _task(env, fields, code="202"):
    return [
        "tid",
        {
            "message_pk": str(env["message_pk"]),
            "code": code,
            "fields": json.dumps(fields),
            "log_level": "20",
            "otel": "{}",
        },
    ]


async def test_notification_posted_with_valid_hmac(env, mocker):
    post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=httpx.Response(
            200, request=httpx.Request("POST", "https://ui.example/notify")
        ),
    )
    await notify_worker.ars_notify(
        _task(env, {"event_type": "merged_version_begun", "complete": False}),
        LOGGER,
    )
    call = post.await_args
    assert call.kwargs["url"] == "https://ui.example/notify"
    body = call.kwargs["content"]
    headers = call.kwargs["headers"]
    assert headers["Content-Type"] == "application/json"
    # body is compact sorted-key JSON, HMAC verifies with the client secret
    assert crypto.verify_body_signature(body, SECRET, headers["x-event-signature"])
    notification = json.loads(body)
    assert notification["pk"] == str(env["message_pk"])
    assert notification["code"] == 202
    assert notification["event_type"] == "merged_version_begun"
    assert "timestamp" in notification
    # compact sorted encoding, byte-exact with upstream's json.dumps
    assert body == json.dumps(
        notification, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


async def test_last_merged_completed_forces_code_200(env, mocker):
    post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=httpx.Response(
            200, request=httpx.Request("POST", "https://ui.example/notify")
        ),
    )
    await notify_worker.ars_notify(
        _task(
            env, {"event_type": "last_merged_completed", "complete": True}, code="202"
        ),
        LOGGER,
    )
    notification = json.loads(post.await_args.kwargs["content"])
    assert notification["code"] == 200


async def test_failed_delivery_retries(env, mocker):
    post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        return_value=httpx.Response(
            500, request=httpx.Request("POST", "https://ui.example/notify")
        ),
    )
    mocker.patch.object(notify_worker, "MAX_RETRIES", 2)
    mocker.patch.object(notify_worker.asyncio, "sleep", new_callable=AsyncMock)
    await notify_worker.ars_notify(
        _task(env, {"event_type": "admin", "complete": True}), LOGGER
    )
    assert post.await_count == 2


async def test_no_clients_no_posts(env, mocker):
    env["get_subscribed_clients"].return_value = []
    post = mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    await notify_worker.ars_notify(
        _task(env, {"event_type": "admin", "complete": True}), LOGGER
    )
    post.assert_not_awaited()
