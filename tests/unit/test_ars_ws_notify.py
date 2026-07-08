"""Tests for the ars_ws subscriber notification shaping + signing (C1/C2)."""

import logging

import pytest

from workers.ars_ws import worker as ars_ws

logger = logging.getLogger(__name__)


def test_to_notification_maps_pk_and_defaults():
    event = {
        "parent_qid": "p1",
        "event_type": "ara_response_complete",
        "ara_name": "infores:aragorn",
    }
    notification = ars_ws._to_notification(event, "p1")
    # Routing-only parent_qid becomes pk; timestamp + code defaulted.
    assert notification["pk"] == "p1"
    assert "parent_qid" not in notification
    assert notification["event_type"] == "ara_response_complete"
    assert notification["code"] == 200
    assert "timestamp" in notification


def test_to_notification_preserves_explicit_code():
    notification = ars_ws._to_notification(
        {"parent_qid": "p1", "code": 598, "timed_out": True}, "p1"
    )
    assert notification["code"] == 598
    assert notification["timed_out"] is True


@pytest.mark.asyncio
async def test_notify_subscribers_signs_and_posts_per_target(mocker):
    mocker.patch.object(
        ars_ws,
        "list_subscriber_targets",
        new_callable=mocker.AsyncMock,
        return_value=[
            {"callback_url": "http://a", "client_id": "c1"},
            {"callback_url": "http://b", "client_id": None},
        ],
    )
    # sign_notification returns a per-target (body, headers) pair.
    sign = mocker.patch.object(
        ars_ws,
        "sign_notification",
        new_callable=mocker.AsyncMock,
        side_effect=lambda notif, cid, log: (
            b'{"pk":"p1"}',
            {"x-event-signature": cid} if cid else {},
        ),
    )
    post = mocker.patch("httpx.AsyncClient.post", new_callable=mocker.AsyncMock)

    await ars_ws._notify_subscribers({"parent_qid": "p1", "event_type": "x"})

    assert post.await_count == 2
    # Signed with each subscriber's client id (2nd positional arg).
    assert {c.args[1] for c in sign.await_args_list} == {"c1", None}
    # POST sends the canonical body bytes as content.
    assert post.await_args_list[0].kwargs["content"] == b'{"pk":"p1"}'


@pytest.mark.asyncio
async def test_notify_subscribers_noop_without_subscribers(mocker):
    mocker.patch.object(
        ars_ws,
        "list_subscriber_targets",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    post = mocker.patch("httpx.AsyncClient.post", new_callable=mocker.AsyncMock)
    await ars_ws._notify_subscribers({"parent_qid": "p1"})
    post.assert_not_called()
