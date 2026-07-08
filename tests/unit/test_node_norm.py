"""Tests for the node normalization worker (thin wrapper over ars_norm)."""

import logging

import pytest

from workers.node_norm.worker import node_norm

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_node_norm_saves_when_normalized(mocker):
    """A successful normalization re-saves the message."""
    mocker.patch(
        "workers.node_norm.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value={"message": {}},
    )
    mocker.patch(
        "workers.node_norm.worker.normalize_message",
        new_callable=mocker.AsyncMock,
        return_value=True,
    )
    save = mocker.patch(
        "workers.node_norm.worker.save_message", new_callable=mocker.AsyncMock
    )
    await node_norm(("m", {"response_id": "r1"}), logger)
    save.assert_called_once()


@pytest.mark.asyncio
async def test_node_norm_passthrough_when_nothing_normalized(mocker):
    """When normalization is a no-op, the message is not re-saved."""
    mocker.patch(
        "workers.node_norm.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value={"message": {}},
    )
    mocker.patch(
        "workers.node_norm.worker.normalize_message",
        new_callable=mocker.AsyncMock,
        return_value=False,
    )
    save = mocker.patch(
        "workers.node_norm.worker.save_message", new_callable=mocker.AsyncMock
    )
    await node_norm(("m", {"response_id": "r1"}), logger)
    save.assert_not_called()
