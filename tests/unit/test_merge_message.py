import copy
import json
import logging
import pytest

from shepherd_utils.db import get_message, save_message
from workers.merge_message.worker import merge_messages

from tests.helpers.generate_messages import (
    generate_response,
    generate_query,
    response_1,
    response_2,
)


@pytest.mark.asyncio
async def test_message_merge(redis_mock, mocker):
    """Test sort results score."""
    lookup_query_graph = generate_query()["message"]["query_graph"]
    original_query_graph = response_1["message"]["query_graph"]
    callback_response = copy.deepcopy(response_2)
    original_response = generate_response()

    merged_message = merge_messages(
        lookup_query_graph,
        original_query_graph,
        result_messages=[original_response, callback_response],
    )

    assert len(merged_message["message"]["results"]) == 3
