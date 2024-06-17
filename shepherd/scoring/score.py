"""Scoring."""
import logging
from typing import Any, Dict, List

from shepherd.scoring.aragorn.score import send_to_aragorn_ranker
from shepherd.scoring.bte.score import score_bte


async def score_query(
    query_id: str,
    message: Dict[str, Any],
    options: Dict[str, Any],
    shepherd_options: Dict[str, Any],
    logger: logging.Logger,
) -> tuple[List[Any], Dict[str, Any]]:
    """Score the response."""
    response = None
    target = shepherd_options.get("target")
    match target:
        case "example":
            response = message
        case "aragorn":
            response = await send_to_aragorn_ranker(message)
        case "bte":
            response = await score_bte(message)
        case _:
            response = message

    return response
