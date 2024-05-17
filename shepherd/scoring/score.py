"""Scoring."""
from typing import Any, Dict, List, Optional, Union

from shepherd.scoring.aragorn.score import send_to_aragorn_ranker


async def score_query(message: Dict[str, Any], options: Dict[str, Any]) -> tuple[List[Any], Dict[str, Any]]:
    """Score the response."""
    response = None
    target = options.get("target")
    if target == "aragorn":
        response = await send_to_aragorn_ranker(message)
    elif target == "bte":
        response = do_bte_scoring(message)

    return response
