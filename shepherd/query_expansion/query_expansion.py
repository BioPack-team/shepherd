"""Query Expansion."""

from typing import Any, Dict, List, Optional, Union

from shepherd.query_expansion.example.expansion import expand_example_query
from shepherd.query_expansion.aragorn.aragorn import expand_aragorn_query
from shepherd.query_expansion.bte.expansion import expand_bte_query


async def expand_query(
    query: Dict[str, Any], options: Dict[str, Any]
) -> tuple[List[Any], Dict[str, Any]]:
    """Get expanded queries."""
    queries = []
    concurrency = 1
    target = options.get("target")
    concurrency = 1_000_000
    match target:
        case "example":
            queries = expand_example_query(query)
            # you can override template concurrency here:
            # concurrency = 1
        case "aragorn":
            queries = expand_aragorn_query(query)
        case "bte":
            queries = expand_bte_query(query)
        case _:
            queries = []

    return queries, {"concurrency": concurrency}
