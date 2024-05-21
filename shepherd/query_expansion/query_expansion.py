"""Query Expansion."""

from typing import Any, Dict, List, Optional, Union

from shepherd.query_expansion.aragorn.aragorn import expand_aragorn_query
from shepherd.query_expansion.bte.expansion import expand_bte_query


def expand_query(
    query: Dict[str, Any], options: Dict[str, Any]
) -> tuple[List[Any], Dict[str, Any]]:
    """Get expanded queries."""
    queries = []
    concurrency = 1
    target = options.get("target")
    if target == "aragorn":
        queries = expand_aragorn_query(query)
        concurrency = 1_000_000
    elif target == "bte":
        queries = expand_bte_query(query)
        concurrency = 1_000_000

    return queries, {"concurrency": concurrency}