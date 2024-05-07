"""Query Expansion."""
from typing import Any, Dict, List, Optional, Union

from shepherd.modules.aragorn import expand_aragorn_query


def expand_query(query: Dict[str, Any], options: Dict[str, Any]) -> tuple[List[Any], Dict[str, Any]]:
    """Get expanded queries."""
    queries = []
    concurrency = 1
    target = options.get("target")
    if target == "aragorn":
        queries = expand_aragorn_query(query)
        concurrency = 1_000_000
    elif target == "bte":
        queries = get_bte_queries(query)
        concurrency = 1

    return queries, { "concurrency": concurrency }
