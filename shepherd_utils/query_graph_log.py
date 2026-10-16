"""How a query graph is logged."""

# A query node pinned to more ids than this logs as "N curies" instead.
MAX_LOGGED_NODE_IDS = 10


def summarize_query_graph(query_graph):
    """The query graph to log: a node's long id list becomes its count, so a
    lookup pinning thousands of curies logs in a line."""
    if not isinstance(query_graph, dict) or not isinstance(
        query_graph.get("nodes"), dict
    ):
        return query_graph
    nodes = {}
    for node_id, node in query_graph["nodes"].items():
        ids = node.get("ids") if isinstance(node, dict) else None
        if isinstance(ids, list) and len(ids) > MAX_LOGGED_NODE_IDS:
            node = {**node, "ids": f"{len(ids)} curies"}
        nodes[node_id] = node
    return {**query_graph, "nodes": nodes}
