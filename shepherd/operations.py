"""General workflow operations across ARAs."""
import logging
from typing import Dict, Any


async def sort_results_score(
    query_id: str,
    message: Dict[str, Any],
    operation: Dict[str, Any],
    shepherd_options: Dict[str, Any],
    logger: logging.Logger,
) -> Dict[str, Any]:
    # logger.info(f'{guid}: sorting results.')
    results = message["message"].get("results", [])
    aord = operation.get("ascending_or_descending", "descending")
    reverse = aord == "descending"
    try:
        message["message"]["results"] = sorted(
            results,
            key=lambda x: max(
                [y.get("score", 0) for y in x["analyses"]]
            ),
            reverse=reverse
        )
    except KeyError:
        # can't find the right structure of message
        # logger.error(f"{guid}: error sorting results.")
        return message
    # logger.info(f"{guid}: returning sorted results.")
    return message


async def filter_results_top_n(
    query_id: str,
    message: Dict[str, Any],
    operation: Dict[str, Any],
    shepherd_options: Dict[str, Any],
    logger: logging.Logger,
) -> Dict[str, Any]:
    # It's a validation error to not include max_results as a parameter, but let's have a default
    n = operation.get('max_results', 20000)
    try:
        message['message']['results'] = message['message'].get("results", [])[:n]
    except KeyError as e:
        # not a 'mesage' or 'results'
        logger.error(f"Error filtering results: {e}")
        return message
    return message


async def filter_kgraph_orphans(
    query_id: str,
    message: Dict[str, Any],
    operation: Dict[str, Any],
    shepherd_options: Dict[str, Any],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Remove from the knowledge graph any nodes and edges not references by a result, as well as any aux_graphs.
    We do this by starting at results, marking reachable nodes & edges, then remove anything that isn't marked
    There are multiple sources:
    1. Result node bindings
    2. Result.Analysis edge bindings
    3. Result.Analysis support graphs
    4. support graphs from edges found in 2
    5. For all the auxgraphs collect their edges and nodes
    Note that this will fail to find edges and nodes that are recursive.  So if an edge is supported by an edge,
    and that edge is supported by a third edge, then that third edge won't get marked, and will be removed.
    ATM, this is acceptable, but it'll need to be fixed.
    """
    # First, find all the result nodes and edges
    try:
        # logger.info(f'{guid}: filtering kgraph.')
        results = message.get('message', {}).get('results', [])
        nodes = set()
        edges = set()
        auxgraphs = set()
        # 1. Result node bindings
        for result in results:
            for qnode, knodes in result.get('node_bindings', {}).items():
                nodes.update([k['id'] for k in knodes])
        # 2. Result.Analysis edge bindings
        for result in results:
            for analysis in result.get('analyses', []):
                for qedge, kedges in analysis.get('edge_bindings', {}).items():
                    edges.update([k['id'] for k in kedges])
        # 3. Result.Analysis support graphs
        for result in results:
            for analysis in result.get('analyses', []):
                for auxgraph in analysis.get('support_graphs', []):
                    auxgraphs.add(auxgraph)
        # 4. Support graphs from edges in 2
        for edge in edges:
            edge_obj = message.get('message', {}).get('knowledge_graph', {}).get('edges', {}).get(edge, {})
            nodes.update([edge_obj["subject"], edge_obj["object"]])
            for attribute in message.get('message', {}).get('knowledge_graph', {}).get('edges', {}).get(edge, {}).get('attributes', {}):
                if attribute.get('attribute_type_id', None) == 'biolink:support_graphs':
                    auxgraphs.update(attribute.get('value', []))
        # 5. For all the auxgraphs collect their edges and nodes
        for auxgraph in auxgraphs:
            aux_edges = message.get('message', {}).get('auxiliary_graphs', {}).get(auxgraph, {}).get('edges', [])
            for aux_edge in aux_edges:
                if aux_edge not in message["message"]["knowledge_graph"]["edges"]:
                    logger.warning(f"aux_edge {aux_edge} not in knowledge_graph.edges")
                    continue
                edges.add(aux_edge)
                nodes.add(message["message"]["knowledge_graph"]["edges"][aux_edge]["subject"])
                nodes.add(message["message"]["knowledge_graph"]["edges"][aux_edge]["object"])
        # validate_message(message)
        # now remove all knowledge_graph nodes and edges that are not in our nodes and edges sets.
        kg_nodes = message.get('message', {}).get('knowledge_graph', {}).get('nodes', {})
        message['message']['knowledge_graph']['nodes'] = {nid: ndata for nid, ndata in kg_nodes.items() if nid in nodes}
        kg_edges = message.get('message', {}).get('knowledge_graph', {}).get('edges', {})
        message['message']['knowledge_graph']['edges'] = {eid: edata for eid, edata in kg_edges.items() if eid in edges}
        # validate_message(message)
        message["message"]["auxiliary_graphs"] = {
            auxgraph: adata
            for auxgraph, adata in message["message"].get("auxiliary_graphs", {}).items()
            if auxgraph in auxgraphs
        }
        # validate_message(message)
        # logger.info(f'{guid}: returning filtered kgraph.')
        return message
    except Exception as e:
        logger.error(e)
        return message
