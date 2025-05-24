"""Example ARA module."""
import asyncio
import json
import logging
import time
import uuid
from shepherd_utils.db import get_message, save_callback_response, get_query_state
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "filter_kgraph_orphans"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


async def filter_kgraph_orphans(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    query_state = await get_query_state(query_id, logger)
    response_id = query_state[7]
    message = await get_message(response_id, logger)
    try:
        results = message.get("message", {}).get("results", [])
        nodes = set()
        edges = set()
        auxgraphs = set()
        # 1. Result node bindings
        for result in results:
            for qnode, knodes in result.get("node_bindings", {}).items():
                nodes.update([k["id"] for k in knodes])
        # 2. Result.Analysis edge bindings
        for result in results:
            for analysis in result.get("analyses", []):
                for qedge, kedges in analysis.get("edge_bindings", {}).items():
                    edges.update([k["id"] for k in kedges])
        # 3. Result.Analysis support graphs
        for result in results:
            for analysis in result.get("analyses", []):
                for auxgraph in analysis.get("support_graphs", []):
                    auxgraphs.add(auxgraph)
        # 4. Support graphs from edges in 2
        for edge in edges:
            for attribute in message.get("message", {}).get("knowledge_graph", {}).get("edges", {}).get(edge, {}).get("attributes", {}):
                if attribute.get("attribute_type_id", None) == "biolink:support_graphs":
                    auxgraphs.update(attribute.get("value", []))
        # 5. For all the auxgraphs collect their edges and nodes
        for auxgraph in auxgraphs:
            aux_edges = message.get("message", {}).get("auxiliary_graphs", {}).get(auxgraph, {}).get("edges", [])
            for aux_edge in aux_edges:
                if aux_edge not in message["message"]["knowledge_graph"]["edges"]:
                    logger.warning(f"{query_id}: aux_edge {aux_edge} not in knowledge_graph.edges")
                    continue
                edges.add(aux_edge)
                nodes.add(message["message"]["knowledge_graph"]["edges"][aux_edge]["subject"])
                nodes.add(message["message"]["knowledge_graph"]["edges"][aux_edge]["object"])
        #now remove all knowledge_graph nodes and edges that are not in our nodes and edges sets.
        kg_nodes = message.get("message", {}).get("knowledge_graph", {}).get("nodes", {})
        message["message"]["knowledge_graph"]["nodes"] = {nid: ndata for nid, ndata in kg_nodes.items() if nid in nodes}
        kg_edges = message.get("message",{}).get("knowledge_graph", {}).get("edges", {})
        message["message"]["knowledge_graph"]["edges"] = {eid: edata for eid, edata in kg_edges.items() if eid in edges}
        # validate_message(message)
        message["message"]["auxiliary_graphs"] = {
            auxgraph: adata
            for auxgraph, adata in message["message"].get("auxiliary_graphs", {}).items()
            if auxgraph in auxgraphs
        }
        # is_invalid = validate_message(message)
        # if is_invalid:
        #     before_is_invalid = validate_message(initial_message)
        #     if not before_is_invalid:
        #         with open("before_filtering_message.json", "w") as f:
        #             json.dump(initial_message, f, indent=2)
        #         with open("invalid_after_message.json", "w") as f:
        #             json.dump(message, f, indent=2)
        #     else:
        #         print("this message was bad to begin with")
        #         with open("invalid_before_message.json", "w") as f:
        #             json.dump(initial_message, f, indent=2)
    except KeyError as e:
        # can't find the right structure of message
        logger.error(f"Error filtering kgraph orphans: {e}")
        return message, 400

    # save merged message back to db
    await save_callback_response(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def poll_for_tasks():
    async for task, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(filter_kgraph_orphans(task, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
