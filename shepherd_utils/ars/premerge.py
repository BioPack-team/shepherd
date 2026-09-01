"""Pre-merge processing of arriving ARA responses.

Ported from NCATSTranslator/Relay @ dd1e71b tr_sys/tr_ars/utils.py:
get_safe, add_attribute, add_log_entry, scrub_null_attributes,
decorate_edges_with_infores, normalizeScores, ScoreStatCalc,
normalize_scores, remove_phantom_support_graphs, pre_merge_process.

Faithful port -- upstream quirks (the UnboundLocalError when an edge has
non-empty sources but no primary_knowledge_source, the IndexError when only
some results carry scores) are pinned by the golden parity tests. Node
normalization is intentionally absent from pre_merge_process, matching
upstream master (Relay PR #871 removed the call).
"""

import logging
import statistics
from datetime import datetime

from scipy.stats import rankdata

logger = logging.getLogger(__name__)


def get_safe(element, *keys):
    """Traverse nested dicts, returning the terminal value or None."""
    if element is None:
        return None
    _element = element
    for key in keys:
        try:
            _element = _element[key]
            if _element is None:
                return None
            if key == keys[-1]:
                return _element
        except KeyError:
            return None
    return None


def add_log_entry(data, log_tuple):
    """log_tuple = (message, timestamp, level)."""
    log_entry = {
        "message": log_tuple[0],
        "timestamp": log_tuple[1],
        "level": log_tuple[2],
    }
    if "logs" in data.keys():
        data["logs"].append(log_entry)
    else:
        data["logs"] = [log_entry]


def add_attribute(node_or_edge, attribute_json):
    template_attribute = {
        "value": None,
        "value_url": None,
        "attributes": None,
        "description": None,
        "value_type_id": None,
        "attribute_source": None,
        "attribute_type_id": None,
        "original_attribute_name": None,
    }
    for key in attribute_json.keys():
        if key is not None and key in template_attribute.keys():
            template_attribute[key] = attribute_json[key]
    if "attributes" in node_or_edge.keys():
        node_or_edge["attributes"].append(template_attribute)
    else:
        node_or_edge["attributes"] = [template_attribute]


def scrub_null_attributes(data):
    nodes = get_safe(data, "message", "knowledge_graph", "nodes")
    edges = get_safe(data, "message", "knowledge_graph", "edges")
    aux_graphs = get_safe(data, "message", "auxiliary_graphs")
    if nodes is not None:
        for nodeId, nodeStuff in nodes.items():
            nodeAttributes = get_safe(nodeStuff, "attributes")
            if nodeAttributes is not None:
                while None in nodeAttributes:
                    nodeAttributes.remove(None)

    if edges is not None:
        bad_sources = []
        for edgeId, edgeStuff in edges.items():
            edgeAttributes = get_safe(edgeStuff, "attributes")
            if edgeAttributes is not None:
                while None in edgeAttributes:
                    edgeAttributes.remove(None)
                for edgeAttribute in edgeAttributes:
                    if "attributes" in edgeAttribute.keys():
                        edgeAttributeAttributes = get_safe(edgeAttribute, "attributes")
                        if edgeAttributeAttributes is None:
                            edgeAttribute["attributes"] = []

            edgeSources = get_safe(edgeStuff, "sources")
            sources_to_remove = {}
            for edge_source in edgeSources:
                if (
                    "resource_id" not in edge_source.keys()
                    or edge_source["resource_id"] is None
                ):
                    if edgeId not in sources_to_remove.keys():
                        sources_to_remove[edgeId] = [edge_source]
                    else:
                        sources_to_remove[edgeId].append(edge_source)

                if "upstream_resource_ids" not in edge_source.keys() or (
                    "upstream_resource_ids" in edge_source.keys()
                    and edge_source["upstream_resource_ids"] is None
                ):
                    edge_source["upstream_resource_ids"] = []
                if "upstream_resource_ids" in edge_source.keys() and isinstance(
                    edge_source["upstream_resource_ids"], list
                ):
                    while None in edge_source["upstream_resource_ids"]:
                        edge_source["upstream_resource_ids"].remove(None)

            if len(sources_to_remove) > 0:
                bad_sources.append(sources_to_remove)
            for key, sources in sources_to_remove.items():
                for source in sources:
                    edgeSources.remove(source)
    if aux_graphs is not None:
        for aux_graph_id, aux_graph in aux_graphs.items():
            if "attributes" in aux_graph.keys() and aux_graph["attributes"] is None:
                aux_graph["attributes"] = []


def decorate_edges_with_infores(data, inforesid):
    edges = get_safe(data, "message", "knowledge_graph", "edges")
    if inforesid is None:
        inforesid = "infores:unknown"
    # NOTE: deliberately shared across every edge, like upstream -- a later
    # edge's role flip mutates the same dict already appended to an earlier
    # edge's sources.
    self_source = {
        "resource_id": inforesid,
        "resource_role": "primary_knowledge_source",
        "source_record_urls": None,
        "upstream_resource_ids": [],
    }
    if edges is not None:
        for key, edge in edges.items():
            has_self = False
            if (
                "sources" not in edge.keys()
                or edge["sources"] is None
                or len(edge["sources"]) == 0
            ):
                edge["sources"] = [self_source]
            else:
                for source in edge["sources"]:
                    if source["resource_id"] == inforesid:
                        has_self = True
                    if source["resource_role"] == "primary_knowledge_source":
                        has_primary = True
                if not has_self:
                    # upstream: has_primary is only ever assigned above, so a
                    # non-empty sources list with no primary raises
                    # UnboundLocalError here -- kept for parity.
                    if has_primary:  # noqa: F821
                        self_source["resource_role"] = "aggregator_knowledge_source"
                    else:
                        self_source["resource_role"] = "primary_knowledge_source"
                    edge["sources"].append(self_source)


def ScoreStatCalc(results):
    stat = {}
    scoreList = []
    if results is not None and len(results) > 0:
        for res in results:
            if (
                "analyses" in res.keys()
                and res["analyses"] != []
                and res["analyses"] is not None
            ):
                if len(res["analyses"]) > 1:
                    temp_score = []
                    for analysis in res["analyses"]:
                        if "score" in analysis.keys() and analysis["score"] is not None:
                            temp_score.append(analysis["score"])
                    if len(temp_score) > 0:
                        score = statistics.mean(temp_score)
                    else:
                        score = None

                elif len(res["analyses"]) == 1:
                    if "score" in res["analyses"][0]:
                        score = res["analyses"][0]["score"]
                    else:
                        score = None

                if score is not None:
                    scoreList.append(score)
            else:
                logger.error("Results dont have the required fields")
                return stat

        try:
            if len(scoreList) <= 1:
                return stat
            stat["median"] = statistics.median(scoreList)
            stat["mean"] = statistics.mean(scoreList)
            stat["stdev"] = statistics.stdev(scoreList)
            stat["minimum"] = min(scoreList)
            stat["maximum"] = max(scoreList)
        except Exception as e:
            logger.error(f"Error in calculating statistics: {e}")
            return stat
    return stat


def normalizeScores(results):
    scoreList = []
    if results is not None and len(results) > 0:
        for res in results:
            if (
                "analyses" in res.keys()
                and res["analyses"] != []
                and res["analyses"] is not None
            ):
                if len(res["analyses"]) > 1:
                    temp_score = []
                    for analysis in res["analyses"]:
                        if "score" in analysis.keys():
                            if analysis["score"] is not None:
                                temp_score.append(analysis["score"])
                            else:
                                logger.error(
                                    "Analyses score field is null, setting it to zero"
                                )
                                analysis["score"] = 0
                                temp_score.append(analysis["score"])
                    if len(temp_score) > 0:
                        score = statistics.mean(temp_score)
                    else:
                        score = None

                elif len(res["analyses"]) == 1:
                    if "score" in res["analyses"][0]:
                        if res["analyses"][0]["score"] is not None:
                            score = res["analyses"][0]["score"]
                        else:
                            score = 0
                    else:
                        logger.debug("Result doesnt have score field")
                        score = None

                if score is not None:
                    scoreList.append(score)
            else:
                logger.error("Results dont have the required fields")
                return results

        ranked = list(rankdata(scoreList) * 100 / len(scoreList)) if scoreList else []
        if len(ranked) != len(scoreList):
            logger.debug("Score normalization aborted. Score list lengths not equal")
            return results
        if ranked:
            # upstream pops one rank per RESULT while only score-bearing
            # results were ranked -- a mixed corpus raises IndexError, and
            # that error-parity is intentional (the callback errors out).
            for result in results:
                result["normalized_score"] = ranked.pop(0)
    return results


def normalize_scores(data, key, agent_name):
    res = get_safe(data, "message", "results")
    if res is not None:
        if len(res) > 0:
            try:
                data["message"]["results"] = normalizeScores(res)
            except Exception as e:
                logger.error(
                    f"Failed to normalize scores for agent: {agent_name} "
                    f"and pk: {key}"
                )
                raise e


def remove_phantom_support_graphs(response):
    edges = get_safe(response, "message", "knowledge_graph", "edges")
    aux_graphs = get_safe(response, "message", "auxiliary_graphs")
    if edges is not None and aux_graphs is not None:
        for edge_i, edge in edges.items():
            if "attributes" in edge.keys() and edge["attributes"] is not None:
                attributes = edge["attributes"]
                removal_list = []
                for attribute in attributes:
                    if attribute["attribute_type_id"] == "biolink:support_graphs":
                        for value in attribute["value"]:
                            if value not in aux_graphs:
                                if attribute not in removal_list:
                                    removal_list.append(attribute)
                for bad in removal_list:
                    if bad in attributes:
                        attributes.remove(bad)
    else:
        logger.debug(
            "Response lacking edges and/or auxiliary_graphs. "
            "No phantom support graphs to remove."
        )


def pre_merge_process(data, key, agent_name, inforesid):
    """The per-callback processing pipeline, ported from utils.py.

    Raises on any stage failure (the caller marks the child errored, exactly
    as the upstream callback handler's generic except does).
    """
    try:
        scrub_null_attributes(data)
    except Exception as e:
        logger.exception("Error in the scrubbing of null attributes")
        raise e
    # node normalization removed upstream (Relay PR #871): data arrives
    # pre-normalized.
    try:
        decorate_edges_with_infores(data, inforesid)
    except Exception as e:
        logger.exception("Error in ARS edge source decoration")
        raise e
    try:
        normalize_scores(data, key, agent_name)
    except Exception as e:
        logger.exception("Error in ARS score normalization")
        raise e


def timestamp_hms() -> str:
    """The %H:%M:%S wall-clock stamp upstream writes into TRAPI logs."""
    return datetime.now().strftime("%H:%M:%S")
