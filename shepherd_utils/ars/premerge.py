"""Pre-merge processing of arriving ARA responses.

Ported from NCATSTranslator/Relay @ 84a66c1 tr_sys/tr_ars/utils.py:
get_safe, add_attribute, add_log_entry, normalizeScores, ScoreStatCalc,
normalize_scores, remove_phantom_support_graphs, pre_merge_process,
appraise_confidence, get_confidence.

Upstream no longer calls ``pre_merge_process`` or
``remove_phantom_support_graphs`` on an arriving response (Relay PR #898),
and neither does the ars_premerge worker. They are kept, with their goldens,
as the record of what upstream's functions do.

``decorate_edges_with_infores`` is gone: the ARS no longer stamps its ARA's
infores on edge sources, it reports the edges that lack it
(``edges_missing_infores``). See docs/ARS_PARITY_REGISTER.md, deviation 21.

Faithful port, minus the upstream crash (the IndexError when only some
results carry scores) -- see docs/ARS_PARITY_REGISTER.md.
Node normalization is intentionally absent from pre_merge_process, matching
upstream master (Relay PR #871 removed the call).

TRAPI 2.0 (Shepherd speaks 2.0; upstream is 1.5): nothing here emits a null
or a forbidden empty (``add_attribute``), log timestamps
are RFC 3339 (``log_timestamp``), and a result without analyses -- optional
in 2.0 -- is skipped by the score passes instead of aborting them.
"""

import logging
import statistics
from datetime import datetime, timezone
from typing import Optional

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


def response_results(data):
    """The results an arriving ARA response is counted and merged by.

    Upstream's callback (Relay PRs #894/#895): a response whose knowledge
    graph has no nodes counts as having no results (``[]``), whatever its
    results list says -- a Pathfinder answer could come back with a
    "result" and an empty graph, which broke post-processing. Otherwise the
    response's results list, or None when it has none.
    """
    res = get_safe(data, "message", "results")
    nodes = get_safe(data, "message", "knowledge_graph", "nodes")
    if not nodes:
        res = []
    return res


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


#: The TRAPI Attribute members add_attribute copies across. Upstream built a
#: template with every one of them set to None and overwrote the ones the
#: caller supplied, so every attribute it added carried explicit nulls --
#: invalid in TRAPI 2.0, which has no nullable members. Only the members the
#: caller actually supplied (non-null) are copied now.
_ATTRIBUTE_MEMBERS = (
    "value",
    "value_url",
    "attributes",
    "description",
    "value_type_id",
    "attribute_source",
    "attribute_type_id",
    "original_attribute_name",
)


def add_attribute(node_or_edge, attribute_json):
    attribute = {
        key: attribute_json[key]
        for key in _ATTRIBUTE_MEMBERS
        if attribute_json.get(key) is not None
    }
    if isinstance(node_or_edge.get("attributes"), list):
        node_or_edge["attributes"].append(attribute)
    else:
        node_or_edge["attributes"] = [attribute]


def edges_missing_infores(data, inforesid):
    """Ids of the knowledge-graph edges whose ``sources`` do not name
    ``inforesid``, in the response's order. Pure: nothing is modified.

    Replaces upstream's ``decorate_edges_with_infores``, which appended a
    retrieval source for the ARA to every such edge (or, for an edge with no
    sources at all, made the ARA its primary source). The ARS no longer
    edits an ARA's provenance: every hosted ARA is expected to name itself
    on every edge it returns, and an edge that doesn't is reported (see the
    ars_premerge worker) so it can be fixed in the ARA. See
    docs/ARS_PARITY_REGISTER.md, deviation 21.

    An unknown ARA (``inforesid`` None) has nothing to check against.
    """
    if inforesid is None:
        return []
    edges = get_safe(data, "message", "knowledge_graph", "edges")
    if not isinstance(edges, dict):
        return []
    missing = []
    for key, edge in edges.items():
        sources = edge.get("sources") if isinstance(edge, dict) else None
        if not any(
            isinstance(source, dict) and source.get("resource_id") == inforesid
            for source in sources or []
        ):
            missing.append(key)
    return missing


def ScoreStatCalc(results):
    """Summary statistics of the results' ``ordering_components.confidence``
    (Relay PR #891 moved them off the analyses' scores). A result without
    an ordering_components dict, or without a confidence, contributes
    nothing; fewer than two confidences give ``{}``."""
    stat = {}
    confidence_list = []
    if results is not None and len(results) > 0:
        for res in results:
            ordering_components = res.get("ordering_components")
            if not isinstance(ordering_components, dict):
                logger.debug("Result has no valid ordering_components dictionary")
                continue

            confidence = ordering_components.get("confidence")

            if confidence is not None:
                confidence_list.append(confidence)
            else:
                logger.debug("Result doesnt have confidence field")

        try:
            if len(confidence_list) <= 1:
                return stat
            stat["median"] = statistics.median(confidence_list)
            stat["mean"] = statistics.mean(confidence_list)
            stat["stdev"] = statistics.stdev(confidence_list)
            stat["minimum"] = min(confidence_list)
            stat["maximum"] = max(confidence_list)
        except Exception as e:
            logger.error(f"Error in calculating statistics: {e}")
            return stat
    return stat


def normalizeScores(results):
    scoreList = []
    scoredResults = []
    if results is not None and len(results) > 0:
        for res in results:
            if (
                "analyses" in res.keys()
                and res["analyses"] != []
                and res["analyses"] is not None
            ):
                score = None
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
                    scoredResults.append(res)
            else:
                # unscored: see ScoreStatCalc (upstream aborted the batch)
                continue

        # .tolist() (not list()) so ranks are plain Python floats: rankdata
        # yields numpy.float64, which upstream's stdlib-json storage accepts
        # but Shepherd's orjson blob codec rejects. Same numeric values.
        ranked = (
            (rankdata(scoreList) * 100 / len(scoreList)).tolist() if scoreList else []
        )
        if len(ranked) != len(scoredResults):
            logger.debug("Score normalization aborted. Score list lengths not equal")
            return results
        # Assign each rank back to the result it was computed from. Upstream
        # popped one rank per RESULT while ranking only the score-bearing
        # ones, so any response mixing scored and unscored results ran the
        # list dry and raised IndexError, failing the whole callback -- and
        # before it did, it handed the wrong result each score.
        for result, rank in zip(scoredResults, ranked):
            result["normalized_score"] = rank
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
    """Remove every ``biolink:support_graphs`` edge attribute that names an
    auxiliary graph the response does not contain, in place.

    Returns ``{edge_id: [missing aux graph ids]}`` for the edges it changed,
    so the caller can report them: a dangling reference is an ARA bug, and
    removing it silently (as upstream did) hid it.

    A response with support-graph attributes but no ``auxiliary_graphs`` at
    all has nothing those attributes could reference, so they are all
    dangling and are removed too. Upstream skipped the whole pass in that
    case (with only a debug line) and the dangling references went on into
    the merged message.
    """
    edges = get_safe(response, "message", "knowledge_graph", "edges")
    if edges is None:
        return {}
    aux_graphs = get_safe(response, "message", "auxiliary_graphs") or {}
    phantoms = {}
    for edge_i, edge in edges.items():
        if "attributes" in edge.keys() and edge["attributes"] is not None:
            attributes = edge["attributes"]
            removal_list = []
            missing = []
            for attribute in attributes:
                # .get: an attribute without a type id is invalid TRAPI and
                # fails validation as such, rather than crashing this pass
                if attribute.get("attribute_type_id") == "biolink:support_graphs":
                    for value in attribute["value"]:
                        if value not in aux_graphs:
                            if value not in missing:
                                missing.append(value)
                            if attribute not in removal_list:
                                removal_list.append(attribute)
            for bad in removal_list:
                if bad in attributes:
                    attributes.remove(bad)
            if missing:
                phantoms[edge_i] = missing
    return phantoms


def pre_merge_process(data, key, agent_name, inforesid):
    """The per-callback processing pipeline, ported from utils.py.

    Normalizes the scores in place and returns the ids of the edges whose
    sources lack the ARA's infores (``edges_missing_infores``), which
    upstream stamped here and the port only reports.

    Raises on any stage failure (the caller marks the child errored, exactly
    as the upstream callback handler's generic except does).
    """
    # null-attribute scrubbing removed upstream (Relay PR #885) and node
    # normalization before it (Relay PR #871): data arrives pre-normalized.
    missing_infores = edges_missing_infores(data, inforesid)
    try:
        normalize_scores(data, key, agent_name)
    except Exception as e:
        logger.exception("Error in ARS score normalization")
        raise e
    return missing_infores


def appraise_confidence(results):
    """Compute ordering_components locally (replaced the external Appraiser
    call in post_process, Relay PR #884) and order the results by
    confidence, highest first (Relay PR #891)."""
    for result in results:
        confidence = get_confidence(result)
        result["ordering_components"] = {
            "confidence": confidence,
            "clinical_evidence": 0.0,
            "novelty": 0.0,
        }
    # highest confidence first (Relay PR #891); a stable sort, so ties keep
    # their merged order
    results.sort(
        key=lambda result: result["ordering_components"]["confidence"],
        reverse=True,
    )


def get_confidence(result):
    """1 - prod(1 - score) over the result's scored analyses."""
    score_product = 1
    for analysis in result.get("analyses") or []:
        if analysis.get("score") is not None:
            score_product = score_product * (1 - analysis["score"])
    confidence_score = 1 - score_product
    return confidence_score


def log_timestamp(dt: Optional[datetime] = None) -> str:
    """An RFC 3339 UTC timestamp for a TRAPI log entry.

    Upstream stamped its log entries with a bare ``%H:%M:%S`` wall-clock
    time; TRAPI 2.0's LogEntry.timestamp is a zoned date-time. A naive
    ``dt`` is taken to be UTC.
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
