"""Pre-merge processing of arriving ARA responses.

Ported from NCATSTranslator/Relay @ 3e65975 tr_sys/tr_ars/utils.py:
get_safe, add_attribute, add_log_entry, scrub_null_attributes,
decorate_edges_with_infores, normalizeScores, ScoreStatCalc,
normalize_scores, remove_phantom_support_graphs, pre_merge_process,
appraise_confidence, get_confidence.

Faithful port, minus the upstream crashes (the UnboundLocalError when an
edge has non-empty sources but no primary_knowledge_source, the IndexError
when only some results carry scores) -- see docs/ARS_PARITY_REGISTER.md.
Node normalization is intentionally absent from pre_merge_process, matching
upstream master (Relay PR #871 removed the call).

TRAPI 2.0 (Shepherd speaks 2.0; upstream is 1.5): nothing here emits a null
or a forbidden empty (``add_attribute``, ``_self_source``), log timestamps
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


def _self_source(inforesid, role):
    """A fresh retrieval source for this agent.

    Built per edge on purpose. Upstream reused one dict for the whole graph,
    so a later edge flipping the role mutated the source already appended to
    every earlier edge -- the last edge to need a role decided the role every
    edge reported.

    Upstream also set ``source_record_urls: None`` and
    ``upstream_resource_ids: []``; TRAPI 2.0 forbids both (no nulls, and
    ``upstream_resource_ids`` has minItems 1), so the source carries only
    what it knows.
    """
    return {"resource_id": inforesid, "resource_role": role}


def decorate_edges_with_infores(data, inforesid):
    edges = get_safe(data, "message", "knowledge_graph", "edges")
    if inforesid is None:
        inforesid = "infores:unknown"
    if edges is not None:
        for key, edge in edges.items():
            has_self = False
            # upstream only ever assigned has_primary inside the loop below,
            # so a non-empty sources list with no primary_knowledge_source
            # raised UnboundLocalError and failed the whole callback
            has_primary = False
            if (
                "sources" not in edge.keys()
                or edge["sources"] is None
                or len(edge["sources"]) == 0
            ):
                edge["sources"] = [_self_source(inforesid, "primary_knowledge_source")]
            else:
                for source in edge["sources"]:
                    if source.get("resource_id") == inforesid:
                        has_self = True
                    if source.get("resource_role") == "primary_knowledge_source":
                        has_primary = True
                if not has_self:
                    logger.info("decorateEdges: found lacking self")
                    role = (
                        "aggregator_knowledge_source"
                        if has_primary
                        else "primary_knowledge_source"
                    )
                    edge["sources"].append(_self_source(inforesid, role))


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
                score = None
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
                # TRAPI 2.0 makes Result.analyses optional (and forbids an
                # empty list), so a result without analyses is ordinary: it
                # has no score to contribute. Upstream (1.5, analyses
                # required) abandoned the whole batch here.
                continue

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
    # null-attribute scrubbing removed upstream (Relay PR #885) and node
    # normalization before it (Relay PR #871): data arrives pre-normalized.
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


def appraise_confidence(results):
    """Compute ordering_components locally; replaced the external Appraiser
    call in post_process (Relay PR #884)."""
    for result in results:
        confidence = get_confidence(result)
        result["ordering_components"] = {
            "confidence": confidence,
            "clinical_evidence": 0.0,
            "novelty": 0.0,
        }


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
