"""Multi-curie (set-input) enrichment, answered in TRAPI 2.0.

Ported from AnswerCoalesce's MCQ path (``multi_curie_query`` in
``src/single_node_coalescer.py``, with ``coalesce_by_graph`` from
``src/graph_coalescence/graph_coalescer.py`` and the TRAPI builders from
``src/trapi.py``).

An MCQ is a one-hop query whose input qnode is a set: ``set_interpretation:
MANY``, one ``ids`` entry naming the set, and its ``member_ids``. Enrichment
finds the nodes linked to more of the members than chance predicts, and each
becomes one result with one analysis, for its best-scoring relation (every
qualifying relation is a sub-predicate of the queried one; see
``store.enrichment_candidates``):

* an inferred edge from the set to the enriched node, carrying its p-value and
  pointing (``biolink:support_graphs``) at one auxiliary graph per supporting
  member;
* each auxiliary graph is ``member -[member_of]-> set`` plus the knowledge-graph
  edge(s) linking that member to the enriched node.
"""

import hashlib
import math
from dataclasses import dataclass
from typing import Any, Optional

import orjson

from shepherd_utils.query_errors import QueryError
from shepherd_utils.shared import is_set_input_query
from shepherd_utils.trapi import make_binding, qedge_qualifier_sets

from . import store

INFORES = "infores:shepherd-graphenrich"
SUPPORT_GRAPHS = "biolink:support_graphs"
#: A node TRAPI 2.0 must categorize but the graph left uncategorized.
FALLBACK_CATEGORY = "biolink:NamedThing"
#: AnswerCoalesce's defaults for an MCQ that names no limits.
DEFAULT_PVALUE_THRESHOLD = 1e-5
DEFAULT_MAX_RESULTS = 2000


@dataclass(frozen=True)
class MCQ:
    """The parts of a TRAPI 2.0 MCQ that enrichment needs."""

    set_qnode_id: str
    set_id: str
    member_ids: tuple[str, ...]
    set_category: str
    answer_qnode_id: str
    answer_categories: tuple[str, ...]
    qedge_id: str
    set_is_subject: bool
    #: ``{"predicate": ..., <qualifier>: <value>, ...}``, the form the
    #: database's relations are keyed by.
    relation: dict


def _relation(qedge: dict) -> dict:
    predicates = qedge.get("predicates") or ["biolink:related_to"]
    if len(predicates) > 1:
        raise QueryError(
            "Set enrichment takes a single predicate on its query edge", 400
        )
    relation = {"predicate": predicates[0]}
    qualifier_sets = qedge_qualifier_sets(qedge)
    if len(qualifier_sets) > 1:
        raise QueryError(
            "Set enrichment takes at most one qualifier set on its query edge", 400
        )
    for qualifier in qualifier_sets[0] if qualifier_sets else []:
        key = qualifier["qualifier_type_id"].removeprefix("biolink:")
        relation[key] = qualifier["qualifier_value"]
    return relation


def parse_mcq(query_graph: dict) -> MCQ:
    """The MCQ in ``query_graph``; a ``QueryError`` if it isn't one we answer."""
    if not is_set_input_query(query_graph):
        raise QueryError(
            "Set enrichment answers one-hop queries from a set "
            "(set_interpretation MANY with member_ids) to one answer node",
            400,
        )
    nodes = query_graph["nodes"]
    set_qnode_id = next(
        qnode_id
        for qnode_id, qnode in nodes.items()
        if qnode.get("set_interpretation") == "MANY" and qnode.get("member_ids")
    )
    answer_qnode_id = next(q for q in nodes if q != set_qnode_id)
    set_qnode = nodes[set_qnode_id]
    set_ids = set_qnode.get("ids") or []
    if len(set_ids) != 1:
        raise QueryError("The set query node must have exactly one id", 400)
    qedge_id, qedge = next(iter(query_graph["edges"].items()))
    if {qedge.get("subject"), qedge.get("object")} != {set_qnode_id, answer_qnode_id}:
        raise QueryError("The query edge must connect the set to the answer node", 400)
    return MCQ(
        set_qnode_id=set_qnode_id,
        set_id=set_ids[0],
        member_ids=tuple(dict.fromkeys(set_qnode["member_ids"])),
        set_category=(set_qnode.get("categories") or [FALLBACK_CATEGORY])[0],
        answer_qnode_id=answer_qnode_id,
        answer_categories=tuple(
            nodes[answer_qnode_id].get("categories") or [FALLBACK_CATEGORY]
        ),
        qedge_id=qedge_id,
        set_is_subject=qedge["subject"] == set_qnode_id,
        relation=_relation(qedge),
    )


def pvalue_to_score(p_value: float, scale: float = 0.5, shift: float = 5) -> float:
    """AnswerCoalesce's score: a sigmoid of -log10(p), centred on p = 1e-5."""
    if p_value <= 0:
        return 1.0
    x = scale * (-math.log10(p_value) - shift)
    # 1 / (1 + e^-x), without overflowing for very large or small x
    if x >= 0:
        score = 1 / (1 + math.exp(-x))
    else:
        score = math.exp(x) / (1 + math.exp(x))
    return round(score, 5)


def _edge_id(*parts: Any) -> str:
    """A stable knowledge-graph edge id, so a rerun builds the same response."""
    digest = hashlib.sha256(orjson.dumps(parts, default=str)).hexdigest()
    return f"ac_{digest[:20]}"


def _qualifiers(relation: dict) -> list[dict]:
    return [
        {"qualifier_type_id": f"biolink:{key}", "qualifier_value": value}
        for key, value in relation.items()
        if key != "predicate" and value not in (None, "")
    ]


def _with_qualifiers(edge: dict, relation: dict) -> dict:
    qualifiers = _qualifiers(relation)
    if qualifiers:
        edge["qualifiers"] = qualifiers
    return edge


def _local_sources() -> list[dict]:
    return [{"resource_id": INFORES, "resource_role": "primary_knowledge_source"}]


def _aggregated_sources(sources: list[dict]) -> list[dict]:
    """``sources`` with this worker as the aggregator over their heads.

    The heads are the sources nobody else names upstream: what this service
    retrieved the edge from.
    """
    if not sources:
        return _local_sources()
    upstream = {u for s in sources for u in s.get("upstream_resource_ids") or []}
    heads = [s["resource_id"] for s in sources if s["resource_id"] not in upstream]
    aggregator = {
        "resource_id": INFORES,
        "resource_role": "aggregator_knowledge_source",
    }
    heads = list(dict.fromkeys(heads or [s["resource_id"] for s in sources]))
    aggregator["upstream_resource_ids"] = heads
    return [*sources, aggregator]


def _enrichment_parameters(parameters: dict) -> tuple[Optional[float], Optional[int]]:
    pvalue_threshold = parameters.get("pvalue_threshold", DEFAULT_PVALUE_THRESHOLD)
    max_results = parameters.get("max_results", DEFAULT_MAX_RESULTS)
    try:
        if pvalue_threshold is not None:
            pvalue_threshold = float(pvalue_threshold)
        if max_results is not None:
            max_results = int(max_results)
    except (TypeError, ValueError) as e:
        raise QueryError(
            "pvalue_threshold must be a number and max_results an integer", 400
        ) from e
    if max_results is not None and max_results < 1:
        raise QueryError("max_results must be a positive integer", 400)
    return pvalue_threshold, max_results


def find_enrichments(mcq: MCQ, parameters: dict) -> list[store.EnrichmentCandidate]:
    """The enriched features of the set, best p-value first."""
    pvalue_threshold, max_results = _enrichment_parameters(parameters)
    context_qualifiers = {
        key: value
        for key, value in mcq.relation.items()
        if key.endswith("_context_qualifier")
    }
    candidates, _ = store.enrichment_candidates(
        mcq.member_ids,
        mcq.set_category,
        input_is_subject=mcq.set_is_subject,
        node_constraints=list(mcq.answer_categories),
        predicate_constraints=[mcq.relation],
        context_qualifiers=context_qualifiers,
        pvalue_threshold=pvalue_threshold,
        max_results=max_results,
    )
    return candidates


def _support_links(candidate: store.EnrichmentCandidate) -> list[tuple[str, str, str]]:
    """The (subject, predicate_json, object) edges between the enriched node
    and the members that have the feature."""
    # For a symmetric relation either direction is stored; AnswerCoalesce
    # writes the enriched node as the subject.
    neighbor_is_subject = candidate.is_symmetric or not candidate.member_is_subject
    if neighbor_is_subject:
        return [
            (candidate.neighbor_curie, candidate.predicate_json, member)
            for member in candidate.linked_curies
        ]
    return [
        (member, candidate.predicate_json, candidate.neighbor_curie)
        for member in candidate.linked_curies
    ]


def build_response(
    query: dict, mcq: MCQ, candidates: list[store.EnrichmentCandidate]
) -> dict:
    """The TRAPI 2.0 response (message only) for ``candidates``."""
    links = [link for c in candidates for link in _support_links(c)]
    provenance = store.get_edge_provenance(links)
    names = store.get_node_names(
        [c.neighbor_curie for c in candidates] + list(mcq.member_ids)
    )

    nodes: dict[str, dict] = {}
    edges: dict[str, dict] = {}
    aux_graphs: dict[str, dict] = {}
    results: list[dict] = []

    def add_node(curie: str, categories, **extra) -> None:
        if curie in nodes:
            return
        node = {"categories": list(dict.fromkeys(categories)) or [FALLBACK_CATEGORY]}
        if names.get(curie):
            node["name"] = names[curie]
        node.update(extra)
        nodes[curie] = node

    add_node(mcq.set_id, [mcq.set_category], is_set=True)
    member_of: dict[str, str] = {}
    for member in mcq.member_ids:
        add_node(member, [mcq.set_category])
        edge_id = _edge_id("member_of", member, mcq.set_id)
        edges[edge_id] = {
            "subject": member,
            "predicate": "biolink:member_of",
            "object": mcq.set_id,
            # Membership is what the query asserted; nothing more is known.
            "knowledge_level": store.NOT_PROVIDED,
            "agent_type": store.NOT_PROVIDED,
            "sources": _local_sources(),
        }
        member_of[member] = edge_id

    for candidate in candidates:
        relation = orjson.loads(candidate.predicate_json)
        add_node(candidate.neighbor_curie, candidate.neighbor_categories)

        support_graph_ids = []
        for subject, predicate_json, object_ in _support_links(candidate):
            member = object_ if subject == candidate.neighbor_curie else subject
            evidence = provenance.get((subject, predicate_json, object_)) or [None]
            support_edge_ids = []
            for record in evidence:
                edge = {
                    "subject": subject,
                    "predicate": relation["predicate"],
                    "object": object_,
                }
                if record is None:
                    edge.update(
                        knowledge_level=store.NOT_PROVIDED,
                        agent_type=store.NOT_PROVIDED,
                        sources=_local_sources(),
                    )
                else:
                    edge.update(
                        knowledge_level=record.knowledge_level,
                        agent_type=record.agent_type,
                        sources=_aggregated_sources(record.sources),
                    )
                _with_qualifiers(edge, relation)
                edge_id = _edge_id("support", edge)
                edges[edge_id] = edge
                support_edge_ids.append(edge_id)
            aux_id = _edge_id("aux", candidate.predicate_json, subject, object_)
            aux_graphs[aux_id] = {
                "edges": list(dict.fromkeys([*support_edge_ids, member_of[member]]))
            }
            support_graph_ids.append(aux_id)

        if mcq.set_is_subject:
            subject, object_ = mcq.set_id, candidate.neighbor_curie
        else:
            subject, object_ = candidate.neighbor_curie, mcq.set_id
        inferred_edge = _with_qualifiers(
            {
                "subject": subject,
                "predicate": relation["predicate"],
                "object": object_,
                "knowledge_level": "prediction",
                "agent_type": "computational_model",
                "sources": _local_sources(),
                "attributes": [
                    {
                        "attribute_type_id": "biolink:p_value",
                        "value": candidate.p_value,
                    },
                    {
                        "attribute_type_id": SUPPORT_GRAPHS,
                        "value": support_graph_ids,
                    },
                ],
            },
            relation,
        )
        inferred_edge_id = _edge_id(
            "enrichment", subject, candidate.predicate_json, object_
        )
        edges[inferred_edge_id] = inferred_edge

        # The store returns each enriched node once, with its best relation.
        results.append(
            {
                "node_bindings": {
                    mcq.set_qnode_id: make_binding([mcq.set_id]),
                    mcq.answer_qnode_id: make_binding([candidate.neighbor_curie]),
                },
                "analyses": [
                    {
                        "resource_id": INFORES,
                        "edge_bindings": {
                            mcq.qedge_id: make_binding([inferred_edge_id])
                        },
                        "score": pvalue_to_score(candidate.p_value),
                        "scoring_method": "graphenrich_enrichment_pvalue",
                    }
                ],
            }
        )

    message: dict[str, Any] = {
        "query_graph": query["message"]["query_graph"],
        "knowledge_graph": {"nodes": nodes, "edges": edges},
        "results": results,
    }
    if aux_graphs:
        message["auxiliary_graphs"] = aux_graphs
    return {"message": message}


def answer_mcq(query: dict, parameters: Optional[dict] = None) -> dict:
    """Enrich the set in ``query`` and return the TRAPI 2.0 response."""
    mcq = parse_mcq(query["message"]["query_graph"])
    candidates = find_enrichments(mcq, parameters or {})
    return build_response(query, mcq, candidates)
