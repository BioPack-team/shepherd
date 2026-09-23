"""TRAPI 2.0 protocol helpers shared by the Shepherd server and its workers.

Shepherd speaks TRAPI 2.0, and everything TRAPI-shaped is described by
``translator_tom`` (TOM), the Translator-wide TRAPI object model, rather than
being re-derived here. TOM is used the same way Gandalf and Retriever use it:

- **Request validation and the OpenAPI schema** come from TOM's models (see
  ``validate_query`` and ``shepherd_server.base_routes``).
- **Internals stay plain dicts, typed with TOM's TypedDicts**
  (``translator_tom.model_dicts``). A TRAPI response here can carry hundreds of
  thousands of edges and is reloaded by every worker that touches it, so
  building Pydantic models for it would multiply CPU and memory for nothing; a
  ``TypedDict`` is free at runtime.

What TRAPI 2.0 changed that the pipeline depends on
---------------------------------------------------
- A ``NodeBinding`` / ``EdgeBinding`` / ``PathBinding`` is **one object with an
  ``ids`` array** per query node / edge / path, not a list of objects each with
  an ``id``. Use ``binding_ids`` / ``add_binding_ids`` rather than indexing.
- ``Edge.knowledge_level`` and ``Edge.agent_type`` are **required top-level
  Edge properties**, no longer ``biolink:knowledge_level`` /
  ``biolink:agent_type`` attributes.
- ``AuxiliaryGraph`` lost ``attributes``; ``NodeBinding`` / ``EdgeBinding``
  lost ``attributes`` (and ``query_id``).
- A QEdge's ``qualifier_constraints`` and ``attribute_constraints`` became one
  ``constraints`` object, whose ``qualifiers`` entries are plain
  ``{qualifier_type_id: qualifier_value}`` mappings (see
  ``qedge_qualifier_sets``). A QPath constraint's ``intermediate_categories``
  is now ``required_intermediate_categories``.
- ``log_level`` and ``bypass_cache`` moved from the top of the query into
  ``parameters`` (alongside the new ``timeout``), and the server MUST repeat
  ``parameters`` in its Response (``finalize_response``).

Nulls and empty containers
--------------------------
2.0 is an OpenAPI 3.1 document with no ``nullable`` anywhere, so **an absent
value must be an absent property, never null**. Empty containers are
per-property: optional properties with ``minItems``/``minProperties`` of 1
(``Message.auxiliary_graphs``, ``Response.logs``, ``Result.analyses``,
``Analysis.edge_bindings`` / ``support_graphs``, ``Edge.qualifiers``,
``RetrievalSource.upstream_resource_ids`` ...) must be omitted when empty,
while ``Message.results`` SHOULD be ``[]`` when a query found nothing, and
``KnowledgeGraph.nodes`` / ``.edges`` may be empty. ``finalize_response`` applies
those rules once, at the point a response leaves Shepherd.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, cast

from functools import cache

from pydantic import TypeAdapter, ValidationError
from translator_tom import TRAPI_CONFIG
from translator_tom.model_dicts import (
    EdgeDict,
    MessageDict,
    QEdgeDict,
    QualifierDict,
    QueryDict,
    QueryParametersDict,
    ResponseDict,
    dict_up_version,
)
from translator_tom.v1_6 import Response as V16Response
from translator_tom.v2_0._version import SCHEMA_VERSION

__all__ = [
    "BIOLINK_VERSION",
    "SCHEMA_VERSION",
    "TRAPIRequestError",
    "add_binding_ids",
    "binding_ids",
    "edge_binding_ids",
    "edge_support_graphs",
    "finalize_response",
    "is_trapi_1_response",
    "make_binding",
    "node_binding_ids",
    "normalize_query_graph",
    "prune_response",
    "qedge_qualifier_sets",
    "query_bypass_cache",
    "query_log_level",
    "query_parameters",
    "upgrade_trapi_1_response",
    "validate_query",
]

#: Biolink Model version the TRAPI 2.0 models (and so Shepherd's responses) use.
BIOLINK_VERSION: str = TRAPI_CONFIG.biolink_version

SUPPORT_GRAPHS_ATTRIBUTE = "biolink:support_graphs"


class TRAPIRequestError(ValueError):
    """A request body that is not a valid TRAPI 2.0 query.

    The message is client-safe and returned verbatim as the HTTP 400 detail.
    """


# ---------------------------------------------------------------------------
# Query parameters
# ---------------------------------------------------------------------------


def query_parameters(query: Optional[Mapping[str, Any]]) -> QueryParametersDict:
    """The query's TRAPI 2.0 ``parameters`` object, or ``{}`` if it has none.

    >>> query_parameters({"parameters": {"log_level": "DEBUG"}})
    {'log_level': 'DEBUG'}
    >>> query_parameters({"message": {}})
    {}
    """
    parameters = (query or {}).get("parameters")
    if not isinstance(parameters, dict):
        return cast("QueryParametersDict", {})
    return cast("QueryParametersDict", parameters)


def query_log_level(query: Optional[Mapping[str, Any]]) -> Optional[str]:
    """The ``parameters.log_level`` the client asked for, if any.

    >>> query_log_level({"parameters": {"log_level": "DEBUG"}})
    'DEBUG'
    >>> query_log_level({"log_level": "DEBUG"}) is None
    True
    """
    return query_parameters(query).get("log_level")


def query_bypass_cache(query: Optional[Mapping[str, Any]]) -> bool:
    """Whether the client set ``parameters.bypass_cache``.

    >>> query_bypass_cache({"parameters": {"bypass_cache": True}})
    True
    >>> query_bypass_cache({})
    False
    """
    return query_parameters(query).get("bypass_cache") is True


# ---------------------------------------------------------------------------
# Bindings
# ---------------------------------------------------------------------------


def binding_ids(binding: Any) -> list[str]:
    """The ``ids`` of a Node/Edge/PathBinding, tolerating an absent binding.

    >>> binding_ids({"ids": ["A", "B"]})
    ['A', 'B']
    >>> binding_ids(None)
    []
    """
    if not isinstance(binding, dict):
        return []
    return binding.get("ids") or []


def node_binding_ids(result: Mapping[str, Any], qnode_id: str) -> list[str]:
    """The knowledge-graph node ids a result binds to ``qnode_id``.

    >>> node_binding_ids({"node_bindings": {"n0": {"ids": ["X:1"]}}}, "n0")
    ['X:1']
    >>> node_binding_ids({"node_bindings": {}}, "n0")
    []
    """
    return binding_ids((result.get("node_bindings") or {}).get(qnode_id))


def edge_binding_ids(analysis: Mapping[str, Any]) -> list[str]:
    """Every knowledge-graph edge id an analysis binds, across all its qedges.

    >>> edge_binding_ids({"edge_bindings": {"e0": {"ids": ["a"]}, "e1": {"ids": ["b"]}}})
    ['a', 'b']
    """
    ids: list[str] = []
    for binding in (analysis.get("edge_bindings") or {}).values():
        ids.extend(binding_ids(binding))
    return ids


def make_binding(ids: Iterable[str]) -> dict[str, list[str]]:
    """A 2.0 binding object over ``ids``, de-duplicated in order.

    >>> make_binding(["a", "b", "a"])
    {'ids': ['a', 'b']}
    """
    return {"ids": list(dict.fromkeys(ids))}


def add_binding_ids(bindings: dict[str, Any], key: str, ids: Iterable[str]) -> None:
    """Union ``ids`` into ``bindings[key]``, creating the binding if needed.

    >>> b = {"e0": {"ids": ["a"]}}
    >>> add_binding_ids(b, "e0", ["b", "a"]); add_binding_ids(b, "e1", ["c"])
    >>> b
    {'e0': {'ids': ['a', 'b']}, 'e1': {'ids': ['c']}}
    """
    existing = bindings.get(key)
    if existing is None:
        bindings[key] = make_binding(ids)
        return
    existing["ids"] = list(dict.fromkeys([*binding_ids(existing), *ids]))


# ---------------------------------------------------------------------------
# Knowledge graph and query graph
# ---------------------------------------------------------------------------


def edge_support_graphs(edge: Mapping[str, Any]) -> list[str]:
    """The auxiliary graph ids an edge's ``biolink:support_graphs`` names.

    Tolerant of absent / null ``attributes`` on edges from other services.

    >>> edge_support_graphs({"attributes": [{"attribute_type_id": "biolink:support_graphs", "value": ["a1"]}]})
    ['a1']
    >>> edge_support_graphs({})
    []
    """
    support: list[str] = []
    for attribute in edge.get("attributes") or []:
        if attribute.get("attribute_type_id") == SUPPORT_GRAPHS_ATTRIBUTE:
            support.extend(attribute.get("value") or [])
    return support


def qedge_qualifier_sets(qedge: QEdgeDict) -> list[list[QualifierDict]]:
    """A QEdge's qualifier constraints, each as a list of KG ``Qualifier`` s.

    TRAPI 2.0 writes a qualifier set constraint as a
    ``{qualifier_type_id: qualifier_value}`` mapping; an Edge carries its
    qualifiers as ``[{qualifier_type_id, qualifier_value}]``. This converts the
    former into the latter, e.g. to put a queried qualifier on an inferred edge.

    >>> qedge_qualifier_sets({"subject": "n0", "object": "n1", "constraints": {
    ...     "qualifiers": [{"biolink:object_direction_qualifier": "increased"}]}})
    [[{'qualifier_type_id': 'biolink:object_direction_qualifier', 'qualifier_value': 'increased'}]]
    >>> qedge_qualifier_sets({"subject": "n0", "object": "n1"})
    []
    """
    constraints = qedge.get("constraints") or {}
    return [
        [
            cast(
                "QualifierDict",
                {"qualifier_type_id": type_id, "qualifier_value": value},
            )
            for type_id, value in qualifier_set.items()
        ]
        for qualifier_set in constraints.get("qualifiers") or []
        if qualifier_set
    ]


#: Query-graph element properties that 2.0 gives a ``minItems`` of 1, so an
#: empty value has to be dropped rather than passed on (and echoed back).
_EMPTY_FORBIDDEN_QUERY_GRAPH_PROPERTIES = frozenset(
    {"ids", "categories", "member_ids", "constraints", "predicates"}
)
_EMPTY_FORBIDDEN_CONSTRAINTS_PROPERTIES = frozenset({"attributes", "qualifiers"})


def _strip_empty(element: dict[str, Any], empty_forbidden: frozenset) -> None:
    for key in [
        k for k, v in element.items() if v is None or (k in empty_forbidden and not v)
    ]:
        del element[key]


def normalize_query_graph(query_graph: Any) -> None:
    """Drop nulls, and empties 2.0 forbids, from a query graph in place.

    A client that sends ``"ids": null`` on an unpinned node would otherwise
    leave a present ``None`` that the ``qnode.get("ids", [])`` idiom used across
    the workers does not substitute, and every query graph is passed on to
    other services and echoed back in the Response, where both are invalid.

    >>> qg = {"nodes": {"n0": {"ids": None, "categories": []}},
    ...       "edges": {"e0": {"subject": "n0", "object": "n0", "constraints": {"qualifiers": []}}}}
    >>> normalize_query_graph(qg); qg
    {'nodes': {'n0': {}}, 'edges': {'e0': {'subject': 'n0', 'object': 'n0'}}}
    """
    if not isinstance(query_graph, dict):
        return
    for container_key in ("nodes", "edges", "paths"):
        container = query_graph.get(container_key)
        if container is None:
            query_graph.pop(container_key, None)
            continue
        if not isinstance(container, dict):
            continue
        for element in container.values():
            if not isinstance(element, dict):
                continue
            constraints = element.get("constraints")
            if isinstance(constraints, dict):
                _strip_empty(constraints, _EMPTY_FORBIDDEN_CONSTRAINTS_PROPERTIES)
            _strip_empty(element, _EMPTY_FORBIDDEN_QUERY_GRAPH_PROPERTIES)
            if isinstance(element.get("constraints"), list):
                for constraint in element["constraints"]:
                    if isinstance(constraint, dict):
                        _strip_empty(constraint, frozenset())


# ---------------------------------------------------------------------------
# Request validation
# ---------------------------------------------------------------------------

#: TRAPI 1.x request properties that 2.0 moved, mapped to their 2.0 spelling.
#: The 2.0 schema allows additional properties at every one of these levels, so
#: a stale client's value would otherwise be silently ignored -- and a client
#: that asked for a filter would get unfiltered results back.
_RETIRED_QUERY_FIELDS = {
    "log_level": "parameters.log_level",
    "bypass_cache": "parameters.bypass_cache",
}
_RETIRED_QEDGE_FIELDS = {
    "qualifier_constraints": "constraints.qualifiers",
    "attribute_constraints": "constraints.attributes",
}
_RETIRED_QPATH_CONSTRAINT_FIELDS = {
    "intermediate_categories": "required_intermediate_categories",
}


def _reject_retired_fields(query: Mapping[str, Any]) -> None:
    for retired, replacement in _RETIRED_QUERY_FIELDS.items():
        if retired in query:
            raise TRAPIRequestError(
                f"'{retired}' is a TRAPI 1.x query property; TRAPI 2.0 moved it "
                f"to '{replacement}'."
            )
    message = query.get("message")
    query_graph = message.get("query_graph") if isinstance(message, dict) else None
    if not isinstance(query_graph, dict):
        return
    for qedge_id, qedge in (query_graph.get("edges") or {}).items():
        if not isinstance(qedge, dict):
            continue
        for retired, replacement in _RETIRED_QEDGE_FIELDS.items():
            if retired in qedge:
                raise TRAPIRequestError(
                    f"Query edge '{qedge_id}' uses '{retired}', which TRAPI 2.0 "
                    f"replaced with '{replacement}'."
                )
    for qpath_id, qpath in (query_graph.get("paths") or {}).items():
        if not isinstance(qpath, dict):
            continue
        for constraint in qpath.get("constraints") or []:
            if not isinstance(constraint, dict):
                continue
            for retired, replacement in _RETIRED_QPATH_CONSTRAINT_FIELDS.items():
                if retired in constraint:
                    raise TRAPIRequestError(
                        f"Query path '{qpath_id}' uses '{retired}', which TRAPI "
                        f"2.0 replaced with '{replacement}'."
                    )


@cache
def _query_adapter() -> TypeAdapter[QueryDict]:
    """Validator over TOM's ``QueryDict``, built once on first use."""
    return TypeAdapter(QueryDict)


def _require_non_empty_query_graph(query: Mapping[str, Any]) -> None:
    """The ``minProperties`` rules TOM's lite validation doesn't check.

    A QueryGraph needs at least one node, and ``edges`` / ``paths`` (at
    least one of which it needs) each hold at least one entry when present.
    """
    message = query.get("message")
    query_graph = message.get("query_graph") if isinstance(message, dict) else None
    if query_graph is None:
        return
    if not query_graph.get("nodes"):
        raise TRAPIRequestError("Invalid TRAPI 2.0 query: query_graph has no nodes.")
    for key in ("edges", "paths"):
        if key in query_graph and not query_graph[key]:
            raise TRAPIRequestError(
                f"Invalid TRAPI 2.0 query: query_graph.{key} is empty; omit it "
                "or give at least one."
            )
    if not query_graph.get("edges") and not query_graph.get("paths"):
        raise TRAPIRequestError(
            "Invalid TRAPI 2.0 query: query_graph needs edges or paths."
        )


def validate_query(query: dict[str, Any]) -> None:
    """Reject a request body that is not a TRAPI 2.0 query.

    Runs TOM's lightweight TypedDict validation (much cheaper than building the
    models, and it leaves ``query`` untouched) over everything but
    ``workflow``, then rejects the 1.x spellings
    2.0 retired, naming the replacement. Finally drops nulls and forbidden
    empties from the query graph, which is passed on to other services and
    echoed back in the Response.

    Raises:
        TRAPIRequestError: with a client-safe description of the problem.
    """
    # Normalize first: the 1.x schema allowed nulls that 2.0 does not, and a
    # client sending ``"ids": null`` means "no ids", not a malformed query.
    message = query.get("message")
    if isinstance(message, dict):
        normalize_query_graph(message.get("query_graph"))
        for key in [k for k, v in message.items() if v is None]:
            del message[key]
    # ``workflow`` is left to the caller: Shepherd runs its own operations
    # (``aragorn.lookup``, ``score_paths``, ...) that the standard workflow
    # schema doesn't know, and checks them against what it supports itself.
    body = {k: v for k, v in query.items() if k != "workflow"}
    try:
        _query_adapter().validate_python(body)
    except ValidationError as e:
        first = e.errors()[0]
        location = ".".join(str(part) for part in first["loc"])
        raise TRAPIRequestError(
            f"Invalid TRAPI 2.0 query: {location}: {first['msg']}"
        ) from e
    _reject_retired_fields(query)
    _require_non_empty_query_graph(query)


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------


def is_trapi_1_response(response: Mapping[str, Any]) -> bool:
    """Whether a response from another service is still TRAPI 1.x shaped.

    Looks for the two changes every non-empty 1.x response exhibits: a list of
    node bindings per qnode, or an edge with neither top-level
    ``knowledge_level`` nor ``agent_type``.
    Only the first result/edge is inspected; a response is one version.

    >>> is_trapi_1_response({"message": {"results": [{"node_bindings": {"n0": [{"id": "X"}]}}]}})
    True
    >>> is_trapi_1_response({"message": {"results": [{"node_bindings": {"n0": {"ids": ["X"]}}}]}})
    False
    """
    message = response.get("message")
    if not isinstance(message, dict):
        return False
    for result in message.get("results") or []:
        for binding in (result.get("node_bindings") or {}).values():
            return isinstance(binding, list)
    edges = (message.get("knowledge_graph") or {}).get("edges") or {}
    for edge in edges.values():
        # Both missing: a 2.0 edge that merely lacks one is malformed, and
        # converting it would reset every edge's values to ``not_provided``.
        return "knowledge_level" not in edge and "agent_type" not in edge
    return False


def upgrade_trapi_1_response(response: dict[str, Any]) -> dict[str, Any]:
    """Return ``response`` in TRAPI 2.0 form, converting it if it is 1.x.

    Services Shepherd calls are moving to 2.0 on their own schedules, so a
    response posted back in the 1.x shape is converted with TOM's own
    1.6 -> 2.0 transforms instead of being mis-read (or crashing) downstream.
    A 2.0 response is returned as-is, without a walk over it.
    """
    if not is_trapi_1_response(response):
        return response
    return cast("dict[str, Any]", dict_up_version(response, V16Response))


def _prune_edge(edge: EdgeDict) -> None:
    """Keep one KG edge valid: no empty qualifiers or empty upstream lists."""
    if "qualifiers" in edge and not edge["qualifiers"]:
        del edge["qualifiers"]
    for source in edge.get("sources") or []:
        if "upstream_resource_ids" in source and not source["upstream_resource_ids"]:
            del source["upstream_resource_ids"]  # type: ignore[misc]
        if "source_record_urls" in source and not source["source_record_urls"]:
            del source["source_record_urls"]  # type: ignore[misc]
    if edge.get("attributes") is None:
        edge.pop("attributes", None)


def _prune_analysis(analysis: dict[str, Any]) -> None:
    for key in ("edge_bindings", "path_bindings", "support_graphs", "attributes"):
        if key in analysis and not analysis[key]:
            del analysis[key]
    for key in ("score", "scoring_method"):
        if key in analysis and analysis[key] is None:
            del analysis[key]
    for bindings_key in ("edge_bindings", "path_bindings"):
        bindings = analysis.get(bindings_key)
        if not bindings:
            continue
        for key in [k for k, b in bindings.items() if not binding_ids(b)]:
            del bindings[key]
        if not bindings:
            del analysis[bindings_key]


def prune_response(response: dict[str, Any]) -> None:
    """Apply TRAPI 2.0's no-null / no-forbidden-empty rules to a response.

    One pass over the results, knowledge graph edges and auxiliary graphs --
    linear, and cheap next to the serialization that follows it. Deliberately
    targeted rather than a recursive "strip falsy" walk: ``Message.results``
    SHOULD stay ``[]`` when a query found nothing, and ``KnowledgeGraph.nodes``
    is required even when empty.
    """
    for key in [k for k, v in response.items() if v is None]:
        del response[key]
    if "logs" in response and not response["logs"]:
        del response["logs"]
    message = response.get("message")
    if not isinstance(message, dict):
        return
    for key in [k for k, v in message.items() if v is None]:
        del message[key]
    if "auxiliary_graphs" in message and not message["auxiliary_graphs"]:
        del message["auxiliary_graphs"]
    for aux_graph in (message.get("auxiliary_graphs") or {}).values():
        aux_graph.pop("attributes", None)
    for result in message.get("results") or []:
        analyses = result.get("analyses")
        if analyses is not None:
            for analysis in analyses:
                _prune_analysis(analysis)
            analyses[:] = [
                a for a in analyses if a.get("edge_bindings") or a.get("path_bindings")
            ]
            if not analyses:
                del result["analyses"]
    knowledge_graph = message.get("knowledge_graph")
    if isinstance(knowledge_graph, dict):
        for edge in (knowledge_graph.get("edges") or {}).values():
            _prune_edge(edge)
    normalize_query_graph(message.get("query_graph"))


#: Query members that describe the request rather than the answer; they are
#: carried on the stored response (which starts life as a copy of the query)
#: but are not Response properties.
_QUERY_ONLY_MEMBERS = ("callback", "submitter", "log_level", "bypass_cache")


def finalize_response(
    response: dict[str, Any],
    query: Optional[Mapping[str, Any]] = None,
    logs: Optional[list[dict[str, Any]]] = None,
) -> ResponseDict:
    """Stamp the TRAPI 2.0 envelope onto a response leaving Shepherd, in place.

    - ``schema_version`` / ``biolink_version`` name what the response is.
    - ``parameters`` repeats the query's, which 2.0 says the server MUST do.
      Without a ``query``, a ``parameters`` already on the response is kept.
    - Query-only members (``callback``, ``submitter``) are dropped.
    - ``logs``, when given, replace any on the response; an empty list is
      omitted (``Response.logs`` has a ``minItems`` of 1).
    - ``prune_response`` drops nulls and forbidden empties.

    Key order is fixed so that serialized, the payload ends with the logs
    array when there are logs and with the message object when there are not
    -- ``finish_query`` appends late log entries by rewriting that tail.

    >>> r = finalize_response({"message": {"results": [], "auxiliary_graphs": {}}, "logs": []},
    ...                       {"parameters": {"log_level": "DEBUG"}})
    >>> r["schema_version"], r["parameters"], "logs" in r, r["message"]
    ('2.0.0', {'log_level': 'DEBUG'}, False, {'results': []})
    >>> list(finalize_response({"logs": [{"message": "x"}], "message": {}}))[-1]
    'logs'
    """
    for member in _QUERY_ONLY_MEMBERS:
        response.pop(member, None)
    existing_logs = response.pop("logs", None)
    message = response.pop("message", None)
    response["schema_version"] = SCHEMA_VERSION
    response["biolink_version"] = BIOLINK_VERSION
    if query is not None:
        parameters = query_parameters(query)
        if parameters:
            response["parameters"] = parameters
        else:
            response.pop("parameters", None)
    response["message"] = message if isinstance(message, dict) else {}
    final_logs = logs if logs is not None else existing_logs
    if final_logs:
        response["logs"] = final_logs
    prune_response(response)
    return cast("ResponseDict", response)


def empty_message() -> MessageDict:
    """A fresh, valid, empty 2.0 message."""
    return cast(
        "MessageDict",
        {"knowledge_graph": {"nodes": {}, "edges": {}}, "results": []},
    )
