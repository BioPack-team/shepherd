from __future__ import annotations
from typing import Any, Callable, Dict, Iterable, Optional, Union
import json
import networkx as nx


def trapi_kg_to_nx(
    trapi: Union[Dict[str, Any], str, bytes],
    *,
    multigraph: bool = True,
    directed: bool = True,
    default_weight: float = 1.0,
    edge_weight_attr: Optional[str] = None,
    edge_weight_transform: Optional[Callable[[Any], float]] = None,
    edge_payload: str = "full",  # "full" | "weight_only"
    weight_agg: Union[str, Callable[[float, float], float]] = "sum",  # collapse-only
) -> nx.Graph:
    """
    Convert a TRAPI ``knowledge_graph`` into a NetworkX graph that’s ready for downstream
    algorithms (e.g., Personalized PageRank). Supports directed/undirected graphs, multi-edge
    preservation, optional weight derivation, and (when collapsed) weight aggregation.

    Overview
    --------
    - **Input forms:** Accepts a full TRAPI Response/Message/Result or a raw KG dict.
      The KG is discovered at:
      ``response['message']['knowledge_graph']`` → ``message['knowledge_graph']`` → ``trapi['knowledge_graph']``.
    - **Graph type:** Controlled by ``directed`` and ``multigraph``:
      - ``multigraph=True`` → (Multi)DiGraph/(Multi)Graph with one edge per TRAPI edge.
      - ``multigraph=False`` → DiGraph/Graph where parallel TRAPI edges between the same (u, v)
        are either collapsed with metadata retained (“full”) or reduced to a single weighted edge
        (“weight_only”).
    - **Nodes:** Created for all TRAPI nodes and any edge endpoints not listed in the node map.
      Node metadata is preserved and an ``attributes_flat`` dict is added, where TRAPI attributes
      are keyed by ``original_attribute_name`` (fallback: ``attribute_type_id``); duplicate keys
      become lists.
    - **Edges & payload control (``edge_payload``):**
      - ``"full"``: keep TRAPI edge metadata (predicate, qualifiers, sources, attributes, etc.).
        In collapsed mode, the *last encountered* edge’s metadata wins for (u, v).
      - ``"weight_only"``: strip all edge metadata and keep only an optional ``weight``; in
        collapsed mode, multiple TRAPI edges between (u, v) are combined via ``weight_agg``.
      In multigraph mode, full metadata is always kept; ``edge_payload`` only affects collapsed graphs.
    - **Edge IDs (multigraph only):** Each multiedge gets an ``id`` attribute set to the TRAPI
      edge id (or a synthetic string if missing) and is used as the multiedge key.

    Weights
    -------
    - **Enable weights** by setting ``edge_weight_attr`` to the TRAPI attribute name you want to use
      (e.g., ``"normalized_google_distance"``). The value is looked up in the edge’s flattened
      attributes. If the attribute is missing or cannot be parsed as ``float``, ``default_weight``
      is used.
    - **Transform weights** by providing ``edge_weight_transform(value) -> float``; if this transform
      raises, the untransformed numeric value is used instead of ``default_weight``.
    - **Disable weights entirely** by passing ``edge_weight_attr=None``. No ``weight`` attribute will be
      set on any edge (including in collapsed graphs).
    - **Aggregation (collapsed + weight_only only):** ``weight_agg`` combines weights for multiple TRAPI
      edges between (u, v). Built-ins: ``"sum"``, ``"max"``, ``"min"``, ``"mean"``, ``"first"``, or
      a callable ``(existing_weight, new_weight) -> combined_weight``.
      *Note:* the built-in ``"mean"`` is a simple pairwise average; supply a custom aggregator if you
      need the exact arithmetic mean across many edges.

    Parameters
    ----------
    trapi : dict | str | bytes
        TRAPI Response/Message dict, or a JSON string/bytes containing one. The function extracts
        the ``knowledge_graph`` as described above.
    multigraph : bool, default True
        Preserve parallel TRAPI edges (recommended to retain provenance/predicate distinctions).
    directed : bool, default True
        Build a directed graph (TRAPI ``subject`` → ``object``). Set ``False`` for undirected.
    default_weight : float, default 1.0
        Fallback edge weight when weights are enabled but the specified attribute is missing or
        non-numeric. Ignored when ``edge_weight_attr=None``.
    edge_weight_attr : str | None, default None
        Name of the TRAPI edge attribute to use as the weight source (flattened lookup via
        ``original_attribute_name``/``attribute_type_id``). If ``None``, no weights are added.
    edge_weight_transform : callable(value) -> float | None, default None
        Optional transform applied to the numeric attribute value. On error, the raw numeric value
        is used (not ``default_weight``).
    edge_payload : {"full", "weight_only"}, default "full"
        Controls how much edge metadata is retained in collapsed graphs. Ignored for multigraphs.
    weight_agg : {"sum","max","min","mean","first"} | callable, default "sum"
        Only used when ``multigraph=False`` and ``edge_payload="weight_only"``. Aggregates multiple
        weights for the same (u, v). For a callable, provide ``f(existing, new) -> combined``.

    Returns
    -------
    networkx.(Multi)DiGraph or (Multi)Graph
        Graph whose node ids are TRAPI node CURIEs. Node data includes original node metadata and
        ``attributes_flat``. Edge data depends on the mode:
          • multigraph: full TRAPI edge metadata + optional ``weight``.
          • collapsed + "full": last-seen metadata for (u, v) + optional ``weight``.
          • collapsed + "weight_only": only ``weight`` (or no attributes if weights disabled).

    """
    # Check to see if `edge_payload` is `full`, if so, there should be no weight_agg
    if edge_payload not in ("full", "weight_only"):
        raise ValueError("edge_payload must be 'full' or 'weight_only'")
    if edge_payload == "full" and weight_agg != "sum":
        raise ValueError("weight_agg is only used when edge_payload is 'weight_only'")

    # if multigraph is True, then none of the aggregation stuff is relevant
    def _as_dict(obj: Union[Dict[str, Any], str, bytes]) -> Dict[str, Any]:
        return json.loads(obj) if isinstance(obj, (str, bytes)) else obj

    def _get_kg(root: Dict[str, Any]) -> Dict[str, Any]:
        if (
            "message" in root
            and isinstance(root["message"], dict)
            and "knowledge_graph" in root["message"]
        ):
            return root["message"]["knowledge_graph"]
        if "knowledge_graph" in root:
            return root["knowledge_graph"]
        raise KeyError("No TRAPI knowledge_graph found in input.")

    def _flatten_attributes(
        attrs: Optional[Iterable[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        flat: Dict[str, Any] = {}
        if not attrs:
            return flat
        for a in attrs:
            key = (
                a.get("original_attribute_name")
                or a.get("attribute_type_id")
                or f"attr_{len(flat)}"
            )
            val = a.get("value")
            if key in flat:
                flat[key] = (
                    flat[key] + [val]
                    if isinstance(flat[key], list)
                    else [flat[key], val]
                )
            else:
                flat[key] = val
        return flat

    def _derive_weight(edge_attr_flat: Dict[str, Any]) -> Optional[float]:
        # NEW: if weights are disabled, return None
        if edge_weight_attr is None:
            return None
        val = edge_attr_flat.get(edge_weight_attr)
        if val is None:
            return float(default_weight)
        try:
            raw = val[0] if isinstance(val, list) else val
            num = float(raw)
        except Exception:
            return float(default_weight)
        if edge_weight_transform:
            try:
                return float(edge_weight_transform(num))
            except Exception:  # if you can't transform it, just return the raw
                return float(num)
        return num

    def _agg_fn():
        if callable(weight_agg):
            return weight_agg
        return {
            "sum": lambda a, b: a + b,
            "max": lambda a, b: a if a >= b else b,
            "min": lambda a, b: a if a <= b else b,
            "mean": lambda a, b: 0.5 * (a + b),
            "first": lambda a, b: a,
        }.get(weight_agg, lambda a, b: a + b)

    data = _as_dict(trapi)
    kg = _get_kg(data)
    weights_enabled = edge_weight_attr is not None

    # Choose graph class
    if directed and multigraph:
        G: nx.Graph = nx.MultiDiGraph()
    elif directed and not multigraph:
        G = nx.DiGraph()
    elif not directed and multigraph:
        G = nx.MultiGraph()
    else:
        G = nx.Graph()

    nodes = kg.get("nodes", {}) or {}
    edges = kg.get("edges", {}) or {}

    # Nodes
    for nid, nobj in nodes.items():
        n_attrs = dict(nobj)
        n_attrs["attributes_flat"] = _flatten_attributes(nobj.get("attributes"))
        G.add_node(nid, **n_attrs)

    combine = _agg_fn()

    # Edges
    for eid, eobj in edges.items() if isinstance(edges, dict) else enumerate(edges):
        u = eobj.get("subject")
        v = eobj.get("object")
        if u is None or v is None:
            continue
        if u not in G:
            G.add_node(u)
        if v not in G:
            G.add_node(v)

        e_attr_flat = _flatten_attributes(eobj.get("attributes"))
        w = _derive_weight(e_attr_flat)  # None if weights disabled

        if isinstance(G, (nx.MultiGraph, nx.MultiDiGraph)):
            # Keep edge metadata in multigraph mode; add weight only if enabled
            e_attrs = dict(eobj)
            e_attrs["attributes_flat"] = e_attr_flat
            e_attrs["id"] = eid if isinstance(eid, str) else eobj.get("id", str(eid))
            if weights_enabled and w is not None:
                e_attrs["weight"] = w
            G.add_edge(u, v, key=e_attrs["id"], **e_attrs)
        else:
            # Collapsed (simple) graph
            if edge_payload == "weight_only":
                if not weights_enabled or w is None:
                    # weights disabled → bare edge, no attributes
                    G.add_edge(u, v)
                else:
                    # weights enabled → aggregate weight for (u,v)
                    if G.has_edge(u, v):
                        G[u][v]["weight"] = combine(G[u][v].get("weight", w), w)
                    else:
                        G.add_edge(u, v, weight=w)
            else:
                # edge_payload == "full": keep metadata but only add 'weight' if enabled
                e_attrs = dict(eobj)
                e_attrs["attributes_flat"] = e_attr_flat
                e_attrs["id"] = (
                    eid if isinstance(eid, str) else eobj.get("id", str(eid))
                )
                if weights_enabled and w is not None:
                    e_attrs["weight"] = w
                G.add_edge(u, v, **e_attrs)

    return G
