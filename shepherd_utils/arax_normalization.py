"""Whether the ARAX port calls the Node Normalizer, per query.

Every Node Normalizer call the ARAX port makes goes through
``NodeSynonymizer._call_normalizer_api``. Its answer for each CURIE depends on
the query's mode:

- ``on``: it asks the Node Normalizer, as upstream ARAX does.
- ``off``: it asks nothing and treats every CURIE as already normalized: the
  CURIE is its own preferred id, with no other equivalent ids, and its name and
  categories are the ones a KP (Retriever) gave the node in this query
  (``remember_kp_nodes``), or none for a CURIE no KP has returned.
- ``compare``: it asks the Node Normalizer and uses its answer, as ``on``
  does, and also records, per call site, every CURIE where ``off``'s answer
  would have been different (``report``).
- ``query_nodes``: ``off`` for lookups of knowledge-graph nodes
  (``KG_NODE_SITES``: NGD, FET's neighbor counts, add_node_pmids, COHD), ``on``
  for every other lookup -- the query's own ids and names, a few CURIEs each,
  where the Node Normalizer's category decides what ARAX does (FET's category
  for a query node without one, the query graph interpreter).

The mode is ``parameters.arax_node_normalization`` when the query sets it to
one of those, else ``settings.arax_node_normalization``. It is set for the
duration of a query by the ``arax`` worker (``query_mode``); outside a query
(the server's ``/entity`` lookups, for example) it is ``on``.

A pool child runs one query at a time, so the state here is per process.
"""

import contextlib
import re
import sys
from collections import defaultdict
from typing import Iterator, Optional

from shepherd_utils.config import settings
from shepherd_utils.trapi import query_parameters

MODES = ("on", "off", "compare", "query_nodes")

# The functions that look up knowledge-graph nodes (thousands per query) and use
# only each one's preferred id. query_nodes mode answers them as off does; any
# other caller asks the Node Normalizer.
KG_NODE_SITES = frozenset(
    {
        "Overlay/compute_ngd.py:_get_canonical_curies_map",
        "Overlay/fisher_exact_test.py:query_size_of_adjacent_nodes",
        "KnowledgeSources/COHD_local/scripts/COHDIndex.py:_get_preferred_curie",
    }
)
PARAMETER = "arax_node_normalization"
LOG_CODE = "NodeNormalizationComparison"
MAX_EXAMPLES = 5

# What a difference is, for ``report``:
# - id: the Node Normalizer's preferred id is another CURIE
# - unknown: the Node Normalizer doesn't know the CURIE
# - category: the preferred (most specific) category differs
# - no_category: off mode has no category for the CURIE (no KP returned it)
# - name: the name differs
# - equivalents: the Node Normalizer knows other equivalent ids (only matters
#   where a caller uses them, e.g. Expand's synonym lookups)
KINDS = ("id", "unknown", "category", "no_category", "name", "equivalents")

# A CURIE: a prefix, a colon, a local id without spaces. ARAX sends names
# ("metformin") through the same lookup as CURIEs (get_canonical_curies(curies=
# [name], names=[name])); the Node Normalizer doesn't know them, and the name
# goes on to the Name Resolver.
_CURIE = re.compile(r"[A-Za-z][\w.\-]*:\S+")

_mode = "on"
_kp_nodes: dict[str, dict] = {}
_sites: dict[str, dict] = {}


def resolve_mode(query: Optional[dict]) -> str:
    """The query's mode: its parameter when valid, else the setting."""
    requested = query_parameters(query or {}).get(PARAMETER)
    if requested in MODES:
        return requested
    configured = str(settings.arax_node_normalization).lower()
    return configured if configured in MODES else "on"


def mode() -> str:
    return _mode


@contextlib.contextmanager
def query_mode(query: Optional[dict]) -> Iterator[str]:
    """Run one query in its mode, with fresh KP-node and comparison state."""
    global _mode
    reset()
    _mode = resolve_mode(query)
    try:
        yield _mode
    finally:
        _mode = "on"


def reset() -> None:
    _kp_nodes.clear()
    _sites.clear()


def remember_kp_nodes(nodes: dict) -> None:
    """Keep the name and categories a KP gave each node (CURIE -> Node).

    Off mode answers with these; compare mode compares with them.
    """
    if _mode == "on":
        return
    for curie, node in nodes.items():
        if not curie:
            continue
        known = _kp_nodes.setdefault(curie, {"name": None, "categories": []})
        name = getattr(node, "name", None)
        if name and not known["name"]:
            known["name"] = name
        for category in getattr(node, "categories", None) or []:
            if category not in known["categories"]:
                known["categories"].append(category)


def remember_name_resolver_hit(candidate: dict) -> None:
    """Keep the label and types the Name Resolver gives the CURIE it picked for
    a name (its clique's preferred name and categories, from the same
    normalized data): off mode answers with them, so ARAX's check that the
    name matches the CURIE's name works as with the Node Normalizer.
    """
    if _mode == "on" or not candidate.get("curie"):
        return
    known = _kp_nodes.setdefault(candidate["curie"], {"name": None, "categories": []})
    known["name"] = known["name"] or candidate.get("label")
    for category in candidate.get("types") or []:
        if category not in known["categories"]:
            known["categories"].append(category)


def assumed_normalized(curie: str, category_levels: dict) -> Optional[dict]:
    """What the Node Normalizer would answer if ``curie`` were normalized
    (None for a string that isn't a CURIE, as the Node Normalizer answers).

    The answer has the Node Normalizer's shape, so every caller handles it as
    it handles a real one. ``type`` is most specific first, as the Node
    Normalizer's is (``category_levels``: depth in the Biolink hierarchy,
    without the ``biolink:`` prefix).
    """
    if not _CURIE.fullmatch(curie):
        return None
    known = _kp_nodes.get(curie) or {}
    categories = sorted(
        known.get("categories") or [],
        key=lambda c: -category_levels.get(c.replace("biolink:", ""), -1),
    )
    name = known.get("name")
    return {
        "id": {"identifier": curie, "label": name},
        "equivalent_identifiers": [{"identifier": curie, "label": name}],
        "type": categories,
    }


def call_site() -> str:
    """The ARAX code that asked: the first two frames outside the
    synonymizer, e.g. ``Overlay/fisher_exact_test.py:query_size_of_adjacent_nodes``.
    """
    frames = []
    frame = sys._getframe(1)
    while frame is not None and len(frames) < 2:
        path = frame.f_code.co_filename.replace("\\", "/")
        if not path.endswith(
            ("/NodeSynonymizer/node_synonymizer.py", "/arax_normalization.py")
        ):
            short = path.split("/shepherd_utils/arax/")[-1]
            frames.append(f"{short}:{frame.f_code.co_name}")
        frame = frame.f_back
    return " <- ".join(frames) or "unknown"


def skips_node_normalizer(site: str) -> bool:
    """Whether this query answers a lookup from ``site`` without the Node
    Normalizer."""
    if _mode == "off":
        return True
    return _mode == "query_nodes" and site.split(" <- ")[0] in KG_NODE_SITES


def count(site: str, curies: list, asked: bool = False) -> None:
    """Off / query_nodes mode: count the lookups, and whether each call site
    asked the Node Normalizer (``asked``) or answered without it."""
    entry = _sites.setdefault(site, {"calls": 0, "curies": 0, "asked": asked})
    entry["calls"] += 1
    entry["curies"] += len(curies)


def _differences(
    curie: str, real: Optional[dict], assumed: Optional[dict]
) -> list[str]:
    if real is None:
        return [] if assumed is None else ["unknown"]
    if assumed is None:
        return ["id"]
    kinds = []
    real_id = (real.get("id") or {}).get("identifier")
    if real_id != curie:
        kinds.append("id")
    real_types = real.get("type") or []
    if real_types:
        if not assumed["type"]:
            kinds.append("no_category")
        elif real_types[0] != assumed["type"][0]:
            kinds.append("category")
    real_name = (real.get("id") or {}).get("label")
    if real_name and real_name != assumed["id"]["label"]:
        kinds.append("name")
    real_equivalents = {
        e.get("identifier") for e in real.get("equivalent_identifiers") or []
    }
    if real_equivalents - {curie}:
        kinds.append("equivalents")
    return kinds


def record(site: str, curies: list, real: dict, category_levels: dict) -> None:
    """Compare mode: record where off mode would have answered differently."""
    entry = _sites.setdefault(
        site,
        {
            "calls": 0,
            "curies": 0,
            **{kind: 0 for kind in KINDS},
            "examples": defaultdict(list),
        },
    )
    entry["calls"] += 1
    for curie in curies:
        entry["curies"] += 1
        assumed = assumed_normalized(curie, category_levels)
        found = real.get(curie)
        for kind in _differences(curie, found, assumed):
            entry[kind] += 1
            if len(entry["examples"][kind]) < MAX_EXAMPLES:
                entry["examples"][kind].append(_example(curie, found, assumed))


def _example(curie: str, real: Optional[dict], assumed: Optional[dict]) -> dict:
    real = real or {}
    assumed = assumed or {"id": {}, "type": []}
    return {
        "curie": curie,
        "normalized": (real.get("id") or {}).get("identifier"),
        "normalized_category": (real.get("type") or [None])[0],
        "assumed_category": (assumed["type"] or [None])[0],
        "normalized_name": (real.get("id") or {}).get("label"),
        "assumed_name": assumed["id"].get("label"),
    }


def report() -> Optional[dict]:
    """This query's lookups, or None when it made none.

    ``totals`` sums the call sites; ``sites`` has each one's counts and, in
    compare mode, a few examples of each kind of difference.
    """
    if not _sites:
        return None
    sites = {}
    for site, entry in sorted(_sites.items()):
        sites[site] = dict(entry)
        if "examples" in entry:
            sites[site]["examples"] = dict(entry["examples"])
    if _mode == "compare":
        keys = ("calls", "curies", *KINDS)
        totals = {k: sum(entry.get(k, 0) for entry in _sites.values()) for k in keys}
    else:
        # calls / curies: answered without the Node Normalizer; asked_*: by it
        totals = {"calls": 0, "curies": 0, "asked_calls": 0, "asked_curies": 0}
        for entry in _sites.values():
            prefix = "asked_" if entry.get("asked") else ""
            totals[f"{prefix}calls"] += entry["calls"]
            totals[f"{prefix}curies"] += entry["curies"]
    return {"mode": _mode, "totals": totals, "sites": sites}
