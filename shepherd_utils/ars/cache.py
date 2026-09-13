"""ARS whole-response cache.

Shepherd-native (the upstream ARS has none); design in
docs/ARS_RESPONSE_CACHE_PLAN.md. There is exactly one message tree per
distinct query per cache generation: the first submit to see a query
becomes its *leader* and runs it; every later structurally identical submit
is handed the leader's pk. A hit therefore stores nothing and transfers
nothing but the envelope -- the 201 is the source parent's row (already
Done, ``merged_version`` set) with the caller's own submit body as its
``data`` -- and the client fetches the tree exactly as it would its own.

The cache index (``ars_response_cache``) maps a canonical query-graph hash
to the source parent's pk; the payloads are the ones already kept in
``ars_message.data``. Source trees backing a live generation are exempt
from the payload retention purge.

Two request-side knobs: TRAPI ``bypass_cache`` (no read, no write) and
``parameters.overwrite_cache`` (no read, forced write). Whole-cache
invalidation bumps a generation counter; old pks keep working.

Canonicalization is renaming-invariant: node / edge / path ids are labels.
The served tree keeps the source's labels (its bindings agree with its own
query graph); the index records the source's label map for inspection.
"""

import datetime
import hashlib
import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple

import orjson

from shepherd_utils.config import settings

import shepherd_utils.db as shepherd_db

from . import db as ars_db
from .completion import MERGE_AGENT_NAME

# Baked into every key: bump when any canonicalization rule changes, which
# orphans (and lazily purges) every existing entry without an explicit
# invalidation.
CACHE_KEY_VERSION = "1"

MODE_NORMAL = "normal"
MODE_BYPASS = "bypass"
MODE_OVERWRITE = "overwrite"

ROLE_LEADER = "leader"
ROLE_OVERWRITE = "overwrite"
ROLE_BYPASS = "bypass"
# Cache bookkeeping failed for this submit; it ran as an ordinary query.
ROLE_UNCACHED = "uncached"

# Outcomes of the submit-side steps
DISPATCH = "dispatch"  # fan out as usual (this parent leads, or bypass/overwrite)
SERVED = "served"  # answered from a ready entry: the source parent's pk
WAITING = "waiting"  # coalesced onto a pending leader: the leader's pk, still Running

MERGE_INFORESID = "infores:ars"

# Beyond this many candidate labelings of tied nodes, fall back to a
# deterministic (but not renaming-invariant) tie-break. Only a *miss* can
# result, never a wrong hit.
MAX_TIE_PERMUTATIONS = 5040

_ENDPOINT_KEYS = ("subject", "object")


# ---------------------------------------------------------------------------
# Canonicalization
# ---------------------------------------------------------------------------


def _dumps(value: Any) -> str:
    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode()


def _is_empty(value: Any) -> bool:
    return value is None or value == {} or value == []


def canonicalize(value: Any) -> Any:
    """Value-level canonical form.

    Drops object members whose value is null / {} / [] (recursively, so a
    member that *becomes* empty is dropped too), sorts object keys, and
    treats every list as a set -- elements canonicalized then sorted by
    their serialization. Empty strings, booleans and numbers are kept as-is.
    Returns None for a value that canonicalizes to nothing.
    """
    if isinstance(value, dict):
        out = {}
        for key in sorted(value, key=str):
            item = canonicalize(value[key])
            if not _is_empty(item):
                out[str(key)] = item
        return out or None
    if isinstance(value, list):
        items = [canonicalize(v) for v in value]
        items = [v for v in items if not _is_empty(v)]
        items.sort(key=_dumps)
        return items or None
    return value


def _body_without_endpoints(item: Any) -> Any:
    if not isinstance(item, dict):
        return canonicalize(item)
    return canonicalize({k: v for k, v in item.items() if k not in _ENDPOINT_KEYS})


def _refine_colors(
    nodes: Dict[str, Any],
    incidence: Dict[str, List[Tuple[str, str, Optional[str]]]],
) -> Dict[str, str]:
    """Weisfeiler-Lehman style color refinement over node bodies + incident
    edge/path bodies + direction + neighbor color, iterated to a fixpoint."""
    color = {nid: _sha(_dumps(nodes[nid])) for nid in nodes}
    for _ in range(len(nodes) + 1):
        new = {}
        for nid in nodes:
            neigh = sorted(
                _dumps([entry, direction, color.get(other)])
                for entry, direction, other in incidence[nid]
            )
            new[nid] = _sha(color[nid] + "|" + "|".join(neigh))
        stable = len(set(new.values())) == len(set(color.values()))
        color = new
        if stable:
            break
    return color


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _relabel_collection(
    collection: Dict[str, Any], node_map: Dict[str, str]
) -> List[Tuple[str, str, Any]]:
    """[(serialization, original id, relabeled canonical body)] sorted."""
    out = []
    for cid, item in collection.items():
        if isinstance(item, dict):
            body = dict(item)
            for key in _ENDPOINT_KEYS:
                if key in body and body[key] in node_map:
                    body[key] = node_map[body[key]]
            body = canonicalize(body)
        else:
            body = canonicalize(item)
        out.append((_dumps(body), str(cid), body))
    out.sort(key=lambda t: t[0])
    return out


def _serialize_candidate(
    nodes: Dict[str, Any],
    edges: Dict[str, Any],
    paths: Dict[str, Any],
    order: List[str],
) -> Tuple[str, Dict[str, Any], Dict[str, Dict[str, str]]]:
    node_map = {nid: f"n{i}" for i, nid in enumerate(order)}
    canon_nodes = {node_map[nid]: nodes[nid] for nid in order}
    edge_items = _relabel_collection(edges, node_map)
    path_items = _relabel_collection(paths, node_map)
    canon_edges = {f"e{i}": body for i, (_, _, body) in enumerate(edge_items)}
    canon_paths = {f"p{i}": body for i, (_, _, body) in enumerate(path_items)}
    graph = canonicalize(
        {"nodes": canon_nodes, "edges": canon_edges, "paths": canon_paths}
    ) or {}
    label_map = {
        "nodes": node_map,
        "edges": {orig: f"e{i}" for i, (_, orig, _) in enumerate(edge_items)},
        "paths": {orig: f"p{i}" for i, (_, orig, _) in enumerate(path_items)},
    }
    return _dumps(graph), graph, label_map


def canonical_graph(query_graph: Any) -> Tuple[Any, Dict[str, Dict[str, str]]]:
    """Structural canonical form of a TRAPI query graph plus the label map.

    Node, edge and path ids are treated as labels: two graphs that differ
    only in those names canonicalize identically. Returns
    ``(canonical_graph, {"nodes": {orig: canon}, "edges": {...}, "paths": {...}})``.
    Anything that is not a dict-of-dicts query graph falls back to plain
    value canonicalization with an empty label map.
    """
    if not isinstance(query_graph, dict):
        return canonicalize(query_graph), {"nodes": {}, "edges": {}, "paths": {}}
    raw_nodes = query_graph.get("nodes")
    raw_edges = query_graph.get("edges")
    raw_paths = query_graph.get("paths")
    if not isinstance(raw_nodes, dict):
        return canonicalize(query_graph), {"nodes": {}, "edges": {}, "paths": {}}
    edges = raw_edges if isinstance(raw_edges, dict) else {}
    paths = raw_paths if isinstance(raw_paths, dict) else {}
    nodes = {str(nid): (canonicalize(n) or {}) for nid, n in raw_nodes.items()}

    incidence: Dict[str, List[Tuple[str, str, Optional[str]]]] = {
        nid: [] for nid in nodes
    }
    for kind, collection in (("edge", edges), ("path", paths)):
        for item in collection.values():
            if not isinstance(item, dict):
                continue
            entry = f"{kind}:{_dumps(_body_without_endpoints(item))}"
            subj = item.get("subject")
            obj = item.get("object")
            subj = str(subj) if subj is not None else None
            obj = str(obj) if obj is not None else None
            if subj in incidence:
                incidence[subj].append((entry, "out", obj))
            if obj in incidence:
                incidence[obj].append((entry, "in", subj))

    color = _refine_colors(nodes, incidence)
    groups: Dict[str, List[str]] = {}
    for nid in nodes:
        groups.setdefault(color[nid], []).append(nid)
    ordered_groups = [sorted(groups[c]) for c in sorted(groups)]

    total = 1
    for g in ordered_groups:
        for k in range(2, len(g) + 1):
            total *= k
    if total > MAX_TIE_PERMUTATIONS:
        # deterministic but label-dependent tie-break: can only cause a miss
        order = [nid for g in ordered_groups for nid in g]
        _, graph, label_map = _serialize_candidate(nodes, edges, paths, order)
        return graph, label_map

    best: Optional[Tuple[str, Any, Dict[str, Dict[str, str]]]] = None
    for combo in itertools.product(
        *(itertools.permutations(g) for g in ordered_groups)
    ):
        order = [nid for g in combo for nid in g]
        candidate = _serialize_candidate(nodes, edges, paths, order)
        if best is None or candidate[0] < best[0]:
            best = candidate
    assert best is not None
    return best[1], best[2]


def key_material(body: Dict[str, Any]) -> Tuple[Any, Dict[str, Dict[str, str]]]:
    """What the key hashes: the canonical query graph plus a non-empty
    workflow. Returns it with the caller's label map."""
    message = body.get("message") if isinstance(body, dict) else None
    query_graph = message.get("query_graph") if isinstance(message, dict) else None
    graph, label_map = canonical_graph(query_graph)
    material: Dict[str, Any] = {"query_graph": graph}
    workflow = body.get("workflow") if isinstance(body, dict) else None
    if isinstance(workflow, list) and workflow:
        # workflow order is meaningful upstream; keep it, canonicalize values
        material["workflow"] = [canonicalize(step) for step in workflow]
    return material, label_map


def cache_key(body: Dict[str, Any]) -> Tuple[str, Dict[str, Dict[str, str]]]:
    material, label_map = key_material(body)
    digest = hashlib.sha256(
        (CACHE_KEY_VERSION + "\n" + _dumps(material)).encode()
    ).hexdigest()
    return digest, label_map


def resolve_mode(body: Any) -> str:
    if not isinstance(body, dict):
        return MODE_NORMAL
    if body.get("bypass_cache") is True:
        return MODE_BYPASS
    parameters = body.get("parameters")
    if isinstance(parameters, dict) and parameters.get("overwrite_cache") is True:
        return MODE_OVERWRITE
    return MODE_NORMAL


def append_log(payload: Any, message: str) -> Any:
    """Append a TRAPI log entry to a payload's top-level ``logs`` (created or
    replaced when missing / malformed). Non-dict payloads pass through."""
    if not isinstance(payload, dict):
        return payload
    logs = payload.get("logs")
    if not isinstance(logs, list):
        logs = []
    logs.append(
        {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "level": "INFO",
            "code": None,
            "message": message,
        }
    )
    payload["logs"] = logs
    return payload


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _cache_params(parent_row: Dict[str, Any], **cache_fields) -> Dict[str, Any]:
    params = dict(parent_row.get("params") or {})
    params["cache"] = cache_fields
    return params


async def _set_role(parent_row, **cache_fields) -> Dict[str, Any]:
    params = _cache_params(parent_row, **cache_fields)
    updated = await ars_db.update_message(parent_row["id"], params=params)
    return updated or dict(parent_row, params=params)


class _BrokenEntry(Exception):
    """The source tree behind a ready entry is unusable."""


async def _serve_ready(
    entry: Dict[str, Any], logger: logging.Logger
) -> Optional[Dict[str, Any]]:
    """The source parent row behind a ready entry, after checking the tree
    can actually be read (Done, merged_version set, merged payload present
    in Redis or Postgres). None (and the entry dropped) otherwise."""
    source_pk = entry["source_pk"]
    try:
        source = await ars_db.get_message_row(source_pk)
        if source is None or source.get("status") != "D":
            raise _BrokenEntry(f"source {source_pk} missing or not Done")
        merged_pk = source.get("merged_version")
        if merged_pk is None:
            raise _BrokenEntry(f"source {source_pk} has no merged_version")
        if not await ars_db.message_has_data(merged_pk):
            raise _BrokenEntry(f"merged message {merged_pk} has no payload")
    except _BrokenEntry as e:
        logger.warning(f"Cache entry {entry.get('cache_key')} unusable, dropping: {e}")
        try:
            await ars_db.delete_cache_entry(entry["generation"], entry["cache_key"])
        except Exception as de:
            logger.error(f"Failed to drop broken cache entry: {de}")
        return None
    return source


async def _serve(
    entry: Optional[Dict[str, Any]], body: Any, logger: logging.Logger
) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    """Answer from an existing entry: (SERVED, source row, body) for a ready
    one, (WAITING, leader row, body) for a pending one, None when there is
    nothing usable to answer with. ``body`` -- the caller's own submit --
    rides along as the envelope's ``data``, exactly as for a fresh parent."""
    if entry is None:
        return None
    if entry["state"] == "ready":
        source = await _serve_ready(entry, logger)
        if source is None:
            return None
        try:
            await ars_db.record_cache_hit(entry["generation"], entry["cache_key"])
        except Exception as e:  # bookkeeping only
            logger.debug(f"Cache hit count update failed: {e}")
        logger.info(f"Cache hit: serving {source['id']} for key {entry['cache_key']}")
        return SERVED, source, body
    leader = await ars_db.get_message_row(entry["source_pk"])
    if leader is None:
        logger.warning(
            f"Cache: pending entry {entry.get('cache_key')} has no leader row; dropping"
        )
        await ars_db.delete_cache_entry(entry["generation"], entry["cache_key"])
        return None
    logger.info(f"Cache: coalescing onto pending leader {leader['id']}")
    return WAITING, leader, body


async def lookup(body: Any, logger: logging.Logger) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    """Submit step 1, before any row exists: answer a normal-mode submit
    from the cache when it can. None means "create a parent row and call
    claim_or_serve". Never raises."""
    if not settings.ars_cache_enabled or resolve_mode(body) != MODE_NORMAL:
        return None
    try:
        key, _ = cache_key(body)
        generation = await ars_db.get_cache_generation()
        entry = await ars_db.get_cache_entry(generation, key)
        return await _serve(entry, body, logger)
    except Exception as e:
        logger.error(f"Cache lookup failed: {e}", exc_info=True)
        return None


async def _discard_parent(parent_row: Dict[str, Any], logger: logging.Logger) -> None:
    """A freshly created parent that lost the leadership race is not
    needed: the caller gets the winner's pk instead."""
    pk = parent_row["id"]
    try:
        await ars_db.delete_message(pk)
    except Exception as e:
        logger.warning(f"Cache: could not delete orphan parent {pk}: {e}")
    try:
        await shepherd_db.data_db_client.delete(str(pk))
    except Exception:
        pass


async def claim_or_serve(
    parent_row: Dict[str, Any], body: Any, logger: logging.Logger
) -> Tuple[str, Dict[str, Any], Any]:
    """Submit step 2, once the parent row and its query blob exist.

    Records the request's cache role on the row and, in normal mode, claims
    leadership of the key. Returns ``(DISPATCH, row, body)`` when this
    parent should fan out, or the SERVED / WAITING answer from a concurrent
    winner -- in which case this parent row has been discarded. Any failure
    degrades to DISPATCH: the cache never makes a submit fail.
    """
    if not settings.ars_cache_enabled:
        return DISPATCH, parent_row, body
    parent_pk = parent_row["id"]
    mode = resolve_mode(body)
    try:
        key, _ = cache_key(body)
        generation = await ars_db.get_cache_generation()
        base = {"key": key, "generation": generation}
        if mode == MODE_BYPASS:
            return DISPATCH, await _set_role(parent_row, role=ROLE_BYPASS, **base), body
        if mode == MODE_OVERWRITE:
            return DISPATCH, await _set_role(parent_row, role=ROLE_OVERWRITE, **base), body
        for _ in range(2):
            entry, claimed = await ars_db.claim_or_get_cache_entry(
                generation, key, parent_pk
            )
            if claimed:
                return DISPATCH, await _set_role(parent_row, role=ROLE_LEADER, **base), body
            served = await _serve(entry, body, logger)
            if served is not None:
                await _discard_parent(parent_row, logger)
                return served
            # the entry was broken/vanished and has been dropped: claim again
        return DISPATCH, await _set_role(parent_row, role=ROLE_UNCACHED, **base), body
    except Exception as e:
        logger.error(f"Cache bookkeeping failed for {parent_pk}: {e}", exc_info=True)
        return DISPATCH, parent_row, body


async def annotate_cached_read(
    row: Dict[str, Any], actor: Dict[str, Any], payload: Any, logger: logging.Logger
) -> Any:
    """GET-time note for readers of a cached tree: when ``row`` is the final
    merged message of a cache source, append a log line saying so. The
    stored bytes are never touched."""
    if not settings.ars_cache_enabled or not isinstance(payload, dict):
        return payload
    if row.get("ref") is None or actor.get("inforesid") != MERGE_INFORESID:
        return payload
    try:
        entry = await ars_db.get_ready_cache_entry_by_source(row["ref"])
        if entry is None:
            return payload
        parent = await ars_db.get_message_row(row["ref"])
        if parent is None or str(parent.get("merged_version")) != str(row["id"]):
            return payload
        cached = entry.get("ready_at")
        cached = cached.isoformat() if hasattr(cached, "isoformat") else str(cached)
        append_log(
            payload,
            f"Served from ARS response cache: this message is the cached answer "
            f"for its query (source {row['ref']}, generation "
            f"{entry.get('generation')}, cached {cached}, served "
            f"{entry.get('hit_count', 0)} time(s))",
        )
    except Exception as e:
        logger.debug(f"Cache read annotation skipped: {e}")
    return payload


async def _cacheable(parent_row: Dict[str, Any], children, logger) -> bool:
    """Q5: never cache "nothing, because everything failed"; optionally
    require every ARA child to have finished cleanly."""
    ara_children = [
        c
        for c in children
        if c.get("agent_name") != MERGE_AGENT_NAME
        and c.get("inforesid") != MERGE_INFORESID
    ]
    errored = any(c.get("status") == "E" for c in ara_children)
    if not settings.ars_cache_store_partial and errored:
        return False
    merged_pk = parent_row.get("merged_version")
    merged = next((c for c in children if str(c["id"]) == str(merged_pk)), None)
    empty = merged is None or not (merged.get("result_count") or 0)
    if empty and errored:
        logger.info(
            f"Cache: not caching {parent_row['id']}: empty result with errored children"
        )
        return False
    return True


async def on_parent_complete(parent_row: Dict[str, Any], logger: logging.Logger) -> None:
    """Completion hook, once a parent reaches Done: a leader flips its
    pending entry to ready (or drops it when the run is not cacheable, so
    the next identical submit re-runs); an overwrite run repoints its key.
    Anything else is a no-op."""
    cache_info = (parent_row.get("params") or {}).get("cache") or {}
    role = cache_info.get("role")
    if role not in (ROLE_LEADER, ROLE_OVERWRITE):
        return
    parent_pk = parent_row["id"]
    try:
        children = await ars_db.get_children(parent_pk)
        cacheable = await _cacheable(parent_row, children, logger)
        if not cacheable:
            if role == ROLE_LEADER:
                await ars_db.delete_pending_cache_entry(parent_pk)
            else:
                logger.info(f"Cache: overwrite run {parent_pk} not cacheable")
            return
        query = await ars_db.load_message_data(parent_pk, logger)
        _, label_map = cache_key(query if isinstance(query, dict) else {})
        if role == ROLE_LEADER:
            entry = await ars_db.mark_cache_entry_ready(parent_pk, label_map)
            if entry is None:
                logger.info(f"Cache: {parent_pk} finished but no longer leads its entry")
            else:
                logger.info(f"Cache: entry {entry['cache_key']} ready from {parent_pk}")
        else:
            await ars_db.upsert_cache_entry_ready(
                cache_info.get("generation"), cache_info.get("key"), parent_pk, label_map
            )
            logger.info(f"Cache: entry {cache_info.get('key')} overwritten by {parent_pk}")
    except Exception as e:
        logger.error(f"Cache completion hook failed for {parent_pk}: {e}", exc_info=True)


async def on_parent_failed(parent_row: Dict[str, Any], logger: logging.Logger) -> None:
    """A leader ended in Error: drop its pending entry so the next identical
    submit runs the query again (whoever already holds the pk sees the
    error, as they would for any failed query)."""
    cache_info = (parent_row.get("params") or {}).get("cache") or {}
    if cache_info.get("role") != ROLE_LEADER:
        return
    try:
        await ars_db.delete_pending_cache_entry(parent_row["id"])
    except Exception as e:
        logger.error(f"Cache cleanup for failed leader {parent_row['id']}: {e}")


async def invalidate_all(reason: Optional[str] = None) -> int:
    """Bump the generation: every existing entry becomes a miss."""
    return await ars_db.bump_cache_generation(reason)


async def stats() -> Dict[str, Any]:
    return await ars_db.cache_stats()


async def repair_sweep(logger: logging.Logger) -> Dict[str, int]:
    """Watchdog pass: settle stale pending entries (finish a Done leader's
    bookkeeping, drop anything else so the query can be re-run) and purge
    superseded generations."""
    counts = {"stale_pending": 0, "purged": 0}
    if not settings.ars_cache_enabled:
        return counts
    for entry in await ars_db.get_stale_pending_cache_entries(
        settings.ars_cache_pending_max_sec
    ):
        counts["stale_pending"] += 1
        leader_pk = entry["source_pk"]
        try:
            if entry.get("leader_status") == "D":
                row = await ars_db.get_message_row(leader_pk)
                if row is not None:
                    await on_parent_complete(row, logger)
                    # a run the hook judged not cacheable leaves the row
                    # behind; make sure it is gone either way
                    await ars_db.delete_pending_cache_entry(leader_pk)
                    continue
            logger.warning(
                f"Cache: pending entry led by {leader_pk} "
                f"(status {entry.get('leader_status')}) is stale; dropping it"
            )
            await ars_db.delete_pending_cache_entry(leader_pk)
        except Exception as e:
            logger.error(f"Cache repair for leader {leader_pk} failed: {e}")
    try:
        generation = await ars_db.get_cache_generation()
        counts["purged"] = await ars_db.purge_stale_cache_entries(
            generation, settings.ars_cache_stale_grace_sec
        )
    except Exception as e:
        logger.error(f"Cache purge failed: {e}")
    return counts
