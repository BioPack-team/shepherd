"""ARS whole-response cache.

Shepherd-native (the upstream ARS has none); design in
docs/ARS_RESPONSE_CACHE_PLAN.md. A submit whose query graph is structurally
identical to a completed prior submit is answered by copying that tree --
parent, final merged message, every per-ARA child -- so no other Shepherd
worker runs. Identical in-flight submits coalesce onto one leader run.

The cache stores no payloads: ``ars_response_cache`` maps a canonical
query-graph hash to the parent pk of a completed tree (the *source tree*),
whose blobs already live in ``ars_message.data``. Source trees backing a
live generation are exempt from the payload retention purge.

Two request-side knobs: TRAPI ``bypass_cache`` (no read, no write) and
``parameters.overwrite_cache`` (no read, forced write). Whole-cache
invalidation bumps a generation counter.

Canonicalization is renaming-invariant: node / edge / path ids are labels,
so results served from the cache have their bindings rewritten to the
caller's ids, and the merged message gets a TRAPI log entry saying where it
came from.
"""

import asyncio
import datetime
import hashlib
import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple

import orjson

from shepherd_utils.broker import add_task
from shepherd_utils.config import settings
from shepherd_utils.logger import resolve_log_level

from . import db as ars_db
from .completion import MERGE_AGENT_NAME
from .notify import notify_subscribers

# Baked into every key: bump when any canonicalization rule changes, which
# orphans (and lazily purges) every existing entry without an explicit
# invalidation.
CACHE_KEY_VERSION = "1"

MODE_NORMAL = "normal"
MODE_BYPASS = "bypass"
MODE_OVERWRITE = "overwrite"

ROLE_HIT = "hit"
ROLE_LEADER = "leader"
ROLE_FOLLOWER = "follower"
ROLE_OVERWRITE = "overwrite"
ROLE_BYPASS = "bypass"
# Cache bookkeeping failed for this submit; it ran as an ordinary query.
ROLE_UNCACHED = "uncached"

# Outcomes of before_dispatch
DISPATCH = "dispatch"  # fan out as usual
SERVED = "served"  # tree copied from the cache; parent already Done
WAITING = "waiting"  # coalesced onto a pending leader; parent stays Running

MERGE_INFORESID = "infores:ars"

# Beyond this many candidate labelings of tied nodes, fall back to a
# deterministic (but not renaming-invariant) tie-break. Only a *miss* can
# result, never a wrong hit.
MAX_TIE_PERMUTATIONS = 5040

_ENDPOINT_KEYS = ("subject", "object")
_LABEL_KINDS = ("nodes", "edges", "paths")


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


# ---------------------------------------------------------------------------
# Payload rewriting for a hit
# ---------------------------------------------------------------------------


def compose_label_maps(
    source_map: Optional[Dict[str, Dict[str, str]]],
    caller_map: Optional[Dict[str, Dict[str, str]]],
) -> Dict[str, Dict[str, str]]:
    """source id -> caller id, via the shared canonical ids. Identity
    entries are dropped so an empty result means "nothing to rename"."""
    out: Dict[str, Dict[str, str]] = {}
    for kind in _LABEL_KINDS:
        src = (source_map or {}).get(kind) or {}
        caller = (caller_map or {}).get(kind) or {}
        canon_to_caller = {canon: orig for orig, canon in caller.items()}
        mapping = {}
        for source_id, canon in src.items():
            target = canon_to_caller.get(canon)
            if target is not None and target != source_id:
                mapping[source_id] = target
        if mapping:
            out[kind] = mapping
    return out


def _rename_keys(obj: Any, mapping: Dict[str, str]) -> Any:
    if not isinstance(obj, dict) or not mapping:
        return obj
    return {mapping.get(k, k): v for k, v in obj.items()}


def rename_labels(payload: Any, mapping: Dict[str, Dict[str, str]]) -> Any:
    """Rewrite query-graph labels and result bindings in a TRAPI payload.

    Renames ``message.query_graph`` node/edge/path keys and the
    subject/object references inside edges and paths, every
    ``results[*].node_bindings`` key, and every
    ``results[*].analyses[*].edge_bindings`` / ``path_bindings`` key.
    Knowledge-graph and auxiliary-graph ids are KG ids, not query ids, and
    are untouched. Mutates and returns ``payload``; a no-op for an empty
    mapping or a non-dict payload.
    """
    if not mapping or not isinstance(payload, dict):
        return payload
    node_map = mapping.get("nodes") or {}
    edge_map = mapping.get("edges") or {}
    path_map = mapping.get("paths") or {}
    message = payload.get("message")
    if not isinstance(message, dict):
        return payload
    qg = message.get("query_graph")
    if isinstance(qg, dict):
        if isinstance(qg.get("nodes"), dict):
            qg["nodes"] = _rename_keys(qg["nodes"], node_map)
        for section, section_map in (("edges", edge_map), ("paths", path_map)):
            items = qg.get(section)
            if not isinstance(items, dict):
                continue
            for item in items.values():
                if isinstance(item, dict):
                    for key in _ENDPOINT_KEYS:
                        if key in item and item[key] in node_map:
                            item[key] = node_map[item[key]]
            qg[section] = _rename_keys(items, section_map)
    results = message.get("results")
    if isinstance(results, list):
        for result in results:
            if not isinstance(result, dict):
                continue
            if isinstance(result.get("node_bindings"), dict):
                result["node_bindings"] = _rename_keys(result["node_bindings"], node_map)
            analyses = result.get("analyses")
            if isinstance(analyses, list):
                for analysis in analyses:
                    if not isinstance(analysis, dict):
                        continue
                    if isinstance(analysis.get("edge_bindings"), dict):
                        analysis["edge_bindings"] = _rename_keys(
                            analysis["edge_bindings"], edge_map
                        )
                    if isinstance(analysis.get("path_bindings"), dict):
                        analysis["path_bindings"] = _rename_keys(
                            analysis["path_bindings"], path_map
                        )
    return payload


def cache_log_entry(source_pk, ready_at, generation) -> Dict[str, Any]:
    now = datetime.datetime.now(datetime.timezone.utc)
    cached = ready_at.isoformat() if hasattr(ready_at, "isoformat") else str(ready_at)
    return {
        "timestamp": now.isoformat(),
        "level": "INFO",
        "code": None,
        "message": (
            f"Served from ARS response cache (source {source_pk}, "
            f"cached {cached}, generation {generation})"
        ),
    }


def append_cache_log(payload: Any, entry: Dict[str, Any]) -> Any:
    if not isinstance(payload, dict):
        return payload
    logs = payload.get("logs")
    if not isinstance(logs, list):
        logs = []
    logs.append(entry)
    payload["logs"] = logs
    return payload


def _rewrite_payload(payload, mapping, log_entry):
    """CPU-bound part of a copy, run off the event loop."""
    payload = rename_labels(payload, mapping)
    if log_entry is not None:
        payload = append_cache_log(payload, log_entry)
    return payload


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _cache_params(parent_row: Dict[str, Any], **cache_fields) -> Dict[str, Any]:
    params = dict(parent_row.get("params") or {})
    params["cache"] = cache_fields
    return params


async def _set_role(parent_row, logger, **cache_fields) -> Dict[str, Any]:
    params = _cache_params(parent_row, **cache_fields)
    updated = await ars_db.update_message(parent_row["id"], params=params)
    return updated or dict(parent_row, params=params)


async def before_dispatch(
    parent_row: Dict[str, Any], body: Dict[str, Any], logger: logging.Logger
) -> Tuple[str, Dict[str, Any]]:
    """Cache step of ``submit``, after the parent row + query blob exist.

    Returns ``(outcome, parent_row)`` where outcome is DISPATCH (fan out as
    usual), SERVED (the tree was copied; the row is already Done) or
    WAITING (coalesced onto a pending leader; no fan-out). Any failure in
    the cache bookkeeping degrades to DISPATCH -- the cache must never make
    a submit fail.
    """
    if not settings.ars_cache_enabled:
        return DISPATCH, parent_row
    parent_pk = parent_row["id"]
    mode = resolve_mode(body)
    try:
        key, caller_map = cache_key(body)
        generation = await ars_db.get_cache_generation()
        base = {"key": key, "generation": generation}
        if mode == MODE_BYPASS:
            row = await _set_role(parent_row, logger, role=ROLE_BYPASS, **base)
            return DISPATCH, row
        if mode == MODE_OVERWRITE:
            row = await _set_role(parent_row, logger, role=ROLE_OVERWRITE, **base)
            return DISPATCH, row

        entry, claimed = await ars_db.claim_or_get_cache_entry(
            generation, key, parent_pk
        )
        if claimed:
            row = await _set_role(parent_row, logger, role=ROLE_LEADER, **base)
            return DISPATCH, row
        if entry is not None and entry["state"] == "ready":
            served = await materialize(parent_row, entry, caller_map, logger)
            if served is not None:
                return SERVED, served
            # broken entry was dropped inside materialize: try to lead
            entry, claimed = await ars_db.claim_or_get_cache_entry(
                generation, key, parent_pk
            )
            if claimed:
                row = await _set_role(parent_row, logger, role=ROLE_LEADER, **base)
                return DISPATCH, row
        if entry is None or entry["state"] != "pending":
            row = await _set_role(parent_row, logger, role=ROLE_UNCACHED, **base)
            return DISPATCH, row

        leader_pk = entry["source_pk"]
        await ars_db.add_cache_waiter(parent_pk, leader_pk, generation, key)
        row = await _set_role(
            parent_row,
            logger,
            role=ROLE_FOLLOWER,
            leader_pk=str(leader_pk),
            **base,
        )
        # The leader may have completed between our conflict and the waiter
        # insert, resolving its waiters without us; a ready entry now means
        # nobody else will copy for this parent.
        fresh = await ars_db.get_cache_entry(generation, key)
        if fresh is not None and fresh["state"] == "ready":
            served = await materialize(row, fresh, caller_map, logger)
            await ars_db.delete_cache_waiter(parent_pk)
            if served is not None:
                return SERVED, served
            row = await _set_role(parent_row, logger, role=ROLE_LEADER, **base)
            return DISPATCH, row
        logger.info(f"Cache: {parent_pk} coalesced onto leader {leader_pk}")
        return WAITING, row
    except Exception as e:
        logger.error(f"Cache bookkeeping failed for {parent_pk}: {e}", exc_info=True)
        return DISPATCH, parent_row


class _BrokenEntry(Exception):
    """The source tree behind a ready entry is unusable."""


async def materialize(
    parent_row: Dict[str, Any],
    entry: Dict[str, Any],
    caller_map: Optional[Dict[str, Dict[str, str]]],
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    """Copy the entry's source tree under ``parent_row``.

    Every non-merge child and the final merged message are re-created as
    children of the new parent with their original status / code /
    counts; payloads are rewritten to the caller's query-graph labels and
    the merged message gets the cache log entry. The parent ends Done with
    ``merged_version`` pointing at the copy and ``params.cache`` recording
    the hit. Returns the updated parent row, or None when the copy did not
    happen (a broken entry is deleted; any transient failure leaves the
    entry alone). Partially created children are removed either way.
    """
    parent_pk = parent_row["id"]
    source_pk = entry["source_pk"]
    try:
        source = await ars_db.get_message_row(source_pk)
        if source is None or source.get("status") != "D":
            raise _BrokenEntry(f"source {source_pk} missing or not Done")
        merged_pk = source.get("merged_version")
        if merged_pk is None:
            raise _BrokenEntry(f"source {source_pk} has no merged_version")
        children = await ars_db.get_children(source_pk)
        merged_row = next(
            (c for c in children if str(c["id"]) == str(merged_pk)), None
        )
        if merged_row is None:
            raise _BrokenEntry(f"merged message {merged_pk} not among children")
        merged_data = await ars_db.load_message_data(merged_pk, logger)
        if merged_data is None:
            raise _BrokenEntry(f"merged message {merged_pk} has no payload")

        mapping = compose_label_maps(entry.get("label_map"), caller_map)
        log_entry = cache_log_entry(
            source_pk, entry.get("ready_at"), entry.get("generation")
        )
        new_merged_pk = None
        for child in children:
            is_merge = (
                child.get("inforesid") == MERGE_INFORESID
                or child.get("agent_name") == MERGE_AGENT_NAME
            )
            if is_merge and str(child["id"]) != str(merged_pk):
                continue  # intermediate merges are not part of the answer
            if is_merge:
                data = merged_data
            else:
                data = await ars_db.load_message_data(child["id"], logger)
            new = await ars_db.create_message(
                actor_id=child["actor"],
                status=child["status"],
                code=child["code"],
                ref=parent_pk,
                params=child.get("params"),
                name=child.get("name") or "",
            )
            if data is not None:
                data = await asyncio.to_thread(
                    _rewrite_payload, data, mapping, log_entry if is_merge else None
                )
                await ars_db.save_message_data(new["id"], data, logger)
            await ars_db.update_message(
                new["id"],
                skip_coercion=True,
                status=child["status"],
                code=child["code"],
                url=child.get("url"),
                result_count=child.get("result_count"),
                result_stat=child.get("result_stat"),
            )
            await ars_db.persist_data_copy(new["id"], logger)
            if is_merge:
                new_merged_pk = new["id"]

        params = dict(parent_row.get("params") or {})
        source_params = source.get("params") or {}
        if "stats" in source_params:
            params["stats"] = source_params["stats"]
        params["cache"] = {
            "key": entry.get("cache_key"),
            "generation": entry.get("generation"),
            "role": ROLE_HIT,
            "source_pk": str(source_pk),
            "served_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        updated = await ars_db.update_message(
            parent_pk,
            status="D",
            code=200,
            merged_version=str(new_merged_pk),
            merged_versions_list=[[str(new_merged_pk), "ars"]],
            result_count=source.get("result_count"),
            result_stat=source.get("result_stat"),
            params=params,
        )
        await ars_db.persist_data_copy(parent_pk, logger)
        if updated is not None:
            await notify_subscribers(updated, None, logger)
        try:
            await ars_db.record_cache_hit(entry["generation"], entry["cache_key"])
        except Exception as e:  # bookkeeping only
            logger.debug(f"Cache hit count update failed: {e}")
        logger.info(
            f"Cache hit: {parent_pk} served from source {source_pk} "
            f"({len(mapping)} label kinds renamed)"
        )
        return updated
    except _BrokenEntry as e:
        logger.warning(f"Cache entry {entry.get('cache_key')} unusable, dropping: {e}")
        try:
            await ars_db.delete_cache_entry(entry["generation"], entry["cache_key"])
        except Exception as de:
            logger.error(f"Failed to drop broken cache entry: {de}")
    except Exception as e:
        logger.error(f"Cache copy for {parent_pk} failed: {e}", exc_info=True)
    try:
        await ars_db.delete_children(parent_pk)
    except Exception as e:
        logger.error(f"Failed to clean up partial copy under {parent_pk}: {e}")
    return None


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


async def _dispatch_fanout(parent_pk, logger) -> None:
    await add_task(
        "ars.fanout",
        {
            "parent_pk": str(parent_pk),
            "query_id": str(parent_pk),
            "log_level": resolve_log_level(settings.log_level),
            "otel": await ars_db.load_otel_carrier(parent_pk, logger),
        },
        logger,
        raise_on_failure=True,
    )


async def fail_over(leader_pk, logger: logging.Logger) -> Optional[str]:
    """A leader will not produce a cache entry: promote its oldest waiter
    to leader (and fan it out), repoint the rest, or drop the pending entry
    when nobody is waiting. Returns the new leader pk, if any."""
    waiters = await ars_db.get_cache_waiters(leader_pk)
    if not waiters:
        await ars_db.delete_pending_cache_entry(leader_pk)
        return None
    new_leader = waiters[0]
    new_pk = new_leader["parent_pk"]
    repointed = await ars_db.repoint_pending_cache_entry(leader_pk, new_pk)
    if not repointed:
        # entry vanished (invalidated/dropped): re-create it so later
        # identical submits coalesce onto the successor
        await ars_db.claim_or_get_cache_entry(
            new_leader["generation"], new_leader["cache_key"], new_pk
        )
    await ars_db.repoint_cache_waiters(leader_pk, new_pk)
    row = await ars_db.get_message_row(new_pk)
    if row is not None:
        await _set_role(
            row,
            logger,
            key=new_leader["cache_key"],
            generation=new_leader["generation"],
            role=ROLE_LEADER,
            promoted_from=str(leader_pk),
        )
    await _dispatch_fanout(new_pk, logger)
    logger.info(f"Cache: leader {leader_pk} failed over to {new_pk}")
    return str(new_pk)


async def _copy_to_waiter(waiter: Dict[str, Any], entry: Dict[str, Any], logger) -> bool:
    """Claim a waiter (delete-first, so concurrent resolvers never both copy
    under one parent), copy the tree to it, and put it back on failure so
    the repair sweep retries."""
    waiter_pk = waiter["parent_pk"]
    if not await ars_db.claim_cache_waiter(waiter_pk):
        return False
    row = await ars_db.get_message_row(waiter_pk)
    if row is None:
        return False
    body = await ars_db.load_message_data(waiter_pk, logger)
    _, caller_map = cache_key(body if isinstance(body, dict) else {})
    served = await materialize(row, entry, caller_map, logger)
    if served is not None:
        return True
    logger.warning(f"Cache: copy for waiter {waiter_pk} failed; left for the sweep")
    await ars_db.add_cache_waiter(
        waiter_pk, waiter["leader_pk"], waiter["generation"], waiter["cache_key"]
    )
    return False


async def _resolve_waiters(leader_pk, entry: Dict[str, Any], logger) -> None:
    for waiter in await ars_db.get_cache_waiters(leader_pk):
        await _copy_to_waiter(waiter, entry, logger)


async def on_parent_complete(parent_row: Dict[str, Any], logger: logging.Logger) -> None:
    """Completion hook: called once a parent reaches Done.

    Leaders flip their pending entry to ready and copy the tree to every
    waiter; overwrite runs repoint the key at their tree. Anything else is
    a no-op.
    """
    cache_info = (parent_row.get("params") or {}).get("cache") or {}
    role = cache_info.get("role")
    if role not in (ROLE_LEADER, ROLE_OVERWRITE):
        return
    parent_pk = parent_row["id"]
    try:
        children = await ars_db.get_children(parent_pk)
        cacheable = await _cacheable(parent_row, children, logger)
        if role == ROLE_LEADER and not cacheable:
            await fail_over(parent_pk, logger)
            return
        query = await ars_db.load_message_data(parent_pk, logger)
        _, label_map = cache_key(query if isinstance(query, dict) else {})
        if role == ROLE_LEADER:
            entry = await ars_db.mark_cache_entry_ready(parent_pk, label_map)
            if entry is None:
                logger.info(
                    f"Cache: {parent_pk} finished but no longer leads its entry"
                )
                # waiters, if any were left behind, still get this answer
                entry = {
                    "generation": cache_info.get("generation"),
                    "cache_key": cache_info.get("key"),
                    "source_pk": parent_pk,
                    "label_map": label_map,
                    "ready_at": datetime.datetime.now(datetime.timezone.utc),
                }
        else:
            if not cacheable:
                logger.info(f"Cache: overwrite run {parent_pk} not cacheable")
                return
            entry = await ars_db.upsert_cache_entry_ready(
                cache_info.get("generation"),
                cache_info.get("key"),
                parent_pk,
                label_map,
            )
            logger.info(f"Cache: entry {cache_info.get('key')} overwritten by {parent_pk}")
        await _resolve_waiters(parent_pk, entry, logger)
    except Exception as e:
        logger.error(f"Cache completion hook failed for {parent_pk}: {e}", exc_info=True)


async def on_parent_failed(parent_row: Dict[str, Any], logger: logging.Logger) -> None:
    """A leader ended in Error: its waiters must not wait on it."""
    cache_info = (parent_row.get("params") or {}).get("cache") or {}
    if cache_info.get("role") != ROLE_LEADER:
        return
    try:
        await fail_over(parent_row["id"], logger)
    except Exception as e:
        logger.error(
            f"Cache fail-over for {parent_row['id']} failed: {e}", exc_info=True
        )


async def invalidate_all(reason: Optional[str] = None) -> int:
    """Bump the generation: every existing entry becomes a miss."""
    return await ars_db.bump_cache_generation(reason)


async def stats() -> Dict[str, Any]:
    return await ars_db.cache_stats()


async def repair_sweep(logger: logging.Logger) -> Dict[str, int]:
    """Watchdog pass: finish or fail over stuck leaders, re-copy stuck
    waiters, purge superseded generations."""
    counts = {"stale_pending": 0, "stuck_waiters": 0, "purged": 0}
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
                    continue
            logger.warning(
                f"Cache: pending entry led by {leader_pk} "
                f"(status {entry.get('leader_status')}) is stale; failing over"
            )
            await fail_over(leader_pk, logger)
        except Exception as e:
            logger.error(f"Cache repair for leader {leader_pk} failed: {e}")
    for waiter in await ars_db.get_stuck_cache_waiters():
        counts["stuck_waiters"] += 1
        waiter_pk = waiter["parent_pk"]
        try:
            # a half-built copy from the crashed attempt is discarded first
            await ars_db.delete_children(waiter_pk)
            await _copy_to_waiter(waiter, waiter["entry"], logger)
        except Exception as e:
            logger.error(f"Cache repair for waiter {waiter_pk} failed: {e}")
    try:
        generation = await ars_db.get_cache_generation()
        counts["purged"] = await ars_db.purge_stale_cache_entries(
            generation, settings.ars_cache_stale_grace_sec
        )
    except Exception as e:
        logger.error(f"Cache purge failed: {e}")
    return counts
