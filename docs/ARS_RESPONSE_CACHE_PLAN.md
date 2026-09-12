# ARS Response Cache — Design Plan

**Status:** Implemented (revision 2 design). Code: `shepherd_utils/ars/cache.py`,
SQL in `shepherd_utils/ars/db.py` (response-cache section), routes in
`shepherd_server/aras/ars.py`, hook in `shepherd_utils/ars/lifecycle.py`,
sweep in `workers/ars_watchdog/worker.py`, CLI `scripts/ars_cache.py`.
Tests: `tests/unit/ars/test_cache_key.py`, `test_cache_flow.py`,
`test_ars_api_contract.py` (cache section). Register: deviation 13.
**Scope:** A whole-response cache in front of the ARS pipeline. A cache hit on
`POST /ars/api/submit` produces a fully completed ARS message tree (parent,
merged message, per-ARA children) without enqueuing work for any other
Shepherd worker (no fan-out, no ARA lookups, no merge, no post-process).

Revision 2 changes (from review): the cache reuses the completed trees
already stored in `ars_message.data` instead of a separate blob table;
retention rules are redefined around that; query-graph canonicalization is
renaming-invariant for node/edge/path ids; per-ARA children are always
carried in full; a served response carries a TRAPI log entry saying it came
from the cache.

---

## 1. Requirements

| # | Requirement | Design answer (section) |
|---|---|---|
| R1 | Cache the **full response**; a hit must not touch any other Shepherd worker | The leader's own completed tree *is* the cache entry; a hit copies it into a new tree inline in `submit` (§4, §6.1) |
| R2 | Key = hash of the query graph, agnostic to key order, null / missing / empty fields, **and to node/edge/path id names** | Structural canonicalization + SHA-256 (§3) |
| R3 | **No TTL** on cache entries | Postgres; cache-source trees are exempt from the retention purge while their generation is live (§7) |
| R4 | Invalidate the **whole cache** on demand | Generation counter; bump = invalidate all; superseded sources age out under normal retention (§7) |
| R5 | TRAPI `bypass_cache` skips the cache | No read, no write (§5) |
| R6 | `parameters.overwrite_cache` overwrites that one entry | No read, forced write (§5) |
| R7 | Two identical in-flight misses → run once, answer both | Leader/follower coalescing on a `pending` index row, resolved at parent completion (§6) |
| R8 | Full-fidelity per-ARA children on a hit | Every child row + blob is copied (§6.1) |
| R9 | A served response says it came from the cache | TRAPI `logs` entry appended to the merged message (§5) |

---

## 2. Where the cache lives — Postgres, reusing `ars_message.data`

Every terminal ARS message already has a zstd copy of its payload in
`ars_message.data` (`persist_data_copy`). The cache adds **no second copy of
any blob**. It adds one small **index table** mapping a cache key to the
parent pk of a completed tree (the *source tree*), and a rule that source
trees backing a live cache generation are never purged. A hit copies the
source tree into a fresh tree for the new parent (rows + blobs), so every
client still owns an ordinary message tree with its own pk.

Why not Redis: `shepherd_broker/redis.conf` runs `maxmemory 6gb` with
`maxmemory-policy volatile-ttl`, under which only keys *with* a TTL are ever
evicted. TTL-less data would grow until the cap, and then **every write to
the shared instance fails** (Streams broker, hot blobs, locks, logs). Postgres
disk is the cheap, growable resource here; Redis memory is not. Redis stays
what it is today: the hot copy (with `redis_ttl`) that the UI reads, written
for a cached tree exactly as for a fresh one.

Disk cost model: one source tree per distinct query (kept until the
generation is invalidated *and* the normal retention window has passed) plus
one copied tree per hit (aging out under normal retention like any other
tree). The per-hit copy exists because bindings are rewritten to the
caller's ids and a cache log entry is appended (§5, §6.1), so the bytes are
never identical to the source anyway. A zero-copy variant (`data_ref`
pointer column, rewrite on read) is possible later if hit volume makes copy
storage a problem; not in v1.

---

## 3. Cache key

`cache_key = sha256(CACHE_KEY_VERSION + "\n" + canonical_json(key_material))`

**Key material** (Q1 in §10, accepted):

```
{ "query_graph": <canonical structural form of body.message.query_graph>,
  "workflow":    <canonical body.workflow, only when a non-empty list> }
```

Everything else on the submit body is excluded: `submitter`, `callback`,
`log_level`, `name`, `bypass_cache`, `parameters`, `validate`, any
pre-populated `knowledge_graph` / `results`.

### 3.1 Value canonicalization (`cache.canonicalize`)

1. Recursively drop object members whose value is `null`, `{}` or `[]`,
   so `"ids": null`, `"ids": []` and a missing `ids` are the same. Empty
   strings are kept (an empty id/predicate is a different, malformed query).
2. Object keys sorted.
3. Lists are **sets**: elements canonicalized, then sorted by their
   canonical serialization. Every list in a TRAPI query graph is set-like
   (`ids`, `categories`, `predicates`, `constraints`,
   `qualifier_constraints`, `qualifier_set`, `attribute_constraints`,
   `member_ids`).
4. Booleans/numbers untouched. TRAPI schema defaults (`set_interpretation:
   "BATCH"`, `knowledge_type: "lookup"`) are not folded in v1 (Q2).

### 3.2 Structural canonicalization — renaming-invariant ids (`cache.canonical_graph`)

Node, edge and path ids (`n0`, `sn`, `t_edge`, `p0`) are **labels**, not
content. Two query graphs that differ only in those labels must produce the
same key, and (§5) the hit must map the source's labels onto the caller's.

Algorithm (graphs are tiny — 2 to ~6 nodes — so exactness is cheap):

1. **Node content color**: `c0(n) = hash(canonical(node minus nothing))` —
   the node's own body (ids, categories, constraints, set_interpretation…).
2. **Color refinement** (Weisfeiler–Lehman style), iterate until stable:
   `c(n) = hash(c_prev(n), sorted multiset of (canonical(edge body minus
   subject/object), "out"|"in", c_prev(other end)))` over both `edges` and
   `paths` incident to `n`.
3. **Order nodes** by final color. Nodes that still tie are automorphic
   candidates; for each tie group enumerate permutations (bounded: groups
   are small; if the product of factorials exceeds 5 040 fall back to color
   order only, which is still deterministic and only risks a *miss*, never
   a wrong hit). Pick the permutation giving the lexicographically smallest
   full serialization below. Assign `n0, n1, …`.
4. **Relabel edges and paths**: rewrite `subject`/`object` to the new node
   labels, sort edges by `canonical(edge)` and assign `e0, e1, …`; same for
   `paths` → `p0, p1, …`. Edge/path ids are not referenced anywhere else
   inside a query graph.
5. The canonical graph is `{nodes: {n0:…}, edges: {e0:…}, paths: {p0:…}}`
   with §3.1 applied. The **label map** `{caller_id → canonical_id}` for
   nodes, edges and paths is returned alongside; it is what §5 uses to
   rewrite bindings.

Correctness note: a *wrong* hit is impossible from label symmetry — two
nodes with identical colors are structurally interchangeable, so any
consistent choice yields a graph the ARAs would answer identically. The only
failure mode of an imperfect canonicalization is a missed hit.

`CACHE_KEY_VERSION` is baked into the hash; changing any rule above bumps it
and orphans old entries without an explicit invalidation.

---

## 4. Data model

```sql
-- Singleton generation counter; bumped to invalidate everything.
CREATE TABLE IF NOT EXISTS ars_cache_meta (
  id            BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  generation    INT NOT NULL DEFAULT 1,
  bumped_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  bumped_reason TEXT
);
INSERT INTO ars_cache_meta (id) VALUES (TRUE) ON CONFLICT DO NOTHING;

-- The index: key -> source tree. 'pending' while the leader runs.
CREATE TABLE IF NOT EXISTS ars_response_cache (
  generation    INT  NOT NULL,
  cache_key     TEXT NOT NULL,
  state         TEXT NOT NULL CHECK (state IN ('pending','ready')),
  source_pk     UUID NOT NULL REFERENCES ars_message(id),   -- leader parent = source tree root
  label_map     JSONB,          -- source ids -> canonical ids (nodes/edges/paths), set at ready
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ready_at      TIMESTAMPTZ,
  hit_count     INT NOT NULL DEFAULT 0,
  last_hit_at   TIMESTAMPTZ,
  PRIMARY KEY (generation, cache_key)
);
CREATE INDEX IF NOT EXISTS idx_ars_response_cache_source ON ars_response_cache (source_pk);
CREATE INDEX IF NOT EXISTS idx_ars_response_cache_state_created ON ars_response_cache (state, created_at);

-- Followers waiting on a pending leader.
CREATE TABLE IF NOT EXISTS ars_response_cache_waiter (
  parent_pk   UUID PRIMARY KEY REFERENCES ars_message(id),
  leader_pk   UUID NOT NULL REFERENCES ars_message(id),
  generation  INT  NOT NULL,
  cache_key   TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ars_response_cache_waiter_leader ON ars_response_cache_waiter (leader_pk);
```

No new columns on `ars_message`; a hit tree records its provenance in
`params.cache` (§5). `idx_ars_response_cache_source` must be added to the
marker-index list in `shepherd_utils/db.py::apply_schema_upgrades`, or
pre-existing volumes never receive the new tables.

Sizing: the index rows are bytes; the storage that grows is
`ars_message.data` on source trees (kept while live) and on hit copies
(normal retention). Plan the Postgres volume for
`distinct-queries × tree-size + hits-per-retention-window × tree-size`.

---

## 5. Request-side semantics (`POST /ars/api/submit`)

```
mode = "normal"
if body.bypass_cache is true:                 mode = "bypass"     # no read, no write
elif body.parameters.overwrite_cache is true: mode = "overwrite"  # no read, forced write
```

| mode | lookup | `ready` hit | `pending` hit | miss | at completion |
|---|---|---|---|---|---|
| normal | yes | copy source tree inline (§6.1) | become follower (§6.2) | claim leadership, fan out | leader flips row to `ready`, resolves followers |
| overwrite | no | — | — | fan out (not a leader; ignores pending rows) | upsert row to point at *this* tree (last write wins) |
| bypass | no | — | — | fan out | nothing |

The submit body is forwarded to ARAs unchanged in every mode.

**What a hit serves.** For each blob copied from the source tree:

1. **Label rewrite**: compose `source_id → canonical_id` (stored
   `label_map`) with `canonical_id → caller_id` (computed from the incoming
   graph) and rename, inside every payload: `message.query_graph` node /
   edge / path keys and the `subject`/`object` references; every
   `results[*].node_bindings` key; every `results[*].analyses[*].edge_bindings`
   key; every `analyses[*].path_bindings` key. Knowledge-graph and
   auxiliary-graph ids are untouched (they are KG ids, not query ids). When
   the composed map is the identity, the rename pass is skipped.
2. **Cache log entry** (merged message only — the per-ARA children are
   copied verbatim so they remain what each ARA actually returned):
   ```json
   {"timestamp": "<now iso>", "level": "INFO", "code": null,
    "message": "Served from ARS response cache (source <source_pk>, cached <ready_at>, generation <g>)"}
   ```
   appended to top-level `logs` (created if absent).
3. Re-encode with the shared `encode_message` codec, write to Redis DB 1
   (`redis_ttl`) and `ars_message.data`.

Provenance on the parent row, visible in the envelope and cheap for the
monitor to count:

```json
"cache": {"key": "<sha256>", "generation": 3,
          "role": "hit" | "leader" | "follower" | "overwrite" | "bypass",
          "source_pk": "<uuid>"   /* hit, follower */,
          "leader_pk": "<uuid>"   /* follower while pending */,
          "served_at": "<iso>"    /* hit, follower */}
```

**Response to the client.** `201` with the parent envelope. On a hit the
parent already reads `Done/200` with `merged_version` set (Q7). Clients
polling `messages/<pk>?trace=y` see a complete tree with all children.

---

## 6. Coalescing and materialization

### 6.1 Copying the source tree — `cache.materialize(parent_pk, entry, caller_graph, logger)`

Called inline from `submit` (ready hit) and from the completion hook
(followers). Runs the CPU-bound decode/rename/encode in a thread
(`asyncio.to_thread`) like `save_message` already does.

1. Load the source parent row and its children (`get_children(source_pk)`).
   If the source parent is no longer `D`, or its merged message has no data
   anywhere (Redis or Postgres), treat the entry as **broken**: delete the
   index row, log a warning, and continue as a miss (become leader).
2. For each non-merge child, in `ts` order: `create_message(actor_id=
   child.actor, status, code, ref=parent_pk, params=child.params,
   name=child.name)`; copy the blob via `load_message_data(child.id)` →
   rename → `save_message_data(new_pk)`; `update_message(new_pk,
   skip_coercion=True, status, code, url, result_count, result_stat)` so a
   cached `E/598` child stays `598`; `persist_data_copy(new_pk)`.
3. Merge child (`ars-ars-agent`, i.e. the source's `merged_version`):
   same, plus the cache log entry; `status D / code 200`.
4. `update_message(parent_pk, status="D", code=200, merged_version=<new
   merge pk>, merged_versions_list=[[<new merge pk>, "ars"]],
   params={...source params (query_type, stats), "cache": {...}})`, then
   `persist_data_copy(parent_pk)`.
5. `notify_subscribers(parent, None)` — the parent→`D` save-time
   notification; relevant for followers, a no-op for inline hits.
6. `UPDATE ars_response_cache SET hit_count = hit_count + 1, last_hit_at = NOW()`.

Actors are referenced by id straight from the source rows (same database,
same registry), so no `(inforesid, path)` re-resolution is needed.

### 6.2 Leader / follower on a miss (in `submit`)

```sql
INSERT INTO ars_response_cache (generation, cache_key, state, source_pk)
VALUES (%s, %s, 'pending', %s) ON CONFLICT (generation, cache_key) DO NOTHING RETURNING source_pk;
```

- Inserted → **leader**: proceed exactly as today (`add_task("ars.fanout")`),
  `params.cache.role = "leader"`.
- Conflict → re-read the row: `ready` (lost a race) → materialize as a hit;
  `pending` → **follower**: insert a waiter row, **no** fan-out, parent stays
  `Running/202`, return the 201 envelope.

### 6.3 Completion hook (in `lifecycle.check_parent_completion`)

After the parent transitions to `D` (both branches), `cache.on_parent_complete(parent_pk)`:

- **leader**: cacheability check (Q5 — skip when the merged result is the
  synthesized empty message *and* an ARA child ended in `E`; behind
  `ars_cache_store_partial`). If cacheable: compute `label_map` from the
  parent's own query graph and `UPDATE … SET state='ready', ready_at=NOW(),
  label_map=%s WHERE source_pk=%s AND state='pending'`. If not: delete the
  pending row and fail followers over (below).
- **overwrite**: `INSERT … ON CONFLICT DO UPDATE SET state='ready',
  source_pk=<this parent>, label_map=…, ready_at=NOW()` (the previous
  source tree becomes an ordinary tree and ages out).
- Then resolve waiters: `SELECT parent_pk FROM ars_response_cache_waiter
  WHERE leader_pk = %s`; for each, load its stored query blob (for the
  caller-side label map), `materialize`, delete the waiter row. Failures
  are per-waiter and left for the repair sweep.

If the parent transitions to **`E`** (leader failed): **fail over** — oldest
waiter becomes the new leader (`UPDATE ars_response_cache SET source_pk =
<it>`, repoint remaining waiters, set its `params.cache.role = "leader"`),
and `add_task("ars.fanout")` for it from its own stored query. No waiters →
delete the pending row.

### 6.4 Repair sweep (`ars_watchdog`, existing 60 s loop)

1. `pending` rows older than `ars_cache_pending_max_sec` (default 1200 s)
   whose leader is terminal or missing → run §6.3 for that leader.
2. Waiters whose entry is `ready` but who are still `R` (a materialization
   crashed) → delete any half-created children (`DELETE FROM ars_message
   WHERE ref = waiter_pk`) and materialize again; idempotent.
3. Delete index rows with `generation < current` once older than
   `ars_cache_stale_grace_sec` (default 86400 s), in batches. Their source
   trees then fall under normal retention (§7).

---

## 7. Invalidation and retention

**Whole-cache invalidation**: `UPDATE ars_cache_meta SET generation =
generation + 1, bumped_reason = %s`. Lookups filter on the current
generation, so every entry is a miss instantly and atomically. In-flight
leaders finish under the generation they claimed and simply age out.

Front doors, both calling `cache.invalidate_all(reason)`:

1. `scripts/ars_cache.py invalidate --reason "…"`, plus `stats`,
   `show <key>`, `evict <key>` — direct Postgres, the operator path.
2. `POST /ars/api/cache/invalidate`, `GET /ars/api/cache/` — gated on a new
   `ars_admin_token` (`Authorization: Bearer …`); `403` when the setting is
   empty (disabled by default; the upstream ARS has no admin auth at all).

**Retention redefined.** Today `purge_old_message_data` nulls
`ars_message.data` for whole trees older than `ars_data_retention_days`
where `retain = false`. With the cache, that purge would silently destroy
cache entries. New rule, implemented in that one query:

```
a tree is purgeable  ⇔  root.ts < cutoff
                     AND no message in the tree has retain = true
                     AND root is not source_pk of any ars_response_cache row
                         with generation = current generation
```

Consequences, and how the settings/flags now read:

| setting / flag | old meaning | new meaning |
|---|---|---|
| `ars_data_retention_days` (30) | purge payloads of every non-retained tree after N days | purge payloads of non-retained trees **that do not back a live cache entry**; cache-source trees are kept for as long as their generation is live, then fall under this window (measured from their own `ts`, so they go on the next sweep after invalidation + grace) |
| `retain` (per message, `retain/<pk>`) | user-requested exemption from cleanup | unchanged; orthogonal to and independent of the cache (a retained tree can also be a cache source; a cache source is not marked retained) |
| `ars_cache_stale_grace_sec` (new) | — | how long superseded-generation index rows linger before deletion; only after they are gone does their source tree become purgeable |

Since disk is the accepted cost, `ars_data_retention_days` can also simply be
raised; nothing in the cache depends on its value. Hit-copy trees are
ordinary trees and age out under it.

---

## 8. Settings (`shepherd_utils/config.py`)

| setting | default | purpose |
|---|---|---|
| `ars_cache_enabled` | `True` | master switch; off = every submit behaves as `bypass` |
| `ars_cache_store_partial` | `True` | cache trees where some ARA children ended in `E`/`598` |
| `ars_cache_pending_max_sec` | `1200.0` | repair threshold for stuck leaders |
| `ars_cache_stale_grace_sec` | `86400.0` | how long superseded generations' index rows linger |
| `ars_admin_token` | `""` | bearer token for the cache admin routes; empty disables them |

(`ars_cache_store_children` from revision 1 is dropped: children are always
carried in full.)

---

## 9. Parity register entry

Add to `docs/ARS_PARITY_REGISTER.md` → *Behavioral deviations*:

> **Response cache** (Shepherd-native; upstream has none). With
> `ars_cache_enabled`, a submit whose structurally-canonical query graph
> (+workflow) matches a completed prior submit is answered by copying that
> tree: the `201` envelope already reads `Done/200` with `merged_version`
> set; the merged message carries an appended `logs` entry naming the cache
> source; query-graph labels and result bindings are rewritten to the
> caller's ids; children are copied with their original status/code.
> Identical in-flight submits coalesce onto one run. Opt out per query with
> TRAPI `bypass_cache`; refresh one entry with `parameters.overwrite_cache`;
> flush all with the generation bump. Trees backing a live cache entry are
> exempt from the payload retention purge. `params.cache` on the parent
> records the role.

---

## 10. Decisions

Resolved in review: reuse `ars_message.data` (§2, §7); renaming-invariant
ids (§3.2); full-fidelity children (§6.1); cache log entry in the response
(§5). Still open, with proposed defaults:

| # | Question | Proposed default |
|---|---|---|
| Q1 | Key material = `query_graph` **+ `workflow`**? | Include `workflow` when non-empty |
| Q2 | Fold TRAPI schema defaults (`set_interpretation: "BATCH"`, `knowledge_type: "lookup"`) so explicit-default ≡ missing? | Not in v1; trivial behind `CACHE_KEY_VERSION` |
| Q5 | Cache trees where some ARAs errored/timed out? | Yes, except "empty because everything failed"; `ars_cache_store_partial` to tighten |
| Q6 | HTTP invalidation route gated by a bearer token, or script-only? | Both; route disabled unless `ars_admin_token` is set |
| Q7 | `201` on a hit reports `Done`, or stays `Running` and materializes via a task? | Report `Done` |
| Q8 | Cache log entry on the merged message only, or also on each copied child? | Merged only; children stay verbatim ARA output |

---

## 11. Implementation outline

Files, in dependency order:

1. `shepherd_utils/ars/schema.sql`, `shepherd_db/init_db.sql`,
   `shepherd_utils/db.py` (marker index) — §4 tables.
2. `shepherd_utils/config.py` — §8 settings.
3. `shepherd_utils/ars/cache.py` (new) — `canonicalize`, `canonical_graph`
   (+ label map), `cache_key`, `resolve_mode`, `rename_labels(payload,
   mapping)`, `append_cache_log`, `lookup_or_claim`, `register_waiter`,
   `materialize`, `on_parent_complete`, `fail_over`, `invalidate_all`,
   `stats`, `repair_sweep`. SQL helpers in `shepherd_utils/ars/db.py`,
   including the rewritten `purge_old_message_data`.
4. `shepherd_server/aras/ars.py` — `submit` branches (§5, §6.2); `GET
   /api/cache/`, `POST /api/cache/invalidate` (§7).
5. `shepherd_utils/ars/lifecycle.py` — call `cache.on_parent_complete` in
   both `D` branches and the `E` branch.
6. `workers/ars_watchdog/worker.py` — `cache.repair_sweep` each tick.
7. `scripts/ars_cache.py` — operator CLI.
8. Docs: status → Implemented; register entry (§9); README section.

Tests (`tests/unit/ars/test_cache_*.py`, existing fakeredis + `ars_db`
AsyncMock conventions):

- **Key / canonicalization**: reordered keys; reordered `ids` /
  `categories` / `predicates`; `null` vs missing vs `[]` / `{}` — all equal.
  **Renaming**: `{sn, on, t_edge}` ≡ `{n0, n1, e0}` ≡ `{a, b, x}` with
  subject/object rewritten; a 2-hop chain with two blank middle nodes is
  stable under every relabeling; swapping subject/object changes the key;
  pathfinder `paths` relabel like edges; a graph with a symmetric pair of
  identical nodes yields the same key under either assignment and a label
  map that composes to a valid rename. `CACHE_KEY_VERSION` bump changes all.
- **Rename pass**: `query_graph`, `node_bindings`, `edge_bindings`,
  `path_bindings` keys renamed; KG / aux-graph ids untouched; identity map
  is a no-op that preserves bytes.
- **Submit contract**: miss → leader claims + fanout enqueued; ready hit →
  no fanout, tree copied (children count, statuses, `598` preserved),
  merged `logs` has the cache entry, envelope `Done`, `params.cache.role =
  hit`, hit_count incremented; pending → waiter, no fanout, `Running`;
  `bypass_cache` → no lookup/claim; `overwrite_cache` → no lookup, fanout,
  `role=overwrite`; `ars_cache_enabled=false` → bypass; broken source
  (missing data) → row deleted, becomes leader.
- **Completion hook**: leader `D` → row `ready` with `label_map`, waiters
  materialized (with *their* label maps) and deleted, subscribers notified;
  leader `E` → oldest waiter promoted + fanned out, others repointed;
  empty-merge with an `E` child → not cached, followers failed over;
  overwrite → `source_pk` repointed.
- **Retention**: purge skips a live cache source, purges it after
  invalidation + grace; retained trees still exempt; hit-copy trees purge
  normally.
- **Watchdog repair**: stale pending resolved; stuck follower
  re-materialized idempotently; stale generations purged after grace only.
- **Invalidation**: bump makes a ready row a miss; route `403` without
  token, `200` with; script `stats` / `invalidate` smoke test.
- **Parity harness**: second identical submit (with different node labels)
  yields an equivalent terminal tree after label normalization, with no
  stub-ARA requests in the mockworld journal and the cache log line present.
