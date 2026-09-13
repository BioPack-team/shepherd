# ARS Response Cache — Design Plan

**Status:** Implemented (revision 3 design). Code: `shepherd_utils/ars/cache.py`,
SQL in `shepherd_utils/ars/db.py` (response-cache section), routes in
`shepherd_server/aras/ars.py`, hook in `shepherd_utils/ars/lifecycle.py`,
sweep in `workers/ars_watchdog/worker.py`, CLI `scripts/ars_cache.py`.
Tests: `tests/unit/ars/test_cache_key.py`, `test_cache_flow.py`,
`test_ars_api_contract.py` (cache section). Register: deviation 13.

**Scope:** A whole-response cache in front of the ARS pipeline. There is
**one message tree per distinct query per cache generation**. The first
submit to see a query runs it; every later structurally identical submit is
handed that same pk. A hit enqueues nothing for any Shepherd worker and
stores nothing.

Revision history:

- *Revision 2:* reuse the completed trees already stored in
  `ars_message.data` instead of a separate blob table; renaming-invariant
  canonicalization; full per-ARA children; cache log line.
- *Revision 3:* **shared pks**. A hit no longer copies the source tree
  under a fresh pk; it returns the source pk. This removed the tree copy,
  the waiter table, leader fail-over and the stuck-waiter repair.
- *Revision 3.1 (this):* **the 201 carries no payload.** Embedding the
  merged response (converted to the caller's labels) in the hit's 201 cost
  seconds per hit -- decompress + stdlib `json.dumps(indent=2)` of tens of
  MB on the event loop, then the same bytes fetched again via
  `merged_version` -- and serialized concurrent hits. The 201 is now the
  source envelope with the caller's own submit body as `data`, like any
  fresh parent; clients fetch `merged_version` as usual and get the stored
  original under the source's labels. The label-rewrite code was removed.
  The merged-message GET decodes and serializes (orjson) in a worker thread.

---

## 1. Requirements

| # | Requirement | Design answer (section) |
|---|---|---|
| R1 | Cache the **full response**; a hit must not touch any other Shepherd worker | The leader's own completed tree *is* the cache entry; a hit answers with that tree's pk (§5, §6) |
| R2 | Key = hash of the query graph, agnostic to key order, null / missing / empty fields, **and to node/edge/path id names** | Structural canonicalization + SHA-256 (§3) |
| R3 | **No TTL** on cache entries | Postgres; cache-source trees are exempt from the retention purge while their generation is live (§7) |
| R4 | Invalidate the **whole cache** on demand | Generation counter; bump = invalidate all; superseded sources age out under normal retention (§7) |
| R5 | TRAPI `bypass_cache` skips the cache | No read, no write (§5) |
| R6 | `parameters.overwrite_cache` overwrites that one entry | No read, forced write (§5) |
| R7 | Two identical in-flight misses → run once, answer both | The second submit is handed the leader's pk while it is still Running (§6) |
| R8 | Full-fidelity per-ARA children on a hit | The shared pk *is* the original tree, children included (§6) |
| R9 | A served response says it came from the cache | Render-time TRAPI `logs` entry on GETs of the cached merged message (§5) |
| R10 | One pk per cache key; no per-hit copies | Hits create no rows and no blobs (§2, §6) |

---

## 2. Where the cache lives — Postgres, one tree per query

Every terminal ARS message already has a zstd copy of its payload in
`ars_message.data` (`persist_data_copy`). The cache adds **no copy of any
blob and no rows per hit**. It adds one small **index table** mapping a
cache key to the parent pk of the one completed tree that answers it (the
*source tree*), and a rule that source trees backing a live cache generation
are never purged. Every submit that matches an entry gets **that pk**.

Why not Redis: `shepherd_broker/redis.conf` runs `maxmemory 6gb` with
`maxmemory-policy volatile-ttl`, under which only keys *with* a TTL are ever
evicted. TTL-less data would grow until the cap, and then **every write to
the shared instance fails** (Streams broker, hot blobs, locks, logs). Postgres
disk is the cheap, growable resource here; Redis memory is not. Redis stays
what it is today: the hot copy (with `redis_ttl`) that the UI reads;
`load_message_data` re-warms it from Postgres when a cached tree is read
after its Redis copy expired.

Disk cost model: one source tree per distinct query per generation, kept
until the generation is invalidated *and* the normal retention window has
passed. Hits cost one counter increment.

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
same key. The served tree keeps the source's labels (§5).

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
   nodes, edges and paths is returned alongside and stored on the index
   row for inspection (`scripts/ars_cache.py show`).

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

-- The index: key -> the one tree that answers it. 'pending' while the leader runs.
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
| normal | yes | **201 with the source pk**, already Done; `data` = caller's body | **201 with the leader pk**, still Running; `data` = caller's body | create parent, claim key, fan out | leader flips row to `ready` |
| overwrite | no | — | — | create parent, fan out (no claim) | upsert row to point at *this* tree |
| bypass | no | — | — | create parent, fan out | nothing |

The submit body is forwarded to ARAs unchanged in every mode.

**What a hit's 201 carries.** The envelope of the source parent row (so
`pk`, `status: Done`, `merged_version`, `merged_versions_list`, `params`
are the source's) with `fields.data` set to the **caller's own submit
body**, exactly as a fresh parent's 201 would carry it. The merged response
is **not** embedded: the client polls `messages/<pk>?trace=y` (already
Done on the first read) and fetches `merged_version`, just as for a query
it ran itself. The fetched tree keeps the source's node/edge/path labels;
its bindings agree with its own query graph, which is what a response has
to satisfy. Nothing is written on a hit but the hit counter, and nothing
large is decompressed or serialized on the submit path -- the only
payload check is an `EXISTS` on the merged message.

**GET note.** When `GET /ars/api/messages/<pk>` renders the final merged
message of a tree that backs a ready entry, a log line is appended to the
rendered JSON at render time (`Served from ARS response cache: this message
is the cached answer for its query (source <pk>, generation g, cached ts,
served N time(s))`). The handler already decompresses the blob to render
it, so this is one indexed lookup; the stored bytes are never modified.

**Rendering cost.** That GET is the expensive half of every fetch, cached or
not: a merged message is tens of MB of JSON. `load_message_data` now
decompresses + parses in a worker thread, and the handler serializes once
with orjson in a worker thread (previously stdlib `json.dumps` → `loads` →
`dumps` on the event loop, ~4 s for 50 MB and blocking every other request
on that process meanwhile).

**Provenance.** Source parents carry `params.cache = {key, generation,
role}` with role `leader`, `overwrite`, `bypass` or `uncached` (cache
bookkeeping failed; ran as an ordinary query). Hits leave no row, so hit
volume lives in the index row's `hit_count` / `last_hit_at` (visible via
`scripts/ars_cache.py stats|show` and `GET /ars/api/cache`).

---

## 6. Submit flow in detail

`submit` runs two cache steps around the parent-row creation:

1. **`cache.lookup(body)`** — before any row exists. Normal mode only.
   Reads the current generation and the entry for the key.
   - `ready` → load the source parent row, check its `merged_version`
     payload exists (`EXISTS`, no decompression), bump `hit_count`, return
     **(SERVED, source row, caller's body)**.
   - `pending` → return **(WAITING, leader row, caller's body)**.
   - none → return None: proceed as a miss.
   - A `ready` entry whose source is missing / not Done / has no merged
     payload is **broken**: the entry is deleted and the submit proceeds as
     a miss. A `pending` entry whose leader row is gone is deleted likewise.
2. **`cache.claim_or_serve(parent_row, body)`** — after the parent row and
   its query blob exist (the 201 for a miss still promises a stored query
   and a queued fan-out).
   - bypass / overwrite → record the role, DISPATCH.
   - `INSERT … ON CONFLICT DO NOTHING` on `(generation, key)` with this
     parent as `source_pk`. Inserted → role `leader`, DISPATCH.
   - Conflict → a concurrent identical submit won between our lookup and
     our claim: serve *its* entry exactly as in step 1 and **discard our own
     parent row** (row + Redis blob); the caller gets the winner's pk.
   - Any exception → DISPATCH (the cache never fails a submit).

**Completion hook** (`lifecycle.check_parent_completion` → `cache.on_parent_complete`):
a `leader` whose run is cacheable flips its pending row to `ready` with the
`label_map` of its own query graph; a leader whose run is **not cacheable**
(Q5: empty merged result while an ARA child errored, or any error when
`ars_cache_store_partial` is off) deletes its pending row so the next
identical submit re-runs the query; an `overwrite` run upserts the row to
point at itself. A leader that ends in Error (`on_parent_failed`) deletes its
pending row.

**What a pending-hit caller experiences.** They poll the leader's pk and see
Done at the same instant the leader's own submitter does. If the leader's
run fails or is not cacheable, they see that outcome; the pending row is
gone, so a resubmit (or `bypass_cache`) runs the query afresh.

**Watchdog sweep** (`cache.repair_sweep`, every `ars_watchdog` tick):
`pending` rows older than `ars_cache_pending_max_sec` whose leader is Done
get the completion hook re-run (then the row is removed if still pending);
any other stale pending row — leader Error, missing, or still Running past
the threshold — is dropped so the query can be re-run. Superseded
generations' rows are deleted in batches after `ars_cache_stale_grace_sec`.

**Shared-pk consequences, accepted:** `retain/<pk>` retains the tree for
everyone (desirable); `block/<pk>` rewrites the shared payload for
everyone; the row's `timestamp`, `name` and stored submit body are the
first submitter's; `latest_pk` / `reports` / `get_status` count parent
rows, so hits are not query volume there (use `hit_count`). `filter/<pk>`
creates a *new* parent for its output, so it does not leak into the shared
trace.

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
raised; nothing in the cache depends on its value. Trees from `bypass_cache`
and `overwrite_cache` runs that are not (or no longer) cache sources are
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

See `docs/ARS_PARITY_REGISTER.md`, behavioral deviation 13.

---

## 10. Decisions

Resolved: Postgres index over `ars_message.data` (§2); renaming-invariant
ids with POST-time conversion (§3, §5); one pk per key, no per-hit copies
(§6); `workflow` in the key; TRAPI schema defaults not folded (behind
`CACHE_KEY_VERSION` if wanted later); partial trees cached except the
"empty because everything failed" case (`ars_cache_store_partial`); admin
route gated on `ars_admin_token` plus the CLI; the hit's 201 already reads
`Done` and carries no payload; the served tree keeps the source's labels;
the cache note is a render-time log line on the merged-message GET.

---

## 11. Implementation map

| concern | where |
|---|---|
| canonicalization, key, label maps, log-line helper | `shepherd_utils/ars/cache.py` (pure functions) |
| `lookup`, `claim_or_serve`, `annotate_cached_read`, completion hook, `repair_sweep`, `invalidate_all`, `stats` | `shepherd_utils/ars/cache.py` (orchestration) |
| index SQL, generation, hit counting, stale/purge queries, retention exemption in `purge_old_message_data` | `shepherd_utils/ars/db.py` |
| tables + marker index | `shepherd_utils/ars/schema.sql`, `shepherd_db/init_db.sql`, `shepherd_utils/db.py` |
| submit wiring, GET note, `GET /api/cache`, `POST /api/cache/invalidate` | `shepherd_server/aras/ars.py` |
| hook calls | `shepherd_utils/ars/lifecycle.py` |
| sweep call | `workers/ars_watchdog/worker.py` |
| operator CLI | `scripts/ars_cache.py` |
| tests | `tests/unit/ars/test_cache_key.py`, `test_cache_flow.py`, `test_ars_api_contract.py` |
