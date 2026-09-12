# ARS Response Cache — Design Plan

**Status:** Proposed (not yet implemented)
**Scope:** A whole-response cache in front of the ARS pipeline. A cache hit on
`POST /ars/api/submit` produces a fully completed ARS message tree (parent,
merged message, per-ARA children) without enqueuing work for any other
Shepherd worker (no fan-out, no ARA lookups, no merge, no post-process).

---

## 1. Requirements (from the request)

| # | Requirement | Design answer (section) |
|---|---|---|
| R1 | Cache the **full response**; a hit must not touch any other Shepherd worker | Snapshot of the completed tree stored in Postgres, materialized inline in `submit` (§4, §6.1) |
| R2 | Key = hash of the query graph, agnostic to key order and to null / missing / empty fields | Canonicalization + SHA-256 (§3) |
| R3 | **No TTL** on cache entries | Postgres, not Redis (§2) |
| R4 | Invalidate the **whole cache** on demand (new underlying graph release) | Cache *generation* counter; bump = invalidate all; stale rows purged lazily (§7) |
| R5 | TRAPI top-level `bypass_cache` skips the cache | No read, no write (§5) |
| R6 | `parameters.overwrite_cache` overwrites that one entry | No read, forced write (§5) |
| R7 | Two identical misses in flight → run once, answer both | Leader/follower coalescing on a `pending` cache row, resolved at parent completion (§6) |

---

## 2. Where the cache lives — recommendation: **Postgres**

The cache store is Postgres (new `ars_response_cache*` tables). Redis is
**not** used for the cache itself. Reasons, in order of weight:

1. **No-TTL data does not belong in this Redis.** `shepherd_broker/redis.conf`
   runs `maxmemory 6gb` with `maxmemory-policy volatile-ttl`. Under that
   policy only keys *with* a TTL are ever evicted. A TTL-less cache would grow
   monotonically until Redis hits `maxmemory`, at which point **every write
   fails** — the Streams broker (DB 0), the hot blob store (DB 1), locks
   (DB 2) and logs (DB 3) all share the instance. That is an outage of the
   whole platform caused by a cache. Giving the cache a TTL would violate R3;
   giving it its own Redis instance is new infrastructure for a store whose
   read pattern (one lookup per submit, then a copy into the hot path) does
   not need sub-millisecond latency.
2. **The durable-blob pattern already exists in Postgres.** Every terminal
   ARS message already gets a zstd `BYTEA` copy in `ars_message.data`
   (`persist_data_copy` / `load_message_data`, see §4.3 of the integration
   plan). The cache is the same shape — `hash → zstd bytes` — so codecs,
   pool sizing, retention tooling and operational knowledge carry over.
   TOAST comfortably handles the tens-of-MB compressed blobs merged ARS
   messages produce.
3. **Coalescing needs crash-safe state.** A leader query whose worker dies
   must not strand its followers. A transactional `pending` row with a
   `leader_pk`, repaired by the existing `ars_watchdog` sweep, is durable and
   self-healing. A Redis lock would need TTL refresh plus the same repair
   path anyway.
4. **Invalidation is one row update.** A generation counter row in Postgres
   is atomic and instant; the heavy delete happens lazily off the request
   path.

**What Redis still does on a hit:** the materialized tree's blobs are written
to Redis DB 1 through the existing `save_message_data` path (with the normal
`redis_ttl`), so the UI reads a cached parent exactly the way it reads a
fresh one. The cache is upstream of the hot path, not a replacement for it.

Alternatives considered and rejected:

- *Redis DB 4 without TTL* — rejected for reason 1.
- *Separate Redis instance* — workable, but adds a service, a second
  memory budget and a persistence story for data that must never expire;
  no latency requirement justifies it.
- *Reuse `ars_message.data` by pointing hits at an old parent pk* — the
  retention purge nulls `data` after `ars_data_retention_days`, and the
  `retain` flag is a per-tree user-facing feature; coupling cache lifetime
  to it is fragile. A dedicated table keeps lifetimes independent.

---

## 3. Cache key

`cache_key = sha256(CACHE_KEY_VERSION + "\n" + canonical_json(key_material))`

**Key material** (proposed — confirm §10 Q1):

```
{
  "query_graph": <canonical body.message.query_graph>,
  "workflow":    <canonical body.workflow, only when a non-empty list>
}
```

`workflow` is included because a non-empty workflow routes to the
`ars-workflow-agent` actor set and yields a different answer than the same
query graph on `general`. Everything else on the submit body is excluded:
`submitter`, `callback`, `log_level`, `name`, `bypass_cache`, `parameters`,
`validate`, and any pre-populated `knowledge_graph`/`results` (ARS ignores
them on submit).

**Canonicalization rules** (`shepherd_utils/ars/cache.py::canonicalize`):

1. Recursively drop any object member whose value is `null`, `{}`, `[]`.
   (This is what makes `"ids": null`, `"ids": []` and a missing `ids` hash
   identically.) Empty strings are *kept* — an empty-string id or predicate
   is a different (malformed) query, not an absent field.
2. Object keys are sorted (via `orjson.OPT_SORT_KEYS`).
3. Lists are treated as **sets**: elements are canonicalized, then sorted by
   their canonical serialization. In a TRAPI query graph every list is
   set-like (`ids`, `categories`, `predicates`, `constraints`,
   `qualifier_constraints`, `qualifier_set`, `attribute_constraints`,
   `member_ids`, pathfinder `paths[*].constraints`), so this is safe and
   makes `["A","B"]` ≡ `["B","A"]`.
4. Node/edge/path **identifiers stay literal** (`n0`, `e01`) — TRAPI
   results bind to them, so a graph with renamed ids is a different graph.
   Renaming-invariance is out of scope.
5. Booleans/numbers are left as-is (no `"true"` ≡ `true` coercion).
6. TRAPI **schema defaults are NOT folded** in v1 (`set_interpretation:
   "BATCH"`, `knowledge_type: "lookup"`). Folding them is a one-line
   addition behind `CACHE_KEY_VERSION`; listed as Q2 in §10.

`CACHE_KEY_VERSION` is a constant baked into the hash. Changing the
canonicalization rules bumps it, which orphans (and lazily purges) every old
entry without an explicit invalidation.

---

## 4. Data model (new tables, `shepherd_db/init_db.sql` + `shepherd_utils/ars/schema.sql`)

```sql
-- One row; bumped to invalidate everything. Seeded with generation 1.
CREATE TABLE IF NOT EXISTS ars_cache_meta (
  id            BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),  -- singleton
  generation    INT NOT NULL DEFAULT 1,
  bumped_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  bumped_reason TEXT
);
INSERT INTO ars_cache_meta (id) VALUES (TRUE) ON CONFLICT DO NOTHING;

-- One row per (generation, key). state: 'pending' while a leader runs the
-- query, 'ready' once the snapshot below is complete.
CREATE TABLE IF NOT EXISTS ars_response_cache (
  generation           INT  NOT NULL,
  cache_key            TEXT NOT NULL,                  -- sha256 hex
  state                TEXT NOT NULL CHECK (state IN ('pending','ready')),
  leader_pk            UUID NOT NULL REFERENCES ars_message(id),
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ready_at             TIMESTAMPTZ,
  -- snapshot of the parent's terminal fields
  parent_params        JSONB,        -- query_type, stats, ...
  merged_versions_list JSONB,        -- [[<orig merge pk>, agent], ...] kept for stats
  -- snapshot of the final merged message (merged_version target)
  merged_result_count  INT,
  merged_result_stat   JSONB,
  merged_data          BYTEA,        -- zstd, same codec as ars_message.data
  hit_count            INT NOT NULL DEFAULT 0,
  last_hit_at          TIMESTAMPTZ,
  PRIMARY KEY (generation, cache_key)
);
CREATE INDEX IF NOT EXISTS idx_ars_response_cache_leader ON ars_response_cache (leader_pk);
CREATE INDEX IF NOT EXISTS idx_ars_response_cache_state_created
  ON ars_response_cache (state, created_at);

-- Per-ARA children of the leader's tree, so ?trace=y and per-child GETs on
-- a cached parent look like a real run. Actor identity is stored by
-- (inforesid, path) rather than actor id so re-seeded registries still map.
CREATE TABLE IF NOT EXISTS ars_response_cache_child (
  generation    INT  NOT NULL,
  cache_key     TEXT NOT NULL,
  ordinal       INT  NOT NULL,       -- stable child order
  inforesid     TEXT NOT NULL,
  actor_path    TEXT NOT NULL,
  status        CHAR(1) NOT NULL,
  code          SMALLINT NOT NULL,
  url           TEXT,
  result_count  INT,
  result_stat   JSONB,
  params        JSONB,
  data          BYTEA,               -- zstd; NULL when ars_cache_store_children=false
  PRIMARY KEY (generation, cache_key, ordinal),
  FOREIGN KEY (generation, cache_key)
    REFERENCES ars_response_cache (generation, cache_key) ON DELETE CASCADE
);

-- Followers waiting on a pending leader. Resolved (materialized + deleted)
-- when the leader completes; repaired by the watchdog if the leader dies.
CREATE TABLE IF NOT EXISTS ars_response_cache_waiter (
  parent_pk   UUID PRIMARY KEY REFERENCES ars_message(id),
  leader_pk   UUID NOT NULL REFERENCES ars_message(id),
  generation  INT  NOT NULL,
  cache_key   TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ars_response_cache_waiter_leader
  ON ars_response_cache_waiter (leader_pk);
```

Deployment note: `apply_schema_upgrades` short-circuits when every *marker
index* already exists, so `idx_ars_response_cache_leader` must be added to
the marker list in `shepherd_utils/db.py` or existing volumes never receive
the new tables.

Storage estimate: a merged ARS message compresses to roughly 5–10% of its
JSON size with zstd; storing children too multiplies that by (N ARAs + 1).
`ars_cache_store_children` (default **true**, Q3) lets a deployment keep only
child *metadata* if the table grows faster than expected.

---

## 5. Request-side semantics (`POST /ars/api/submit`)

```
mode = "normal"
if body.bypass_cache is true:                 mode = "bypass"     # no read, no write
elif body.parameters.overwrite_cache is true: mode = "overwrite"  # no read, forced write
```

The three modes are deliberately distinct: `bypass_cache` (a TRAPI field
also forwarded to the ARAs, who honor it themselves) means "don't use caches
for this run" and should not leave a footprint; `overwrite_cache` exists
precisely to refresh one entry. The submit body is forwarded to ARAs
unchanged in every mode.

| mode | lookup | on `ready` hit | on `pending` hit | on miss | at completion |
|---|---|---|---|---|---|
| normal | yes | materialize inline (§6.1) | become follower (§6.2) | claim leadership, fan out | leader writes snapshot, resolves followers |
| overwrite | no | — | — | fan out (not a leader; ignores any pending row) | upsert snapshot (last write wins), resolves nobody |
| bypass | no | — | — | fan out | writes nothing |

The parent row records what happened in `params.cache` (params is already
the ARS-internal, envelope-visible bag used for `query_type`/`stats`):

```json
"cache": {"key": "<sha256>", "generation": 3, "role": "hit" | "leader" | "follower" | "overwrite" | "bypass",
          "leader_pk": "<uuid>"  /* follower only */, "served_at": "<iso>" /* hit/follower */}
```

Monitoring can count hits from this without any new plumbing. The merged
message payload itself is served **byte-for-byte** as cached; no TRAPI log
line is injected (Q4).

**Response to the client.** The envelope stays `201` with the parent row.
On a hit the parent is already `Done/200` with `merged_version` set, so the
201 body says `"status": "Done"` instead of `"Running"`. Clients that poll
`messages/<pk>?trace=y` see a complete tree on the first poll. This is a
documented deviation from upstream (which has no cache); see §9.

---

## 6. Coalescing (R7) and materialization (R1)

### 6.1 Materializing a hit — `cache.materialize(parent_pk, entry, logger)`

Called inline from `submit` (ready hit) and from the completion hook
(followers). Produces a tree indistinguishable from a real run except for
`params.cache`:

1. For each cached child (ordinal order): resolve the actor by
   `(inforesid, actor_path)`; skip with a warning if it no longer exists.
   `create_message(actor_id, status, code, ref=parent_pk, params)` then
   write the child blob **directly as bytes** to Redis DB 1
   (`data_db_client.set(pk, blob, ex=redis_ttl)`, no decompress/recompress)
   and to `ars_message.data`; `update_message` with
   `result_count/result_stat/url` (`skip_coercion=True` so a cached
   `E/598` child stays `598`).
2. Create the merge child (`ars-ars-agent`), write `merged_data` the same
   way, `update_message(status="D", code=200, result_count, result_stat)`.
3. `update_message(parent, status="D", code=200, merged_version=<new merge
   pk>, merged_versions_list=[[<new merge pk>, "ars"]], params=...)`, then
   `persist_data_copy(parent)`.
4. `notify_subscribers(parent, None)` — mirrors the parent→`D` save-time
   notification; for an inline hit there are no subscribers yet, for a
   follower there may be.
5. `UPDATE ars_response_cache SET hit_count = hit_count+1, last_hit_at = NOW()`.

Cost on the request path: one Postgres read of the snapshot, (N+1) Redis
`SET`s of already-compressed bytes, (N+3) small Postgres writes. Tens of
milliseconds for a typical tree; no CPU-heavy JSON work.

### 6.2 Leader / follower on a miss

In `submit`, after the parent row is created and its query blob saved:

```sql
INSERT INTO ars_response_cache (generation, cache_key, state, leader_pk)
VALUES (%s, %s, 'pending', %s)
ON CONFLICT (generation, cache_key) DO NOTHING
RETURNING leader_pk;
```

- Row inserted → **leader**: proceed exactly as today (`add_task("ars.fanout")`).
- Conflict → re-read the row:
  - `state='ready'` (lost a race with a completing leader) → materialize as a hit.
  - `state='pending'` → **follower**: `INSERT INTO ars_response_cache_waiter
    (parent_pk, leader_pk, generation, cache_key)`; **no** fan-out; parent
    stays `Running/202`; return the 201 envelope. Upstream parents are
    watchdog-exempt, so a follower waits indefinitely on its leader — the
    repair sweep in §6.4 bounds that.

### 6.3 Completion hook — in `lifecycle.check_parent_completion`

After the parent transitions to `D` (both the empty-merge and normal
branches), call `cache.on_parent_complete(parent_pk, role)`:

- **leader** or **overwrite**: build the snapshot from the live tree
  (`get_children` + raw blobs from Redis DB 1, falling back to
  `ars_message.data`), write `ars_response_cache_child` rows and flip the
  cache row to `ready` in one transaction. Leader writes are guarded with
  `WHERE leader_pk = %s AND state = 'pending'`; overwrite upserts
  unconditionally and sets `leader_pk` to itself.
  - **Cacheability rule (Q5):** do not cache when the merged result is the
    synthesized empty message *and* at least one ARA child ended in `E`
    (that is "we got nothing because things failed", not "the answer is
    empty"). Everything else — including partial results where some ARAs
    timed out — is cached, on the theory that the operator has
    `overwrite_cache` and whole-cache invalidation to correct it. Behind
    `ars_cache_store_partial` (default true) for deployments that would
    rather only cache all-`D` trees.
  - When the rule says "don't cache", the pending row is deleted and
    followers are **failed over** (below) rather than fed the bad answer.
- Then resolve waiters: `SELECT parent_pk FROM ars_response_cache_waiter
  WHERE leader_pk = %s`; materialize each from the fresh snapshot; delete
  the waiter row. Each follower is materialized independently so one
  failure does not block the rest; a failed one is left for the sweep.

If the parent transitions to **`E`** instead (leader failed): **fail over** —
pick the oldest waiter, `UPDATE ars_response_cache SET leader_pk = <it>`,
repoint the remaining waiters' `leader_pk`, set its `params.cache.role` to
`leader`, and `add_task("ars.fanout")` for it using its own stored query
blob. No waiters → delete the pending row. The failed leader's parent stays
`E` for its own client, as today.

### 6.4 Repair sweep — `ars_watchdog`

Added to the existing 60 s loop, so no new worker or stream:

1. `pending` rows older than `ars_cache_pending_max_sec` (default 1200 s,
   comfortably above the 5/8-minute child timeouts plus merge time) whose
   leader parent is terminal or missing → run the §6.3 logic for that
   leader (snapshot if `D`, fail over if `E`/missing).
2. Waiters whose leader is `D` and cache row is `ready` but who are still
   `R` (a materialization crashed mid-way) → materialize again. Any
   half-created children from the crashed attempt are deleted first
   (`DELETE FROM ars_message WHERE ref = waiter_pk`), making the step
   idempotent.
3. Purge: `DELETE FROM ars_response_cache WHERE generation < <current>
   AND created_at < NOW() - ars_cache_stale_grace_sec` (default 86400 s),
   in batches, so an invalidation never runs a giant delete inline.
   Children cascade.

---

## 7. Whole-cache invalidation (R4)

`UPDATE ars_cache_meta SET generation = generation + 1, bumped_at = NOW(),
bumped_reason = %s`. Every lookup filters on the current generation, so the
bump is instant and atomic; old rows are unreadable immediately and deleted
lazily (§6.4.3). In-flight leaders finish and store under the generation
they claimed — harmless, they age out.

Two front doors, both calling the same `cache.invalidate_all(reason)`:

1. **`scripts/ars_cache.py`** — `invalidate --reason "KG 2.10 release"`,
   `stats` (generation, entries, bytes, hits), `show <key>`, `evict <key>`.
   Talks to Postgres directly with the normal settings; the operator-facing
   primary path (matches how `scripts/` is used today), no auth story.
2. **`POST /ars/api/cache/invalidate`** and **`GET /ars/api/cache/`** on the
   sub-app for automation. The ARS upstream has no admin auth at all
   (`agents` POST is open), so these routes are gated on a new
   `ars_admin_token` setting checked as `Authorization: Bearer …`; with the
   setting empty the routes answer `403` (disabled by default). Q6.

The generation is read once per submit (one indexed singleton read; can be
folded into the lookup query). `parameters.overwrite_cache` covers single
entries; `scripts/ars_cache.py evict <key>` covers ad-hoc single removal.

---

## 8. Settings (`shepherd_utils/config.py`)

| setting | default | purpose |
|---|---|---|
| `ars_cache_enabled` | `True` | master switch; off = every submit behaves as `bypass` |
| `ars_cache_store_children` | `True` | store per-ARA child blobs (vs metadata only) |
| `ars_cache_store_partial` | `True` | cache trees with some `E` children |
| `ars_cache_pending_max_sec` | `1200.0` | watchdog repair threshold for stuck leaders |
| `ars_cache_stale_grace_sec` | `86400.0` | how long superseded generations linger before purge |
| `ars_admin_token` | `""` | bearer token for the cache admin routes; empty disables them |

---

## 9. Parity register entry

Add to `docs/ARS_PARITY_REGISTER.md` → *Behavioral deviations*:

> **Response cache** (Shepherd-native; upstream has none). With
> `ars_cache_enabled`, a submit whose canonical query graph (+workflow)
> matches a completed prior submit is answered from a Postgres snapshot of
> that tree: the `201` envelope already reads `Done/200` with
> `merged_version` set, and the trace tree carries copied children whose
> `updated_at` is the materialization time. Identical in-flight submits
> coalesce onto one run. Opt out per query with TRAPI `bypass_cache`;
> refresh one entry with `parameters.overwrite_cache`; flush all with the
> generation bump. `params.cache` on the parent records the role.

---

## 10. Decisions to confirm before implementation

| # | Question | Proposed default |
|---|---|---|
| Q1 | Key material = `query_graph` **+ `workflow`**? (Pure query-graph keying would return a `general`-channel answer to a workflow-routed query.) | Include `workflow` when non-empty |
| Q2 | Fold TRAPI schema defaults (`set_interpretation: "BATCH"`, `knowledge_type: "lookup"`) into canonicalization so explicit-default ≡ missing? | Not in v1; trivial to add behind `CACHE_KEY_VERSION` |
| Q3 | Store per-ARA child **blobs** (full-fidelity trace + per-child GET) or metadata only (much smaller)? | Blobs, behind `ars_cache_store_children` |
| Q4 | Serve cached merged message byte-for-byte, or append a TRAPI `logs` entry noting the cache hit? | Byte-for-byte; provenance in `params.cache` |
| Q5 | Cache trees where some ARAs errored/timed out? | Yes, except the "empty because everything failed" case; `ars_cache_store_partial` to tighten |
| Q6 | HTTP invalidation route gated by a bearer token setting, or script-only? | Both; route disabled unless `ars_admin_token` is set |
| Q7 | Should the `201` on a hit report `Done`, or stay `Running` and materialize a moment later via a task for wire-shape parity? | Report `Done` (simpler, truly worker-free; documented deviation) |

---

## 11. Implementation outline

Files, in dependency order:

1. `shepherd_utils/ars/schema.sql`, `shepherd_db/init_db.sql`,
   `shepherd_utils/db.py` (marker index) — tables from §4.
2. `shepherd_utils/config.py` — settings from §8.
3. `shepherd_utils/ars/cache.py` (new) — `canonicalize`, `cache_key`,
   `resolve_mode(body)`, `lookup_or_claim`, `register_waiter`,
   `snapshot_tree`, `materialize`, `on_parent_complete`, `fail_over`,
   `invalidate_all`, `stats`, `repair_sweep`. SQL helpers colocated in
   `shepherd_utils/ars/db.py` following its conventions.
4. `shepherd_server/aras/ars.py` — `submit` branches per §5/§6.2; new
   `GET /api/cache/`, `POST /api/cache/invalidate` per §7.
5. `shepherd_utils/ars/lifecycle.py` — call `cache.on_parent_complete` in
   both `D` branches and the `E` branch of `check_parent_completion`.
6. `workers/ars_watchdog/worker.py` — call `cache.repair_sweep` each tick.
7. `scripts/ars_cache.py` — operator CLI.
8. Docs: this file's status → Implemented; register entry (§9); README
   section on `bypass_cache` / `overwrite_cache` / invalidation.

Tests (`tests/unit/ars/test_cache*.py`, existing fakeredis + `ars_db`
AsyncMock conventions):

- **Key**: property-style permutations — reordered keys, reordered
  `ids`/`categories`/`predicates`, `null` vs missing vs `[]`/`{}` all hash
  equal; renamed node ids, different curies, added constraint, added
  workflow all hash differently; `CACHE_KEY_VERSION` bump changes every hash.
- **Submit contract**: miss → leader claims + fanout enqueued; ready hit →
  no fanout, tree materialized, envelope `Done`, `params.cache.role=hit`,
  hit_count incremented; pending hit → waiter row, no fanout, `Running`;
  `bypass_cache` → no lookup, no claim; `overwrite_cache` → no lookup,
  fanout, `role=overwrite`; `ars_cache_enabled=false` → bypass everywhere.
- **Completion hook**: leader `D` → snapshot rows written (children +
  merged), waiters materialized and deleted, subscribers notified; leader
  `E` → oldest waiter promoted and fanned out, others repointed; empty-merge
  with an `E` child → not cached, followers failed over; overwrite
  completion replaces a `ready` row.
- **Materialize**: copied children resolve actors by `(inforesid, path)`,
  cached `E/598` child keeps `598` (skip_coercion), missing actor skipped
  with a warning, blobs land in Redis DB 1 and `ars_message.data`.
- **Watchdog repair**: stale pending with terminal leader → resolved;
  stuck `R` follower with ready row → re-materialized idempotently; stale
  generations purged after the grace window and not before.
- **Invalidation**: generation bump makes a ready row a miss; route `403`
  without token, `200` with; script `stats`/`invalidate` smoke test.
- **Parity harness**: one new scenario asserting a second identical submit
  produces an equivalent terminal tree (normalized) with no stub-ARA
  requests in the mockworld journal.
