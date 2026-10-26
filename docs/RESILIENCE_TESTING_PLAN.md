# Shepherd Translator-Services Resilience Test Plan

**Scope: Test environment only. Never Prod.** This is a self-inflicted robustness
exercise against our own ARA front door to find and close gaps before a real
bad actor finds them. Everything below is abuse we expect to see "in the wild"
(malformed TRAPI, resource exhaustion, enumeration, SSRF) thrown at endpoints we
own.

## 0. Rules of engagement (bake these into the harness)

- **Target allowlist.** The harness refuses to run unless `--base-url` resolves to a
  known Test host. Hard-code a denylist for Prod hostnames; abort on match.
  Require an explicit `--i-understand-this-is-test` flag.
- **No third-party blast radius.** The one genuinely dangerous category here is the
  attacker-controlled `callback` URL (SSRF / reflected amplification). Point those
  only at a sink we run (a local listener) or at RFC-5737 documentation IPs — never
  at a real third party, and never at a cloud metadata address from a shared runner.
- **Bounded by default.** Rate/volume caps are flags with conservative defaults so a
  "quick check" can't accidentally become a real DoS of our own Test cluster.
- **Observability on.** Before each run, confirm we can see: the monitor worker,
  OTEL traces, server `EVENT_LOOP_LAG` / `PG_SLOW` logs, `docker stats` (CPU/RSS vs
  the `compose.test.yml` limits), Redis stream depth, and Postgres pool/connection
  counts. The point is not "did it 500" — it's "what did it cost us and did we
  recover."
- **One variable at a time, then combine.** Run each category in isolation first
  (clean signal), then layer the nastiest combinations.

## 1. Bring up Test the way Prod behaves

`compose.test.yml` exists precisely for this — it applies the cgroup CPU quota and
`mem_limit`s so pool sizing, OOM-kill, and recovery actually reproduce locally:

```
docker compose -f compose.yml -f compose.test.yml up --build
```

Confirm the "process pool sized to N worker(s)" startup logs match the `cpus:` in
the overlay, and note the deployed values of the two knobs that default to
*disabled* (`"0"`): `callback_max_request_size` and `max_response_size`. If Test
runs them at 0, that is itself finding #1 (see §3.C).

## 2. Harness design

A single Python driver (`scripts/resilience_probe.py`), modeled on the existing
`scripts/run_query.py` conventions, with:

- a registry of **probe cases**, each: `{id, category, endpoint, build_request(), expected,
  signal}` where `expected` is the guard we believe exists (e.g. "400 naming the field",
  "413", "504 timeout, next query OK") and `signal` is what to watch beyond the HTTP code.
- a `--only <category>` / `--case <id>` selector, `--rate`, `--concurrency`,
  `--duration`, `--dry-run` (print the request, send nothing).
- per-case result: HTTP code, latency, body snippet, and a PASS/FAIL against
  `expected` — plus a reminder to eyeball the `signal` (logs/stats), since many of
  these fail *quietly* (recovered but at cost) rather than with a bad status code.
- a small embedded callback listener for the async/SSRF cases that records every hit.

## 3. Query-type catalog

Grouped by failure mode. Each case names the endpoint, the guard already in the code
(so we're testing whether it holds), the hypothesis, and the pass/fail signal.

### A. Malformed / schema-abuse requests (does validation hold and fail cheaply?)

Front door: `POST /{ara}/query` and `/asyncquery`.

1. **Not JSON / wrong content-type** — body `=====`, `Content-Type: text/plain`,
   form-encoded, a bare JSON array, a JSON string, `null`. Expect **422** (`QueryBodyError`),
   not 500. Confirms `_is_json_content_type` + `parse_query_body` object check.
2. **Valid JSON, invalid TRAPI** — missing `message`, `message` not an object, empty
   query graph, node referenced by an edge that doesn't exist, `edges`/`nodes` as lists.
   Expect **400** naming the field (`validate_query` / TOM). Watch for any case that
   slips through to a worker 500 instead.
3. **Retired 1.x spellings** — `qualifier_constraints`, `attribute_constraints`,
   `intermediate_categories`, `log_level`/`bypass_cache`/`timeout` at top level. Expect
   **400 naming the 2.0 replacement** (`_reject_retired_fields`). This is a known
   contract; verify it still bites.
4. **Type-confusion on every field** — `ids` as a string not a list, `predicates` as an
   int, `categories: {}`, `parameters: []`, `workflow: {}`, `message: 42`. The goal is to
   find any field whose wrong type reaches `.get(...)`/iteration downstream and throws an
   unhandled `TypeError` (worker crash) instead of a clean 400.
5. **Hostile `workflow`** — valid `[]`; one unsupported op (expect `KeyError`→clean
   reject); a **very long** workflow list (10k ops) to see whether it's rejected before or
   after we iterate/validate the whole thing; operations that are non-dicts; duplicate ops.
6. **Weird-but-legal values** — `parameters.log_level` = `"TRACE"`/garbage/number
   (should fall back, not fail), huge `parameters.timeout` (see §C.3), unicode and
   control chars in curies, extremely long curie strings, `ids` list with 100k entries.

### B. Oversized / structural resource abuse (parse cost, memory, recursion)

7. **Giant query body** — a syntactically valid query of 50MB / 200MB / 1GB. **Key
   hypothesis:** `parse_query_body` does `await request.body()` with **no size cap** (the
   `_read_body_within_limit` guard is only wired to `/callback`), then parses synchronously
   with orjson — the docstring itself says this "stalls every other in-flight request for
   its duration." Signal: `EVENT_LOOP_LAG` spikes, other concurrent `/query` latencies
   climb, possible uvicorn worker SIGKILL. Likely finding → needs a request-body cap on the
   query path.
8. **Deeply nested JSON** — 10k levels of nested objects/arrays, to probe parser recursion
   limits and TOM validation cost. Expect a clean 400/422; watch for a crash or a stack
   blow-up.
9. **Pathologically wide query graph** — tens of thousands of nodes/edges, each with long
   `ids`/`predicates` lists. Valid TRAPI, but how much does validation + enqueue cost, and
   what does it do to the workers that expand it?
10. **Compression tricks on the query path** — `Content-Encoding: zstd`/`gzip` with a
    decompression-bomb body; lying `Content-Length` (too small, too large, absent with a
    huge stream). The callback path has a zstd-bomb guard (`DecompressedTooLargeError`);
    confirm whether the *query* path has any equivalent.

### C. Resource / lifecycle exhaustion (slow, not malformed)

11. **Oversized callback** — `POST /{ara}/callback/{id}` with a body over
    `callback_max_request_size`, both raw and zstd-encoded. Expect **413** and the whole
    query failed as `RESPONSE_TOO_LARGE`. **But** if Test has the knob at its default `"0"`
    (disabled), there is no cap at all → finding. Also a `zstd` bomb → expect 413 via the
    decompress guard.
12. **Connection-holding sync queries** — many concurrent `/query` calls each with a large
    `parameters.timeout`, clients that then just sit there. Each holds an HTTP connection and
    polls Postgres every 0.5s from the pool. Hypothesis: enough of them exhaust the PG pool →
    intake starts answering **503**. The disconnect case is partly mitigated
    (`wait_for_query` bails on `request.is_disconnected()`); verify the *connected* case and
    find the concurrency where it tips.
13. **Slowloris / slow body** — open the connection, send the body a byte at a time, never
    finish. Tie up server workers. Confirm uvicorn/ingress timeouts actually reap these.
14. **Queue flooding** — fire thousands of cheap `/asyncquery` jobs as fast as allowed.
    Watch Redis stream depth, broker memory (the 25g `shepherd_broker` limit), and whether
    intake back-pressures or just keeps enqueuing unboundedly.
15. **Pool-timeout / OOM triggers** — replay a known big/slow query with
    `POOL_TASK_TIMEOUT_SEC` lowered: expect "task exceeded …; killing the stuck child",
    that query → finish_query ERROR, and **the next task still succeeds**. A query crafted to
    balloon a child past `mem_limit` → cgroup OOM-kill, parent survives. These exercise the
    recovery paths the overlay comments describe.

### G. Overly broad / high-cardinality queries (valid, but enormous)

Front door: `POST /{ara}/query`. These are perfectly valid TRAPI 2.0 — nothing a
schema check would reject — but each asks for a huge slice of the knowledge graph,
so the cost is paid downstream (lookup fan-out, KG merge, scoring) rather than at
intake. The HTTP status is a weak signal here; watch response size, scoring/merge
worker RSS and time, pool timeouts, and `lookup_timeout` (210s).

G1. **Ontology-root curie** — subject pinned to `MONDO:0000001` ("disease or
    disorder", the MONDO root) with a broad predicate. This is the example from the
    weekend: a single legitimate-looking curie that fans out to a large fraction of
    the disease graph. Expect either a bounded answer, a `RESPONSE_TOO_LARGE`
    failure, or a timeout — **and confirm a normal query still succeeds right after**.
G2. **Fully unconstrained graph** — both nodes id-less (`categories` only), joined
    by `biolink:related_to` (the catch-all predicate). No anchor at all → maximal
    fan-out. Is there any guard against a query graph with no pinned ids?
G3. **Top-of-hierarchy category** — object category `biolink:NamedThing` (matches
    everything), or other high-level roots (`biolink:ChemicalEntity`,
    `biolink:BiologicalProcess`).
G4. **Super-high-degree node** — e.g. `NCBITaxon:9606` (human) or a hub gene/chemical,
    where "everything connects to this" blows up the result set.

**Why this matters / likely finding:** `max_response_size` defaults to `"0"`
(disabled). If Test runs the default, an overbroad query can build a response large
enough to blow a worker's memory limit before the `RESPONSE_TOO_LARGE` pre-flight
guard would catch it — and even when bounded, one such query can monopolize the
scoring pool. Mitigations to weigh: a cap on unpinned nodes / result cardinality at
intake, a sensible default `max_response_size`, and per-query cost estimation.

### H. Topological abuse (valid graph, explosive structure)

Front door: `POST /{ara}/query`. The query graph is schema-valid; its *shape* is
what forces combinatorial work. Cost lands in the pipeline (lookup fan-out,
intermediate result counts, scoring, pathfinder search), so again the HTTP code
barely matters. The open question behind every case: **is there any limit at intake
on hops, edge count, or node degree?**

H1. **Long n-hop chain** — `n0 -> n1 -> ... -> n10`, only the first node pinned,
    every intermediate unpinned. Each hop multiplies the candidate set against the
    last, so a 10-hop chain is combinatorial explosion. This is the "crazy query"
    example. Watch intermediate result growth hop-over-hop and `lookup_timeout` (210s).
H2. **Cycle** — edges form a loop (`n0->n1->n2->n3->n0`). No topological order, so a
    traverser that assumes a DAG can loop or wedge → watch for the loop-stall watchdog
    (`WORKER_LOOP_STALL_EXIT_SEC`) firing or a hung worker.
H3. **Near-complete graph (clique)** — an edge between every pair of N nodes, so edge
    count grows as N²/2 from a tiny body → expensive joint constraint solve.
H4. **Star / high-degree** — one pinned hub joined to many unpinned leaves; large
    single-node fan-out.
H5. **Self-loop** — an edge whose subject == object. Degenerate; confirm it's handled
    and doesn't become an unbounded self-expand.
H6. **Broad pathfinder** — a query graph carrying `paths` (not `edges`) between two
    very general pinned nodes (MONDO root ↔ human taxon). The ARS routes `paths`
    queries to the pathfinder workers; broad endpoints make the connecting-path search
    huge. Watch `arax_pathfinder` / `aragorn_pathfinder` RSS (22g / 5g limits) and
    their timeouts.

**Likely finding:** multi-hop depth and node degree look unbounded at intake — the
only backstops are the downstream pool/response/timeout guards, which cost a full
expensive run (and possibly an OOM) before they bite. Mitigations to weigh: a max-hop
and max-edge cap at intake, a max unpinned-node count, and a per-query complexity
budget rejected cheaply up front.

### I. Cache abuse (defeating the response cache on purpose)

Front door: `POST /{ara}/query` (and the ARS `/ars/api/submit`). The ARS caches a
response under a canonical query-graph hash; `parameters.bypass_cache=true` means
"no read, no write."

I1. **bypass_cache hammer** — fire one *expensive* (overbroad) query repeatedly with
    `bypass_cache=true`. The cache that should absorb repeats is defeated on purpose, so
    every shot does full work → amplification against our own backend.
I2. **Cached-repeat baseline** — the same expensive query repeated *without*
    `bypass_cache`, run sequentially. Repeats after the first should be fast cache hits;
    compare their latencies to I1. If they're just as slow, the cache isn't absorbing
    repeats — a finding in itself. (The harness prints min/med/max latency per batch so
    the comparison is direct.)

**Mitigation angle:** should `bypass_cache` be rate-limited, or ignored, for
unauthenticated callers? A cheap flag that forces maximum backend cost is a gift to an
abuser.

### J. Constraint stacking

Front door: `POST /{ara}/query`.

J1. **Stacked qualifier constraints** — one edge whose `constraints.qualifiers` is a
    very long list of `{qualifier_type_id: qualifier_value}` mappings. Valid-looking, but
    an expensive constraint set to solve and to echo back on every result edge. Is the
    qualifier-list length bounded anywhere?

### K. Hostile identifier strings (normalizer / injection edge cases)

Front door: `POST /{ara}/query`. Cheap probes (intake / node-normalizer level); the
pass criterion is a clean reject or safe normalization, never a crash or downstream
injection.

K1. **Unicode homoglyphs** — a curie with Cyrillic lookalike characters
    (`NCBIGene:аео`). Must not be treated as a real curie or break the normalizer.
K2. **Control / NUL characters** — a curie containing `\x00\x01…`. No crash, no
    injection into logs or downstream HTTP.
K3. **Newline injection** — a curie with an embedded newline, to confirm it can't forge
    a second line in our logs (log-injection).
K4. **Overlong curie** — a single ~100 KB curie string. Bounded, or carried untouched
    through the whole pipeline?

### D. Enumeration / IDOR / forgery (authz and identifier entropy)

16. **Response enumeration** — `GET /{ara}/response/{id}` and `/asyncquery_status/{id}`
    across guessed ids. `query_id`/`response_id` are `uuid4()[:8]` = **32 bits**, and these
    GETs appear unauthenticated. Hypothesis: another tenant's response/logs are readable by
    guessing/brute-forcing 8 hex chars. Measure hit rate at volume → finding on identifier
    length and/or authz.
17. **Forged callbacks** — `POST /{ara}/callback/{guessed_id}` with attacker-chosen TRAPI.
    Same 32-bit `callback_id` space. Hypothesis: a guessed/collided id lets an attacker inject
    fabricated results into someone else's in-flight query, or flood merge with junk callbacks.
    Confirm whether anything authenticates the callback sender.
18. **Callback replay / duplication** — replay a captured valid callback many times; send
    callbacks for an already-completed query. Confirm idempotency and that it can't corrupt a
    merged response.

### E. SSRF / outbound abuse (the attacker-controlled callback URL)

19. **SSRF via `asyncquery.callback`** — the async path takes any `callback` URL and
    `finish_query` POSTs the finished response to it. Point it (in Test, at our own sink or a
    doc-range IP) at: `http://169.254.169.254/…` (metadata), `http://localhost:<internal>`,
    `file://`, a non-HTTP scheme, a redirect chain. Hypothesis: Shepherd will POST wherever told
    → classic SSRF / internal-port scanner / reflected amplifier. Confirm what URL validation
    (scheme/host allowlist, no link-local) exists. **This is the highest-value case — treat its
    output as a probable Prod-blocking finding.**
20. **Callback amplification** — small async request, large generated response delivered to a
    callback we control, to measure the request→delivered-bytes amplification factor an attacker
    could aim at a victim.

### F. Protocol / surface hygiene (quick wins)

21. **CORS** — server sets `allow_origins=["*"]` with `allow_credentials=True`. Not a
    "query," but verify what that exposes; note for review.
22. **Methods / unknown routes** — `PUT`/`DELETE`/`TRACE` on query routes, unknown
    `/{ara}` targets, `/docs` and `/openapi.json` reachability, oversized/odd headers.
23. **ARS port** — repeat the A/B/C cases against `/ars/api/submit` and its status/trace
    endpoints; the fan-out enqueues one query per hosted ARA with no HTTP hop, so an abusive
    ARS submit is an amplifier into the broker.

## 4. How we read results → mitigations

For each case record: HTTP code vs. `expected`; latency; whether **the next, well-formed
query succeeded** (recovery is the real test); and the `signal` (lag/pool/stream/RSS).
Triage into: (a) clean reject already — keep as a regression test; (b) recovered but
expensive — add a cap/limit/back-pressure; (c) wrong/500/crash/leak — bug to fix before
Prod. Likely early findings from code reading: **no body-size cap on the query path (§B.7)**,
**disabled-by-default size knobs in Test (§C.11)**, **short 32-bit ids with unauthenticated
read/callback (§D)**, and **callback-URL SSRF (§E.19)**.

## 5. Sequencing

1. Stand up Test with the overlay; confirm observability.
2. Run A (malformed) and K (hostile identifiers) — fast, safe, mostly
   expected-pass; fix any 500s / crashes / injections first.
3. Run D + E at low volume (enumeration, SSRF) — cheap requests, high-value findings.
4. Run B (oversized/structural) single-shot, watching the event loop.
5. Run the expensive-pipeline categories last and most carefully, with caps,
   watching recovery — C (exhaustion), G (overbroad), H (topology), I (cache abuse),
   J (constraint stacking). A single overbroad / deep-multi-hop / bypass-cache query
   can tie up the scoring or pathfinder pool, so run them one at a time
   (`--concurrency 1`) and confirm the next normal query still succeeds. For I,
   run `cache.repeat-cached` and `cache.bypass-hammer` back to back and compare the
   per-batch latency spread.
6. Combine the worst (e.g. oversized bodies under concurrency) only once each is understood.
7. File findings; add the clean-reject cases to the suite as regression tests.
