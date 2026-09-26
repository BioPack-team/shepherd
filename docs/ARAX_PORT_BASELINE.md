# ARAX Port Baseline: Functionality Inventory

This is the full list of what the ARAX reasoner does today. It is the baseline
for a faithful port of ARAX into Shepherd. Every item has a stable ID (e.g.
`EXP-07`), so the ARAX team and Shepherd developers can refer to it in reviews,
issues and a future parity register (like `ARS_PARITY_REGISTER.md`).

- **Upstream:** [RTXteam/RTX](https://github.com/RTXteam/RTX), `master` at
  `9485431` (2026-09-21).
- **Shepherd:** inventoried at `main` `e3bc720`; the Shepherd columns reflect the port
  on branch `claude/optimistic-gauss-bjtrzh`.
- **Method:** static reading of the code. Nothing was executed against a live
  ARAX. SmartAPI and `kg2webhost.rtx.ai` could not be reached from the
  analysis environment, so the live KP roster (EXP-30) is inferred from code.
- **Paths:** relative to `RTX/code/`, and `AQ/` means `RTX/code/ARAX/ARAXQuery/`,
  unless a path starts with `shepherd/`.

## Decisions

Recorded 2026-09-24. Where this section conflicts with the inventory below,
**this section wins**. The inventory stays as the record of what upstream ARAX
does.

| ID | Decision | Inventory items affected |
|---|---|---|
| DEC-1 | **Perfect parity by default.** Everything is ported to behave exactly like ARAX, bugs and oddities included, unless a decision here says otherwise. The plan is to finish the port and validate it against ARAX first, and only then fix bugs as a separate, later pass. | All. Every Part D defect is **reproduced** in the port. Part C items are only fixed where another decision says so (DEC-8). |
| DEC-2 | **The ARAX UI itself stays external to Shepherd**, at least for now. **But Shepherd provides everything the ARAX UI needs, so the UI can keep working when pointed at Shepherd.** | AUX-03 (the UI code) is not hosted. The endpoints and behaviors the UI uses become required; see [UI contract](#ui-contract-what-the-arax-ui-requires). |
| DEC-3 | **ARAX's MySQL/S3 response store is replaced by Shepherd's Postgres, but `/response/{id}` does everything it does in ARAX.** (DEC-3 first also left out the KP response cache; DEC-18 supersedes that part.) | Not ported: the MySQL + S3 write path in OPS-05. **Ported in full, reading responses from Postgres:** API-07 / OPS-06 (validation with `reasoner-validator` and ARAX's pinned versions, `validation_result`, `provenance_summary`, the ARS PK/UUID paths (read from **Shepherd's own hosted ARS in Postgres**, not from the remote ARS prod/test/ci/dev instances), `X` attribute stripping with `detail_lookup`, `Z` cache reads, `n_nodes`/`n_edges`/size figures, error tuples) and API-08 (`POST /response` callback sink). `envelope.id` becomes **`{settings.server_url}/arax/response/{id}`**, the Shepherd endpoint that serves the response (the ARAX app is mounted at `/arax`, `shepherd_server/server.py:54`). The same applies to the stored-response URL in ARAX's log line and in `response=false` returns. |
| DEC-4 | **Queries use only Retriever; SmartAPI is kept only for the UI.** During query processing, ARAX never touches SmartAPI / KP-registry / meta-KG KP selection: every Expand query goes to Retriever. Retriever itself queries Gandalf, so ARAX's rtx-kg2 (Gandalf) and `infores:gandalf` routes are covered by Retriever too. SmartAPI stays available for the UI-facing surfaces. | Not used in queries: EXP-07 (blocked KPs), EXP-25 (meta-KG KP selection), EXP-30 (dynamic roster). EXP-29 (SmartAPI client) is kept for the UI only: `/status?authorization=smartapi` (API-09). The rtx-kg2-specific paths (the 600 s timeout in EXP-04, `return_minimal_metadata` in EXP-27, single-node queries in EXP-21) and FET's `rel_edge_key` Gandalf query (OVL-05) go to Retriever. D-3 and D-4 do not apply to queries. |
| DEC-5 | **All dead code is removed.** Nothing in Part E is ported, and no further confirmation is needed. | Part E. |
| DEC-6 | **Each data file is set up exactly like the pathfinder DBs are today:** a settings-driven URL and tier version, a presence check against the volume-mounted directory, download on worker startup via `shepherd_utils/data_download.py`, and a compose volume mount. URLs are placeholders following the `kg2webhost.rtx.ai/tier0` pattern, to be adjusted once the real files are available. **The COHD DB to use is `COHDdatabase_v1.0_KG2.8.0.db`**, as in ARAX. **Implemented:** `ensure_arax_dbs` / `arax_db_path` in `shepherd_utils/data_download.py`, with `ARAX_DBS_DIR`, `ARAX_TIER_VERSION`, `ARAX_DBS_BASE_URL`, and a per-file URL override and filename setting for each file. `curie_ngd` and `tier0-info-for-overlay` reuse the pathfinder downloads (`arax_pathfinder_sqlite_paths()`). The workers call it once they exist; compose volume mounts (`./arax_dbs`) are added with them. | Part B §20 (curie_to_pmids, ExplainableDTD, COHD, FDA pickle, autocomplete), OPS-08. |
| DEC-7 | **The ARAX pathfinder is obsolete and is dropped. Shepherd's current `arax_pathfinder` worker is kept as it is.** | CON-01…CON-05 are not parity targets. QGI-01 / QGI-03 route to Shepherd's `arax.pathfinder`. C-1 and C-3…C-7 are no longer parity gaps (under DEC-1 they can be revisited in the post-validation bug-fix pass). The catrax-pathfinder pin difference no longer matters. The UI's `max_pathfinder_paths` / `max_path_length` query options are **accepted as ignored** (C-4 stays). |
| DEC-8 | **Ranker: bring Shepherd's `arax_rank` up to exactly the ARAX version.** | RNK-01…RNK-06; C-2 and C-10 become port work items, including ARAX's quirks per DEC-1 (the `"no value!"`→0 mutation, falsy-zero min test, the `confidence`-attribute override, `row_data`/`table_column_names`, no `edge["confidence"]` in the output, and ARAX's error behavior rather than soft-failing). D-20 is reproduced. Ranking must also run at the ARAX point in the pipeline (RNK-06 / ORC-09). **Implemented:** `workers/arax_rank/ranker.py` is now a function-by-function port. The worker propagates errors instead of returning the message unranked. It was checked against the real ARAX ranker (ARAX's pinned numpy/scipy/networkx) on 3000 generated envelopes: identical order, scores, `row_data`, `table_column_names`, edge mutations, log messages and raised exception types. Regression tests are in `tests/unit/test_arax_rank.py`. The ported `ARAX_ranker.py` now also runs inside the in-process pipeline at the RNK-06 / ORC-09 point (§2a); `arax.rank` stays available as a workflow step. |
| DEC-9 | **No curie-prefix conversion.** All data is already normalized, and incoming queries are assumed to be normalized and matching too. | EXP-26 (KP-supported prefix conversion) is not ported. |
| DEC-10 | **A `kp` list is forwarded to Retriever as `parameters.kp`** (a forward-looking deviation from DEC-1). The intended meaning of `kp` is to filter returned edges to those whose `sources` list the named infores. ARAX does not do that: a user-specified `kp` only chooses which endpoint(s) are queried (`ARAX_expander.py:431-435`). In the port, the query still only goes to Retriever, and the `kp` list (from `expand(kp=…)`, or from the `fill` allowlist via WF-03) is sent in the Retriever request body as `parameters.kp`, alongside `parameters.tiers`, so Retriever can apply the filtering. The value is the list of infores exactly as given (no reformatting). Retriever decides which values are valid, so ARAX's `valid_kps` / `InvalidKP` check is dropped. With a user-specified `kp`, the knowledge-source constraint allowlist/denylist and the blocked-KP list are still skipped, as in ARAX (DEC-1). (An updated TRAPI schema may later formalize this filtering.) | EXP-01, WF-03 and EXP-27 (request body). |
| DEC-12 | **Query-plan entries (for the UI): ARAX reads the provider list from SmartAPI and always marks every provider except Retriever "Skipped", whether or not a `kp` list was given.** Retriever carries the real status (Waiting / Done / Timed out / Error / Warning). This keeps the UI's per-provider progress display meaningful now that only Retriever is queried. | EXP-32, API-02, and the query-plan parts of EXP-07, EXP-16, EXP-25 and EXP-01. The provider list is read from SmartAPI, which is otherwise kept only for the UI (DEC-4). This is a query-plan display read only: it never decides which provider is queried. |
| DEC-13 | **`biolink-helper-pkg` is bumped to 1.0.1 to match ARAX.** Done in `workers/arax_pathfinder/requirements.txt`. The only upstream change is a fix inside `get_predicate_depth_map`, which Shepherd does not call. | BL-01. |
| DEC-14 | **One in-process ARAX worker.** The ARAX plan (interpreter, ARAXi actions, auto-rank, ResultTransformer) runs in a single worker process, as it does in ARAX, calling the ported modules as a library. ARAX's hidden per-query state therefore stays in memory: KG `qnode_keys`/`qedge_keys`, qedge `filled`, the original QG, excluded-edge info, the query plan and the log. The library is `shepherd_utils/arax/`. It runs on ARAX's own OpenAPI-generated TRAPI model classes, vendored unchanged except for their import paths (`shepherd_utils/arax/openapi_server/`, pinned to `9485431`), so ARAX's in-memory attributes, validation and serialization carry over. Ported modules keep ARAX's filenames so they can be diffed against upstream. CPU-heavy parts may still use a process pool. The standalone `arax.rank` and `arax.pathfinder` workers stay available as workflow steps. | All ARAX modules. Resultify is ported first because Expand calls it after every qedge (EXP-19) and when pruning (EXP-13). |
| DEC-11 | **`/meta_knowledge_graph` is built from Retriever's metadata** (Retriever's own `/meta_knowledge_graph`), not from Plover plus SmartAPI-derived KP meta maps. **ARAX's own additions are included**: the `knowledge_types` and `attributes` fill-in, the standard attribute constraints (`original_predicate`, `knowledge_level`, `agent_type`), the `format=simple` view (predicates by categories), the 1 h cache and hourly background refresh, and the JSON backups (keeps 3) with fallback to the newest backup. | AUX-01 (and API-05): only the Plover fetch and the KPInfoCacher merge are replaced. SmartAPI (DEC-4) is then only used by the UI's `/status?authorization=smartapi` view. |
| DEC-15 | **`/arax/asyncquery` and `/arax/asyncquery_status` use Shepherd's server logic**, the same as every other ARA, not ARAX's: Shepherd replies with its own `job_id` and delivers the result to the callback itself. | API-03, API-04 (and the `asynchronous` mode in ORC-13, which Shepherd never uses). D-18's missing `job_id` does not apply. |
| DEC-16 | **No concurrency limit.** ARAX's per-address cap and free-RAM floor (429 `OverLimit`) are not implemented. | ORC-14; the 429 in API-01. |
| DEC-17 | **An ARAX query's TRAPI `workflow` is passed through to ARAX unchecked.** Shepherd's server checks workflow operations against its own list for the other ARAs; for ARAX, the workflow stays in the stored query and the task starts with no Shepherd workflow, so ARAX translates and validates it itself (`operation_to_ARAXi`, answering `NotImplementedError` for an operation it doesn't know). | WF-*, ORC-03. `shepherd_server/base_routes.py` (`run_query`). |
| DEC-18 | **ARAX's KP response cache is ported, stored in Shepherd's Redis data store.** `KPQueryCacher` keeps upstream's logic: the sha256 key of `{url, body}` (categories sorted, in place), what is cached (every Expand KP response, timeouts as `-1`; Connect's PathFinder and xCRG results; the xDTD result, whose read stays disabled), hits and misses, `bypass_cache` (EXP-06, ORC-04), the record fields and statistics, and the `/status?mode=kp_cache` listing. The arax worker runs upstream's refresh (`refresh_cache`: entries older than 6 h re-queried, timeouts after 72 s, each pass capped at 60 s) every minute, behind a Redis lock so one replica refreshes at a time. **Deliberate differences, all from the shared, persistent store:** entries expire `ARAX_KP_CACHE_TTL_SEC` (3 days) after their last request (a refresh does not extend them), and the cache is not cleared at startup; a data-store error is a miss (lookup) or a skipped store; TLS is verified (D-5's `ssl=False` does not apply). `ARAX_KP_CACHE_ENABLED=false` turns it off (every lookup misses, every store is a no-op), which is how the parity tests run, matching upstream's goldens. | EXP-31, CRT-05, CON-05, OPS-07 (KP-cache refresh), API-09 (`kp_cache`), EXP-06, ORC-04. `shepherd_utils/arax/Expand/trapi_query_cacher.py`, `workers/arax/worker.py` (`kp_cache_refresh_loop`), `shepherd_server/aras/arax_status.py`. |
| DEC-19 | **`/response/{id}` accepts the UI's `X` prefix on a Shepherd response id.** The UI sends `X`+id for every id that is not a number (`isNaN(id) ? "X"+id : id` in its load box, `?r=` links, session history and workflow import), meaning an ARS PK. ARAX's own ids are integers, but Shepherd's are 8 hex digits (`916eaac7`), so every UI lookup of a Shepherd response would miss. Hex ids never contain `X`, so a short `X…` id (30 characters or fewer; ARS PKs are longer) is looked up without it. | API-07, the UI contract's load-by-id row. `shepherd_utils/arax/ResponseCache/response_lookup.py`; `tests/unit/test_arax_api.py`. |

### UI contract: what the ARAX UI requires

This follows from DEC-2. The list comes from `UI/interactive/rtx.js` and
`rtxcompletenode.js`. The UI talks to one ARAX base URL (`baseAPI`, or
`config.query_endpoint` for queries), so everything below has to be served
under a single Shepherd ARAX base path.

| UI need | UI call (rtx.js line) | Inventory item | Notes |
|---|---|---|---|
| Submit a query and follow its progress | `POST {query}` with `stream_progress: true` (609, 670) | API-01, API-02, EXP-32 | **Served**: ARAX's own stream (log entries, the kill token, `query_plan` updates, the final envelope), relayed from the worker (§2a). |
| Query options set from the UI settings panel | `query_options.kp_timeout`, `prune_threshold`, `max_pathfinder_paths`, `max_path_length`, `bypass_cache` (610-634) | ORC-04, EXP-03/04/06, C-4 | **Served**. `bypass_cache` skips the KP cache, as in ARAX (DEC-18). `max_pathfinder_paths`/`max_path_length` are ignored (DEC-7). |
| Cancel a running query | `GET /status?terminate_pid=&authorization=` (895) | OPS-02 | **Served:** ends the query's stream, as the UI sees ARAX's kill; the work still finishes in the worker (§2a). |
| Load a response by id (numeric ids, and ARS PKs prefixed with `X`) | `GET /response/{id}` (130, 538, 1270, 1335, 1341, 6862) | API-07, OPS-06 | **Served**: ARAX's `get_response`, reading Shepherd's storage and its own ARS (DEC-3). `X` means ARS lookup plus attribute stripping, and the `stats` view uses `validation_result` and `provenance_summary`. |
| Attribute detail for a stripped response | `GET /response/{detail_lookup}` (4773), `Z` prefix (7357) | API-07 | **Served** (the component cache is in Shepherd's data store). |
| Recent queries list / active queries | `GET /status?last_n_hours=N`, `GET /status?mode=active` (7156) | OPS-03 | **Served**, from Shepherd's query table. |
| Original input query of a past run | `GET /status?id=` (7309) | OPS-01 / `get_status(id_)` | **Served**: returns the stored input query. |
| Recent ARS PKs | `GET /status?mode=recent_pks&last_n_hours=&authorization=<ars host>` (6978) | API-09 | **Served**, from Shepherd's own ARS. |
| KP cache listing | `GET /status?mode=kp_cache` (8301) | API-09, EXP-31 | **Served**: ARAX's listing of the KP cache in Shepherd's data store (DEC-18). |
| SmartAPI listing | `GET /status?authorization=smartapi` (7793) | API-09, EXP-29 | **Served**: kept for the UI (DEC-4). |
| Site configuration | `GET /status?mode=site_config` (9901) | API-09 | **Served**: versions and maturity. |
| Meta-KG for the query builder | `GET /meta_knowledge_graph?format=simple` (6894) | AUX-01 | **Served**: built from Retriever's metadata (DEC-11). |
| Entity lookup | `GET /entity?q=` and `POST /entity` (9648, 9697, 9769) | API-06, SYN-02 | **Served** (§2a). |
| Node-name autocomplete | `GET /rtxcomplete/nodeslike?word=&limit=15`, relative to the UI host (`rtxcompletenode.js:67`) | AUX-02 | **Served** at `/arax/rtxcomplete/nodeslike`; the UI's host must route `/rtxcomplete/` there. |
| Swagger link | `{baseAPI}/ui/` (80) | — | Shepherd serves `/docs`. |
| Calls that do not go to ARAX | ARS submit (523), PloverDB `EXT` (554), `uptime.rtx.ai` (8058, 8164), ARS test-runner artifacts (8449-8537), `rtx.version` (10107) | — | Not Shepherd's concern. |

## Remaining work

What is left after the port (branch `claude/optimistic-gauss-bjtrzh`):

1. **Real data files (DEC-6).** The download URLs for curie_to_pmids,
   ExplainableDTD, COHD, FDA drugs and autocomplete are placeholders, and the
   parity tests ran on small synthetic stand-ins with the same schemas. Once
   the real files exist: set the URLs, size the volumes (ExplainableDTD is
   large), and check that the real schemas match. Until then,
   `python -m shepherd_utils.arax_mock_data --pathfinder` writes mock files
   in the same shape for local testing (README, "Mock ARAX data";
   `tests/unit/arax/test_mock_data.py` runs every reader over them).
2. **Validation against a live ARAX.** The offline evidence, point by point for what the ARAX team asked to keep, is in [ARAX_PRESERVATION.md](ARAX_PRESERVATION.md); upstream's own test suite runs against the port with `--arax-live` (`tests/unit/arax/upstream_suite/`). Every parity test compares the port with
   upstream ARAX's own code, but offline: a mock Retriever, synthetic data, and
   stand-ins for NodeNorm, COHD's web lookup and reasoner-validator. A run of
   real queries through a live ARAX and through Shepherd, compared, is the next
   check. The upstream demo workflow corpus and its two-endpoint diff script
   (§22) are a ready starting point. `connect_nodes` (whose limits are
   Shepherd's, DEC-7) has only port-side tests; `add_node_pmids` and the xCRG
   MVP2 route are parity-tested with NCBI eUtils and the Retriever stubbed.
3. **Build and deploy.** The `arax` worker and server images have not been built
   (the install steps were checked in fresh environments). The `arax` worker's
   resources (`compose.test.yml`: 1 CPU, 3 GB) were sized for the old proxy.
4. **UI deployment.** The real UI was confirmed working against Shepherd, set up as a deployment should be (see [ARAX_PRESERVATION.md](ARAX_PRESERVATION.md)). The UI calls autocomplete at `/rtxcomplete/nodeslike` on
   its own host, which must route to `/arax/rtxcomplete/nodeslike`. Its Swagger
   link (`{baseAPI}/ui/`) has no Shepherd equivalent (Shepherd serves `/docs`).
5. **`GET /status/logs`** (API-10) is not served. The UI does not call it.
6. **The bug-fix pass (DEC-1)**, after validation: the Part D defects (all
   reproduced today), C-1 and C-3 to C-7 in `arax.pathfinder`, and C-12. Found since, while checking what the ARAX team asked to keep: D-22 to D-26. D-22 (an empty response in the UI whenever the TRAPI validator raises) and D-23 (the `filter_kgraph_*` workflow operations crashing after a lookup) are the ones users would hit first.

## How to read this

The **Shepherd** column in each table says where each item stands in the
port (branch `claude/optimistic-gauss-bjtrzh`), using these values:

| Mark | Meaning |
|---|---|
| **Ported** | Shepherd does it as ARAX does, through the ported library in `shepherd_utils/arax/` (run by the `arax` worker) or the `/arax` API. Changes a recorded decision requires are named in the cell. "Parity-tested" means a test compares it with upstream ARAX's own code. |
| **Partial** | Some of it is ported, as the cell says. |
| **Shepherd's own** | By decision, Shepherd's existing implementation is used instead of ARAX's. |
| **Not ported** | Not done: by a decision where one is named, otherwise open (see [Remaining work](#remaining-work)). |
| **Removed** | Dead code, not ported (DEC-5, Part E). |
| **Infra** | ARAX operational plumbing that Shepherd covers with its own design (Postgres/Redis state, callbacks, OTEL). What clients observe is matched; the plumbing is not re-implemented. |
| **External** | Stays outside Shepherd (DEC-2). |

The Details columns still describe what upstream ARAX does.

**Current Shepherd state, in one line:** the `arax` worker runs the ported ARAX
library in-process (DEC-14, §2a) for every non-pathfinder query, TRAPI
pathfinder queries go to the `arax.pathfinder` worker (DEC-7), and the `/arax`
API serves everything the ARAX UI calls. Before the port, the worker proxied
each query to a remote ARAX service (`settings.arax_url`), and only
`arax.pathfinder` and `arax.rank` were native.

---

## Part A: Summary matrix

| Area | IDs | ARAX size | Shepherd now | Main data / service dependencies | Port difficulty (as estimated before the port) |
|---|---|---|---|---|---|
| HTTP API and envelope semantics | API-* | about 1.5k LOC (Flask server) | Ported under `/arax`; `/asyncquery` is Shepherd's own (DEC-15) | MySQL, S3, NodeNorm | Medium (mostly deciding what clients rely on) |
| Orchestration, ARAXi DSL, workflow ops | ORC-*, DSL-*, WF-* | about 3k LOC | Ported (in-process library, DEC-14) | none | Medium |
| QG interpreter and templates | QGI-* | about 1.3k LOC + YAML | Ported (TRAPI `paths` go to `arax.pathfinder`, DEC-7) | NodeNorm (category lookup) | Low to medium |
| Expand (KP querying, multi-hop, merging) | EXP-* | about 5k LOC | Ported, Retriever only (DEC-4), parity-tested | SmartAPI, KP meta-KGs, Retriever, Gandalf, NodeNorm, FDA pickle | **High** |
| NodeSynonymizer / Biolink | SYN-*, BL-* | about 1k LOC | Ported | NodeNorm, Name Resolver, Biolink YAML | Low |
| Overlay | OVL-* | about 3k LOC | Ported (ICEES removed, E-8) | curie_to_pmids, tier0 overlay sqlite, COHD sqlite + cohd.io, Gandalf, eUtils | Medium |
| Filter_KG | FKG-* | about 2.4k LOC | Ported | general_concepts.json | Low to medium |
| Filter_Results | FRS-* | about 1.3k LOC | Ported | none | Low |
| Resultify | RES-* | about 1.75k LOC | Ported, with ARAX's own tests | none | **High** (subtle algorithm) |
| Ranker | RNK-* | about 0.9k LOC | Ported, exact (DEC-8) | none | Low (fix divergences) |
| ResultTransformer | RTF-* | about 0.3k LOC | Ported | none | Medium (ordering matters) |
| Infer (xDTD) + creative treats | INF-*, CRT-* | about 4k LOC | Ported (legacy xCRG removed, E-2) | ExplainableDTD sqlite (large) | **High** |
| Connect: pathfinder | CON-* | package | Shepherd's pathfinder (DEC-7) | curie_ngd, tier0 overlay sqlite, Retriever | Low (fix divergences) |
| Connect: xCRG | XCR-* | package | Ported | curie_ngd, curie_to_pmids, Retriever | Low to medium (package wrap) |
| Caching, tracking, background tasks | OPS-* | about 2.5k LOC | Shepherd's own, with ARAX's views (see OPS-*) | SQLite/MySQL, SmartAPI | Depends on scope |
| Meta-KG, entity, autocomplete, UI | AUX-* | varies | Ported; the UI stays external (DEC-2) | Plover/Gandalf, NodeNorm, autocomplete sqlite | Scope decision |

---

## Part B: Detailed inventory

### 1. HTTP API (`UI/OpenAPI/python-flask-server/openapi_server`)

The spec is `openapi/openapi.yaml`: OpenAPI 3.0.1, ARAX 1.6.2, TRAPI 1.6.0,
Biolink 4.2.5, `infores:arax`, `asyncquery: true`. There is no authentication.

| ID | Endpoint / behavior | Details | Shepherd |
|---|---|---|---|
| API-01 | `POST /query` (sync) | Forks a child per query (RLIMIT_AS 32 GiB). Returns an envelope with an extra top-level `http_status` key. The HTTP status is 200, 400 (any `response.error()`) or 429 (OverLimit). `remote_address` comes from `X-Forwarded-For`. | **Ported** (`/arax/query`): ARAX's own envelope, log and `http_status` (§2a). No 429 (DEC-16); `remote_address` is not recorded. |
| API-02 | `POST /query` with `stream_progress: true` | `text/event-stream` carrying raw NDJSON (not SSE `data:` framing). It emits, in order: log entries as they happen; a `{pid, authorization}` kill token; `query_plan` updates (per-qedge/per-KP status Waiting, Done, Timed out, Skipped, Error, Warning); a 180 s heartbeat; then the final envelope. On a NaN serialization failure it sends an emptied ERROR envelope. The status is always 200. The ARAX UI depends on this. | **Ported**: ARAX's own stream, relayed from the worker (§2a). The kill token is Shepherd's (see OPS-02). |
| API-03 | `POST /asyncquery` | `callback` is required (`^https?://`) and `http://localhost*` is rejected. The fork happens inside `execute_processing_plan`. The immediate reply is the envelope with `status: "Running"`. **No `job_id` is returned.** The child POSTs the result to the callback: 300 s timeout, 3 tries 10 s apart, 200/201 counts as success, and a read timeout also counts as success. | **Shepherd's own** (DEC-15) |
| API-04 | `GET /asyncquery_status/{job_id}` | Tracker state, `response_url=https://arax.ncats.io/?r=<id>`, and `logs` is always `[]`. | **Shepherd's own** (DEC-15) |
| API-05 | `GET /meta_knowledge_graph?format=full\|simple` | See AUX-01. | **Ported** (DEC-11) |
| API-06 | `GET/POST /entity` | `NodeSynonymizer.get_normalizer_results`, which calls NodeNorm and Name Resolver. | **Ported** |
| API-07 | `GET /response/{id}` | Loads a stored response (see OPS-05). It runs `reasoner-validator` and adds `validation_result` and `provenance_summary`. It can also fetch ARS PKs/UUIDs from the ARS prod, test, ci and dev instances. | **Ported** (DEC-3), parity-tested |
| API-08 | `POST /response` | Callback sink. Writes `data/callbacks/NNNNN.json` (cap 5000) and returns `"received!"`. | **Ported**: kept in Shepherd's data store |
| API-09 | `GET /status` | `mode` values: `kp_cache`, `recent_pks`, `site_config`, `system_load`, `active`; also `terminate_pid` + `authorization` and `authorization=smartapi`. Default is the recent-query list. | **Ported**, from Shepherd's query table. `kp_cache` lists the KP cache (DEC-18); `system_load` is `[]`. |
| API-10 | `GET /status/logs` | Returns the whole server error log. | **Not ported** (open; not in the UI contract) |
| API-11 | `POST /translate`, `GET /exampleQuestions` | Always 501. | **Removed** (DEC-5, E-7) |
| API-12 | `GET /PubmedMeshNgd/{t1}/{t2}` | Legacy NGD through NCBI eUtils. | **Removed** (DEC-5, E-7) |
| API-13 | Serialization | The custom JSON provider **drops null fields**. | **Ported**: `/query`, the stream and `/response` send dicts, which ARAX serializes with nulls kept too (the null-dropping applies only to model objects, which these endpoints don't return). Sync `/query` keys are not sorted. |
| API-14 | Startup | Must load BMT, else fatal. Checks DB versions (`ARAXDatabaseManager`), forks the background tasker (OPS-07), loads the general-concept block list, and sets up Jaeger OTEL. | **Infra**: the worker and server fetch their data files at startup (DEC-6); BMT loads on first use. |

### 2. Orchestration (`AQ/ARAX_query.py`)

| ID | Functionality | Details (refs) | Shepherd |
|---|---|---|---|
| ORC-01 | Input examination | Sets `have_operations`, `have_workflow`, `have_message` and `have_query_graph`. Errors: `NoQueryMessageOrOperations`; `OperationsNotSupported` in KG2 mode (AQ:430). | **Ported** |
| ORC-02 | QG validation | Allowed qnode keys: `ids, categories, is_set, set_interpretation, set_id, member_ids, option_group_id, name, constraints`. Allowed qedge keys: `predicates, subject, object, option_group_id, exclude, relation, attribute_constraints, qualifier_constraints, knowledge_type`. A singular `predicate` gets a TRAPI 1.4 migration error. The QG must have `edges` or `paths` (AQ:499). | **Ported** |
| ORC-03 | Dispatch precedence | Precedence is: `workflow`, then (TRAPI workflow → ARAXi, appended to `operations.actions`), then `operations`, then the QG interpreter. If both a QG and operations are given, the interpreter is skipped (AQ:287-426). | **Ported**. Shepherd's server passes an ARAX query's workflow through unchecked, for ARAX to validate (DEC-17) |
| ORC-04 | `query_options` honored | `kp_timeout`, `prune_threshold`, `return_minimal_metadata` (always overwrites the DSL value), `bypass_cache`, `max_path_length`, `max_pathfinder_paths`. A top-level `return_minimal_metadata` is copied into `query_options`. | **Ported**. `bypass_cache` skips the KP cache (DEC-18); `max_path_length` / `max_pathfinder_paths` are validated but ignored (DEC-7). |
| ORC-05 | Query fields **ignored** | Top-level `log_level`, `max_results`, `page_size`, `page_number`, `enforce_edge_directionality`, top-level `bypass_cache`, and `operations.options`. | **Ported** (still ignored) |
| ORC-06 | Submitter derivation | Callback `http://localhost:8000/ars/…` gives `ARS`. Otherwise the `submitter` key is used if present (even null), otherwise the callback host, otherwise `?`. The host regex requires https. | **Ported**, except that a query with no `submitter` gets Shepherd's `infores:shepherd-arax:…` from the worker (as the old proxy did), so the callback-host fallback never applies |
| ORC-07 | Input messages | `operations.message_uris`: ARAX response URLs load locally, others are fetched by HTTP GET, and the last one wins. `operations.messages`: more than one gives a warning and **only the first is used** (no merging). Pre-existing KG edges trigger `recompute_qg_keys`, which fails if there are no results. | **Ported**: Shepherd response URLs load from Shepherd's store (DEC-3); other URIs as upstream |
| ORC-08 | Original-QG preservation | `response.original_query_graph` is saved before processing and restored by the ResultTransformer. | **Ported** |
| ORC-09 | Automatic ranking | Runs after every `resultify` (but not `scoreless_resultify`), unless the plan contains **any** `connect` action or the mode is RTXKG2 (AQ:853-867). | **Ported**: the ported ranker runs in-process at ARAX's point (DEC-8) |
| ORC-10 | Post-processing | Runs ResultTransformer (RTF-*). Sets `total_results_count`. Every result with a null `resource_id` gets `infores:arax`. `OK` becomes `Success`. Sets `envelope.query_options.query_plan`. Stores the response (OPS-05). `envelope.description` is set *before* the text becomes "…with N results", so clients see "Normal completion". | **Ported** |
| ORC-11 | Implicit `return(response=true, store=true)` | **Every API query is stored.** An explicit `return()` defaults any missing key to `false`. | **Ported**: stored under the query's Shepherd response id (DEC-3) |
| ORC-12 | Error model | `response.error()` sets ERROR and HTTP 400 but does **not** stop the current action. After each action the loop stops if the status is not OK. An exception inside an action gives `UncaughtARAXiError`. `MemoryError` empties the results. The full log list (including DEBUG) is always returned in `envelope.logs`. | **Ported** |
| ORC-13 | Modes | `ARAX` (sync), `asynchronous`, `RTXKG2` (one-hop only, `expand(kp=infores:rtx-kg2)`, no ranking or transform; resultify sets `resource_id=infores:rtx-kg2`; no caller in the repo). | **Ported** (`ARAX` mode). `asynchronous` mode is in the code but unused: Shepherd runs async queries itself (DEC-15). RTXKG2 is removed (E-5). |
| ORC-14 | Concurrency limit | Per-remote-address cap of `round(cpu*50/16)` ongoing queries, plus a free-RAM floor of 15%. Denial gives 429 `OverLimit`. Submitter `infores:arax` and null are exempt. | **Not ported** (DEC-16) |
| ORC-15 | Envelope defaults (`ARAX_messenger.create_envelope`) | `resource_id='ARAX'` (not the infores), `tool_version='ARAX <ver>'`, `schema_version=1.6.0`, `biolink_version=4.2.5`, `type='translator_reasoner_response'`, `context` = the Biolink jsonld URL, `datetime` format `%Y-%m-%d %H:%M:%S`. | **Ported** |

#### 2a. Port status (orchestration, Infer, Connect)

**Ported (DEC-14):** `ARAX_query.py`, `ARAX_query_graph_interpreter.py` (with its templates), `operation_to_ARAXi.py`, `result_transformer.py`, `ARAX_infer.py` with `Infer/scripts/` (`infer_utilities`, `ExplianableDTD_db`, `build_mapping_db`), and `ARAX_connect.py`. Each header lists its changes. Shepherd stand-ins (same interface, not ports): `ARAX_query_tracker.py`, `ResponseCache/response_cache.py`, `Path_Finder/utility.py`, and more attributes on `RTXConfiguration.py`.

**Changes from upstream** (all recorded decisions):
- DEC-3: `ResponseCache.add_new_response` assigns the id the caller passes (`ARAXQuery(response_id=...)`, else a new UUID), sets `envelope.id` to `{settings.server_url}/arax/response/{id}` and writes nothing; the caller saves the envelope. `get_response` reads Shepherd's store. The same URL is used in the "stored with id" log line, the `response=false` return and the `message_uris` match for a local response. Connect reads and stores through the KP cache as upstream (DEC-18).
- The query tracker is a no-op: no MySQL tracking and no per-address limit (ORC-14 is infrastructure).
- DEC-5: removed the RTXKG2 mode (E-5), the `filter` and `fetch_message` DSL commands and `ARAXFilter` (E-4, E-6; they now give `UnrecognizedCommand`), the seven dead templates (E-3), the `sort_results_edge/node_attribute` workflow ops and the `predict_drug_treats_disease` step (E-4), the legacy xCRG infer action and `genrete_regulate_subgraphs` (E-2), and the xDTD build modes (E-10).
- DEC-7: `connect(action=connect_nodes)` keeps ARAX's validation and TRAPI conversion, but searches with Shepherd's pathfinder limits (4 hops, 500 paths, whatever `max_path_length`/`max_pathfinder_paths` say), Shepherd's pathfinder data files, `SYNC_KG_RETRIEVAL_URL` and `KG_REHYDRATE_URL`. TRAPI `paths` queries still go to the `arax.pathfinder` worker.
- DEC-4: `connect(action=xcrg)` uses `SYNC_KG_RETRIEVAL_URL` (the `ARAX_XCRG_RETRIEVER_URL` override is kept).
- ARAX's class-name string checks (`"<class 'openapi_server.models.message.Message'>"`) follow the vendored models' package path.
- `ARAXResponse.output` is not switched to STDERR at import.

**Parity check:** `tests/unit/arax/test_query_parity.py` runs 37 queries end to end through `ARAXQuery.query()` against the Expand mock Retriever. The data files are small synthetic ones: tier0 overlay, curie_to_pmids, COHD, ExplainableDTD and FDA drugs. The goldens were recorded from upstream ARAX's own ARAXQuery (`query_parity/run_upstream.py`, with its tracker and response store stubbed and its KP cache always missing; the port runs with `ARAX_KP_CACHE_ENABLED=false`, which does the same). The queries cover:
- the QG templates;
- ARAXi plans with every overlay except `add_node_pmids`, plus filter_kg, filter_results, scoreless resultify and `rank_results`;
- the automatic ranker after `resultify`, and the ResultTransformer (aux graphs for virtual edges, option groups);
- creative treats through Expand, and xDTD Infer with and without a QG;
- TRAPI workflows;
- input validation, query options, submitter derivation, parse, action and KP errors.

The port matches the status, error code, final envelope and INFO-and-above log, and every KP request. The intended differences:
- single-node queries go to Retriever (DEC-4);
- `overlay_exposures_data` is no longer listed as allowable (E-8);
- `filter(...)` is unrecognized (E-4).

Connect is checked by port-only tests (`test_ARAX_connect.py`), because DEC-7 makes pathfinder parity a non-goal and xCRG runs in the same package on both sides. The stand-ins are checked by `test_ARAX_query_shepherd.py`.

**Worker:** `workers/arax/worker.py` runs each non-pathfinder query in a process-pool child, the way ARAX's non-streaming `/query` does: `ARAXQuery(response_id=...).query_return_message(query)`, then `to_dict()` plus `http_status`. The response is saved under the query's response id, whose URL becomes `envelope.id`. A successful response gets Shepherd's provenance (`infores:shepherd-arax`). ARAX's own error responses are saved as ARAX returns them, with its status, description and log, and the task fails with ARAX's HTTP status. A response ARAX could not serialize (NaN) is a 500, as it is in ARAX. On startup the worker fetches the pathfinder DBs plus curie_to_pmids, ExplainableDTD, FDA drugs and COHD. `arax_url` is no longer used.

**API (`shepherd_server/aras/arax.py`, mounted at `/arax`):**
- `POST /query` returns ARAX's own envelope, with ARAX's log and ARAX's HTTP status (API-01). Shepherd's generic `/query` would replace the log with Shepherd's and answer any failure with 500. A response ARAX did not produce (a pathfinder query, or an error response the worker wrote) is finished the generic way.
- With `stream_progress: true` (API-02), the worker runs ARAX's own `query_return_stream` and relays each NDJSON line it yields through a Redis list (`shepherd_utils/arax_progress.py`); the server streams the lines as they arrive, then the saved response, serialized as ARAX does. It is HTTP 200 whatever happens, as in ARAX. The server adds ARAX's 180 s heartbeat when nothing arrives (for queries the worker hands to `arax.pathfinder`).

- `GET /response/{id}` (API-07) is upstream's `get_response`, ported to `shepherd_utils/arax/ResponseCache/response_lookup.py`: the branch order, validator calls and result shapes, error tuples, actor lookups, `X` attribute stripping with `detail_lookup`, `Z` component reads and size strings are upstream's, including its quirks. One quirk: the URL branch's validation always ends in the "validator crashed" result, because it reads `validation_messages_text` before assigning it. What changes (DEC-3):
  - a Shepherd response id replaces ARAX's integer ids;
  - ARS PKs are read in-process from Shepherd's own `/ars` app, and `ars_host` is Shepherd's host;
  - the component cache is in Shepherd's data store (7-day TTL) rather than a per-process directory;
  - the actor lookup also knows Shepherd's `ara-shepherd-*` agent names.

  Validation (`reasoner-validator==6.0.2`, ARAX's pin, installed through the `arax-api` extra in the server image) runs in a small process pool, as ARAX forks a child for it. The body is `json.dumps`, as Flask serializes a dict (nulls and NaN kept).
- `POST /response` (API-08) keeps the body in the data store, capped at 5000 like upstream's files, and answers `"received!"`.
- Parity: `tests/unit/arax/test_response_parity.py` runs 28 lookups through the port and through upstream's `get_response`, with storage, URLs and the ARS fed the same data, and matches all of them. The cases cover local, URL, ARS parent and child, `X` then `Z` then cached, and every error path. Both sides use a deterministic stand-in for reasoner-validator, which is the same package on both and needs the network. The one intended difference, labelling results from Shepherd's agent names, is asserted separately.

- `GET /status` (API-09, `shepherd_server/aras/arax_status.py`), in the shapes ARAX's tracker and controllers return:
  - the recent-query list and `mode=active` come from Shepherd's query table (queries routed to ARAX). States map `QUEUED`→`started`, `COMPLETED`→`Completed`, `ABANDONED`→`Died`, and `pid` is null;
  - `id=` returns the stored input query;
  - `mode=site_config` returns the same keys as ARAX's `get_config_settings`, with the data-file versions taken from Shepherd's filenames;
  - `mode=recent_pks` is ARAX's `RecentUUIDManager`, ported to read Shepherd's own ARS whichever host the UI names;
  - `authorization=smartapi` is ARAX's SmartAPI client;
  - `mode=kp_cache` is ARAX's listing of the KP cache in Shepherd's data store (DEC-18), and `mode=system_load` is `[]` (OPS-04 is infrastructure).
- `terminate_pid` (OPS-02): Shepherd can't signal a worker's pool child in another container, and killing it would break the pool. So the server replaces ARAX's `{pid, authorization}` stream line with a deployment-unique token, and `terminate_pid` ends that query's stream, which is all an ARAX client sees when ARAX kills its child. The query itself still runs to completion in the worker.
- The server image installs the `arax-api` extra: `reasoner-validator`, ARAX's `bmt` and `requests-cache` pins, `aiohttp`, `pandas` and `requests`.

- `GET/POST /entity` (API-06) goes to ARAX's `NodeSynonymizer.get_normalizer_results`, as upstream does.
- `GET /meta_knowledge_graph` (API-05 / AUX-01) is ARAX's `KnowledgeSourceMetadata`, ported with only DEC-11's changes. The base comes from Retriever's `/meta_knowledge_graph`, and the KPInfoCacher merge is dropped. The fill-ins, standard attribute constraints, `format=simple`, the 1 h cache and the 3 JSON backups with fallback (in `ARAX_DBS_DIR`) are upstream's. The server runs ARAX's hourly `refresh_meta_kg` in the background.
- `GET /rtxcomplete/nodeslike` (AUX-02) is ARAX's `rtxcomplete.get_nodes_like` over `autocomplete_v1.0_<tier>.sqlite`, JSONP with upstream's callback sanitizing and `"error"` on failure. The server fetches the database at startup (DEC-6), so its compose service now mounts `./arax_dbs`. The UI calls this path relative to *its own* host, so a deployment of the UI must route `/rtxcomplete/` to `/arax/rtxcomplete/`.
- Parity: `tests/unit/arax/test_aux_parity.py` compares the full and simple meta-KG (backup fallback, no backup, and the refresh function) and a 15-lookup autocomplete sequence (the fragment cache carries over between lookups) with upstream's own modules. All of them are identical.

Every row of the UI contract is now served under `/arax`. The one exception is a recorded decision: `terminate_pid` ends the stream rather than the work (see `/status` above).

### 3. ARAXi DSL (`AQ/actions_parser.py`, `AQ/ARAX_messenger.py`)

| ID | Functionality | Details | Shepherd |
|---|---|---|---|
| DSL-01 | Grammar | One command per line, `name(k=v,…)`. A leading `#` is a comment. Values are always strings. A bare `k` means `k=true`. `[a,b]` is a list. No quoting or escaping; splitting is on every comma. A naked command has `parameters: None`, which most handlers crash on. A duplicate key: the last wins. | **Ported** |
| DSL-02 | `create_message` / `create_envelope` | Resets the envelope but not the original QG. | **Ported** |
| DSL-03 | `add_qnode(key, ids, name, categories, is_set, option_group_id)` | Auto keys `n00…`. `name` is resolved through the synonymizer (error `UnresolvableNodeName`). Enforces `option_group_id` with `is_set`. A single-id list forces `is_set=false`. | **Ported** |
| DSL-04 | `add_qedge(key, subject, object, predicates, option_group_id, exclude)` | Auto keys `e00…`. | **Ported** |
| DSL-05 | `add_qpath(key, subject, object)` | Converts to a `PathfinderQueryGraph` and **drops existing edges**. | **Ported** |
| DSL-06 | `expand(...)` | See EXP-*. | **Ported** (Retriever only, DEC-4) |
| DSL-07 | `overlay(action=…)` | See OVL-*. | **Ported** (`overlay_exposures_data` removed, E-8) |
| DSL-08 | `filter_kg(action=…)` | See FKG-*. | **Ported** |
| DSL-09 | `filter_results(action=…)` | See FRS-*. | **Ported** |
| DSL-10 | `resultify(ignore_edge_direction)` / `scoreless_resultify` | See RES-*. `scoreless_` only suppresses the auto-rank. | **Ported** |
| DSL-11 | `rank_results()` | Explicit `aggregate_scores_dmk`. | **Ported** (in-process; `arax.rank` also remains a workflow step) |
| DSL-12 | `infer(action=…)` | See INF-*. | **Ported** (xDTD; the legacy xCRG action is removed, E-2) |
| DSL-13 | `connect(action=connect_nodes\|xcrg)` | See CON-*, XCR-*. | **Ported**: `xcrg` as upstream (DEC-4 Retriever); `connect_nodes` with Shepherd's pathfinder limits and data (DEC-7) |
| DSL-14 | `return(response, store)` | Stops the plan. | **Ported** |
| DSL-15 | `fetch_message(uri)` | **Broken**: the fetched message is discarded. | **Removed** (DEC-5, E-4) |
| DSL-16 | `filter(...)` (legacy) | Broken or a no-op. | **Removed** (DEC-5, E-4, E-6) |
| DSL-17 | `remove_qedge` | Exists in the messenger but is not dispatched (`UnrecognizedCommand`). | **Removed** (DEC-5, E-4) |

### 4. TRAPI workflow operations (`AQ/operation_to_ARAXi.py`)

The `x-trapi.operations` list in ARAX's OpenAPI spec matches this `implemented`
set exactly. Only `operation.parameters` is read; `runner_parameters` is
ignored.

| ID | Operation | ARAXi emitted | Shepherd |
|---|---|---|---|
| WF-01 | `lookup` | `expand()`, `scoreless_resultify(ignore_edge_direction=true)` | **Ported** (translated in the worker) |
| WF-02 | `lookup_and_score` | `expand()`, `resultify(ignore_edge_direction=true)`, which auto-ranks | **Ported** (translated in the worker) |
| WF-03 | `fill` (`allowlist`, `qedge_keys`) | `expand(kp=[…], edge_key=[…])`. A `denylist` is an error. | **Ported** (translated in the worker); the `allowlist` goes to Retriever as `parameters.kp` (DEC-10) |
| WF-04 | `bind`, `complete_results` | `scoreless_resultify(ignore_edge_direction=true)` | **Ported** (translated in the worker) |
| WF-05 | `score` | `rank_results()` | **Ported** (translated in the worker) |
| WF-06 | `overlay_compute_ngd` (`virtual_relation_label`, `qnode_keys`) | NGD for every pair of qnode keys, `default_value=inf` | **Ported** (translated in the worker) |
| WF-07 | `overlay_compute_jaccard` | `compute_jaccard` | **Ported** (translated in the worker) |
| WF-08 | `overlay_fisher_exact_test` (optional `rel_edge_key`) | `fisher_exact_test` | **Ported** (translated in the worker) |
| WF-09 | `overlay_connect_knodes` | NGD + COHD paired_freq + **predict_drug_treats_disease (now errors)** + FET both ways + Jaccard triples | **Ported** (translated in the worker); without the `predict_drug_treats_disease` step (E-4) |
| WF-10 | `annotate_nodes` (`attributes` contains `pmids`) | `overlay(action=add_node_pmids)` | **Ported** (translated in the worker) |
| WF-11 | `filter_results_top_n` (`max_results`, asserted int) | `limit_number_of_results, prune_kg=true` | **Ported** (translated in the worker). For an ARAX query it means ARAX's action, not Shepherd's step of the same name (C-11) |
| WF-12 | `filter_kgraph_orphans` | `filter_kg(action=remove_orphaned_nodes)` | **Ported** (translated in the worker). For an ARAX query it means ARAX's `remove_orphaned_nodes`, not Shepherd's step (C-9) |
| WF-13 | `filter_kgraph_top_n` / `_std_dev` / `_percentile` / `_continuous_kedge_attribute` / `_discrete_kedge_attribute` | The corresponding `filter_kg` actions, with defaults `max_edges` 50, `threshold` 1 or 95, `keep_top_or_bottom` top, `remove_above_or_below` below | **Ported** (translated in the worker) |
| WF-14 | `sort_results_score` (`ascending_or_descending`) | `sort_by_score` | **Ported** (translated in the worker). For an ARAX query it means ARAX's `sort_by_score` (C-11) |
| WF-15 | `sort_results_edge_attribute`, `sort_results_node_attribute` | **Always crash (NameError)** | **Removed** (DEC-5, E-4) |

### 5. Query-graph interpreter (`AQ/ARAX_query_graph_interpreter.py`, `…_templates.yaml`, `AQ/query_graph_info.py`)

| ID | Functionality | Details | Shepherd |
|---|---|---|---|
| QGI-01 | Pathfinder detection (TRAPI 1.6 `paths`) | Emits `connect(action=connect_nodes, max_path_length=<qo or 4>[, max_pathfinder_paths=<qo>])`. | **Shepherd's `arax.pathfinder`** (DEC-7): the worker routes TRAPI `paths` queries there before ARAX runs |
| QGI-02 | xCRG MVP2 detection | `catrax-xcrg.is_xcrg_mvp2_query` gives `connect(action=xcrg)` (see XCR-01). | **Ported** |
| QGI-03 | Legacy pathfinder detection | Any qedge with `knowledge_type=pathfinder`, or ≥2 `inferred` qedges, gives `connect_nodes, max_path_length=4` (ignores `query_options`). | **Ported**: `connect_nodes` runs in-process with Shepherd's pathfinder limits (DEC-7) |
| QGI-04 | `QueryGraphInfo.assess` | Errors on: zero nodes, 1 node with edges, no pinned node (`QueryGraphNoIds`, which applies **even to pathfinder/xCRG**), circular QG, bad subject/object. Looks up the preferred category of the first pinned id through NodeNorm. Skips virtual-predicate edges. Uses only the first predicate and first category. Walks from a singleton node and **truncates forks**. | **Ported** |
| QGI-05 | Template scoring and match | Scores: ids+cat=value 10000, ids 1000, cat=value 100, cat 10, pred=value 90, pred 10. Highest score wins; on a tie, the first found. | **Ported** |
| QGI-06 | Template → DSL remap | Maps `nNN`/`eNN` to real keys by walk order. The edge-placeholder bug is harmless today. | **Ported** |
| QGI-07 | Fallback | Warns, then `expand()`, `resultify()`, `filter_results(limit 500)`. | **Ported** |

**Templates that are reachable** (the others are unreachable; see E-3):

| ID | Template | Shape | DSL |
|---|---|---|---|
| QGI-T01 | `one_node_with_curie` | `n00(ids)` | `expand(node_key=n00)`; `filter_kg(remove_general_concept_nodes)`; `resultify()` |
| QGI-T05 | `one_hop_classic_question` | `ids`–`categories` | `expand()`; `overlay(compute_ngd, N1, n00→n01)`; `filter_kg(remove_general_concept_nodes)`; `resultify()`; `filter_results(limit 500)` |
| QGI-T06 | `one_hop_two_curie_question` | `ids`–`ids` | `expand()`; `filter_kg(remove_general_concept_nodes)`; `resultify()`; `filter_results(limit 500)` |
| QGI-T07 | `one_hop_all_connections` | `ids`–`()` | same as T06 |
| QGI-T11 | `two_hop_classic_question` | `ids`–`cat`–`cat` | `expand()`; `compute_jaccard(J1)`; NGD N1 (n00→n01), N2 (n00→n02), N3 (n01→n02); `resultify()`; `limit 500` |
| QGI-T12 | `two_hop_curie-categories-curie_question` | `ids`–`cat`–`ids` | `expand()`; NGD N1/N2/N3; `resultify()`; `limit 500` |
| QGI-T13 | `two_hop_two_curie_question` | `ids`–`()`–`ids` | same as T12 |
| QGI-T14 | `three_hop_classic_question` | `ids`–`()`–`()`–`()` | `expand()`; FET F1 (n01→n02, top_n 30), F2 (n02→n03, top_n 30); NGD N1/N2/N3; `resultify()`; `limit 500` |
| QGI-T15 | `three_hop_two_pinned_question` | `ids`–`()`–`()`–`ids` | `expand()`; FET F1 (top_n 30); NGD N1/N2/N3; `resultify()`; `limit 500` |
| QGI-T17 | `four_hop_classic_question` | `ids`–`()`×4 | `expand()`; FET F1–F3 (top_n 20); NGD N1–N4; `filter_kg(remove_general_concept_nodes)`; `resultify()`; `limit 500` |

A QG with 6 or more nodes in its walked path fails with
`QueryGraphInterpreterUnsupportedGraph`. A QG whose start node has no ids uses
the fallback.

**Why templates are the main parity lever:** every template's DSL is
effectively "ARAX's workflow" for that shape. A faithful port has to reproduce
the template choice, not just the operations.

### 6. Expand (`AQ/ARAX_expander.py`, `AQ/Expand/*`)

#### 6.1 Parameters

| ID | Parameter | Default and behavior |
|---|---|---|
| EXP-01 | `kp` (str or list) | Chooses the KP(s). Must be in `valid_kps`. **Bypasses** the constraint allowlist/denylist and the blocked-KP list. The user-chosen KP (other than rtx-kg2) is checked against its meta-KG. |
| EXP-02 | `edge_key` / `node_key` | Default: all qedges except Expand-created `subclass:` qedges, and all orphan qnodes. |
| EXP-03 | `prune_threshold` | Default None, which uses EXP-12. |
| EXP-04 | `kp_timeout` | rtx-kg2 is **always 600 s**, otherwise `kp_timeout`, otherwise 120 s. |
| EXP-05 | `return_minimal_metadata` | Only changes a log line. KG2 always gets `return_minimal_metadata: true` in the request. |
| EXP-06 | `bypass_cache` | From `query_options` only. |

#### 6.2 QG normalization, in order (`apply()`, 150-330)

| ID | Step |
|---|---|
| EXP-07 | Blocked KPs: `infores:automat-robokop` and `infores:knowledge-collaboratory` are recorded as "Skipped". |
| EXP-08 | Self-qedges are an error unless the predicate is `subclass_of`. A disconnected required QG is an error. At least one pinned qnode is required. |
| EXP-09 | Constraint whitelist. qnode: `biolink:highest_FDA_approval_status == "regular approval"` only. qedge: `knowledge_source` / `primary_knowledge_source` / `aggregator_knowledge_source` with `==`. Anything else is `UnsupportedConstraint`. Qualifier constraints are passed to KPs unchanged. |
| EXP-10 | Categories. A pinned qnode gets `categories=None`. Otherwise Biolink conflations are added (Gene≡Protein; Disease≡PhenotypicFeature≡DiseaseOrPhenotypicFeature). No categories becomes `NamedThing`. |
| EXP-11 | Predicates are canonicalized. All non-canonical flips the qedge; a mix is an error. `treats*` with a Disease subject flips (only when the subject is unpinned). No predicate becomes `related_to`. |

#### 6.3 Multi-hop engine

| ID | Functionality | Details |
|---|---|---|
| EXP-12 | Prune threshold | Both qnodes pinned: 5000. Open qnode is NamedThing/ChemicalEntity/none with `related_to` or no predicate: 100. Same categories with a specific predicate: 200. Otherwise 500. |
| EXP-13 | Pre-prune between hops | If an already-filled qnode has more nodes than the threshold: set `is_set=false`, run **FET overlay** (if the KG has <100k edges; optional qnodes skipped), **Resultify**, then **Ranker**, and keep nodes from the top results. So Expand depends on OVL-05, RES-* and RNK-*. |
| EXP-14 | Edge ordering | First: a required, non-exclude qedge with a pinned qnode. Then, among qedges connected to the subgraph: required-kryptonite, required, optional-kryptonite, any. |
| EXP-15 | Curie feed-in | Curies found in earlier hops pin the next hop's qnode. The rules depend on required vs optional status. |
| EXP-16 | Per-qedge KP fan-out | KP selection (EXP-25), minus the denylist, intersected with the allowlist, minus blocked KPs. The KPs for one qedge run concurrently (`asyncio.gather`); qedges run sequentially. If one KP errors, the status resets to OK and there are warnings. |
| EXP-17 | Merge semantics | Nodes merge: union of categories, attributes deduplicated by type/value/source, union of `query_ids`. **Edges never merge across KPs**. The edge key is `{kp}:{subj}--{pred}--{qualified_pred}--{obj_dir}--{obj_aspect}--{obj}--{primary_ks}`. A pinned curie cannot fill a different qnode. Aux graphs: the first wins on a duplicate id. |
| EXP-18 | Kryptonite (`exclude: true`) | Removes edges whose shared qnodes match nodes found by the exclude qedge. Required exclude qedges apply globally; optional ones apply per option group. |
| EXP-19 | Dead-end removal | After each hop, runs Resultify with `is_set=true` everywhere and rebuilds the KG from what it keeps. |
| EXP-20 | Early exit | If required qedges are unfulfilled: a warning and a partial KG, **no error**. |
| EXP-21 | Single-node queries | rtx-kg2 only, synchronous `requests`. |
| EXP-22 | Subclass handling | For each KP node whose `query_id` differs from its id, adds a `biolink:subclass_of` edge (child→parent) under qedge `subclass:{q}--{q}` in `option_group-<key>`. Resultify collapses these (RES-06). |
| EXP-23 | Map back to input curies | Canonical curies become the user's input curies (with names), edges are re-pointed, and `query_ids` are remapped. When two inputs map to one canonical curie, only one mapping is kept. |
| EXP-24 | Post-filters | The FDA-approval qnode constraint uses the pickled set `fda_approved_drugs_v1.0.pickle` (honors `_not`). The knowledge-source edge filter is effectively a no-op (see D-2). Self-edges are removed. |

#### 6.4 KP access layer

| ID | Functionality | Details |
|---|---|---|
| EXP-25 | KP selection (`kp_selector.py`) | Uses the meta-KG triple check (descendants of categories and predicates; **the swap is tried for every predicate**). `infores:retriever` skips meta-KG checks and is **always a candidate** unless excluded by version or maturity. |
| EXP-26 | Curie prefix conversion | Pinned qnodes are replaced by NodeNorm canonical curies. For curies fed in from earlier hops, prefixes are converted only if some prefix is unsupported. The input prefix is preferred, otherwise the first supported prefix. Unsupported curies are dropped with a warning. |
| EXP-27 | Request body | Any qnode with no ids or more than one id gets `is_set=true`. Null or empty properties are stripped. `submitter: infores:arax`. **Retriever**: `parameters.tiers=[0]`. **KG2**: `return_minimal_metadata:true`. The request POSTs to `{url}/query`. |
| EXP-28 | Response processing | Drops edge attributes with NaN/Inf values. Validates structure. Builds the KG→QG mapping from `node_bindings`/`edge_bindings` (`query_id` handling). Adds `infores:arax` aggregator provenance (`upstream=[kp]`). Keeps unbound nodes and edges only when an aux graph references them. `genetics-data-provider` has a whitespace fix. |
| EXP-29 | KP info cache (`kp_info_cacher.py`, `smartapi.py`) | SmartAPI at `http://smart-api.info/api/query?q=TRAPI` is filtered to `component==KP`, TRAPI **`1.6.0` (forced)** and the **exact** ARAX maturity. It also fetches each KP's `/meta_knowledge_graph` (10 s). The rtx-kg2 URL is replaced by `plover_url`. The result is a pickle refreshed hourly. **Older than 24 h makes Expand fail** (D-3). |
| EXP-30 | Effective KP roster (production config) | Everything SmartAPI lists at the maturity, except the two blocked KPs. `infores:rtx-kg2` is routed to **Gandalf** (`https://automat.renci.org/translatorkg-gandalf`, via `plover_url_override`). `infores:retriever` is special-cased. `infores:gandalf` is used by FET. *The live roster must be captured from a running ARAX.* |
| EXP-31 | KP response cache (`trapi_query_cacher.py`) | SQLite index plus gzip pickles, keyed by sha256 of `{url, body}`. **Caches timeouts (-1)**. `ssl=False` (D-5). Background refresh: entries older than 6 h are re-queried, timeouts after 72 s. The cache is cleared at startup. `bypass_cache` skips it. |
| EXP-32 | Query-plan telemetry | `response.update_query_plan(qedge, kp, status, description, query)` feeds API-02 streaming. |

#### 6.4a Port status

**Ported (DEC-14):**
- **Expand:** `shepherd_utils/arax/ARAX_expander.py` and `Expand/` (`expand_utilities`, `kp_selector`, `trapi_querier`, `smartapi`).
- **Its dependencies:** the messenger, `query_graph_info`, NodeSynonymizer, BiolinkHelper, `util`, the ranker, the overlay dispatcher with the Fisher exact test, and a Shepherd-backed `RTXConfiguration`.

**Changes from upstream:** each file's header lists them. They are the recorded decisions:
- DEC-4: queries go only to Retriever, at `SYNC_KG_RETRIEVAL_URL`, including single-node queries.
- DEC-18: the KP cache is kept in Shepherd's data store.
- DEC-9: no curie-prefix conversion.
- DEC-10: the `kp` list is forwarded as `parameters.kp`.
- DEC-12: every other SmartAPI KP is marked Skipped.

**Parity check:** `tests/unit/arax/test_expand_parity.py` runs 34 cases against a mock Retriever. The expected outputs were recorded from upstream ARAX's own Expand (`expand_parity/run_upstream.py`). The port matches everything observable:
- the KG, QG and aux graphs;
- the in-memory `qnode_keys`/`qedge_keys`/`query_ids`/`filled` annotations and the excluded-edge info;
- the query plan and INFO-and-above logs;
- every request body sent to the KP.

The test also covers pruning (the Fisher exact test, then Resultify and the ranker), the FDA and knowledge-source constraints, excluded edges, option groups, subclass `query_id` edges, HTTP errors and timeouts. Two differences are intended and asserted separately:
- single-node queries go to Retriever;
- the `kp` list is forwarded.

The goldens need `PYTHONHASHSEED=0`, because ARAX builds several lists from sets. The Shepherd Dockerfiles pin that seed.

**DEC-9 consequence:** pinned curies go to Retriever exactly as given. ARAX would first canonicalize, deduplicate and reorder them (`get_canonical_curies_list` returns `list(set(...))`). For normalized input, only the order differs, and that order is hash-seed dependent in ARAX itself.

**Since ported:** `ARAX_infer` (xDTD), so inferred treats qedges run creative mode (see §2a). Inferred `affects` qedges outside MVP2 still route to the legacy xCRG infer action, which is removed (E-2), so they end in ARAXInfer's `UnknownAction` error instead of upstream's failure on the missing models.

**Since wired:** the `arax` worker runs it (§2a).

#### 6.5 Inferred and creative branches in Expand

See INF-* and CRT-*. In summary: single-qedge inferred `treats`/`ameliorates`
goes to xDTD. Inferred `affects` with a direction goes to the legacy xCRG
(unless MVP2). After inference a **normal lookup also runs** on the same
qedge, with creative-treats widening.

### 7. NodeSynonymizer and Biolink

| ID | Functionality | Details | Shepherd |
|---|---|---|---|
| SYN-01 | `get_canonical_curies(curies, names)` | NodeNorm `POST /get_normalized_nodes` in batches of 2500 (30 s), no conflate flags (server defaults apply). The name path goes through Name Resolver `/bulk-lookup` (batches of 50, 3 retries, prefers human taxon). | **Ported** |
| SYN-02 | `get_equivalent_nodes`, `get_curie_names`, `get_preferred_names`, `get_curie_category` (deepest Biolink level), `get_normalizer_results` | Used by Expand, Overlay, Messenger and `/entity`. | **Ported** |
| SYN-03 | Endpoints by maturity | dev/staging `nodenorm-es.ci.transltr.io`; testing `nodenorm-es.test.transltr.io`; prod `nodenorm.transltr.io/1.4`. Name Resolver is fixed at `name-resolution-sri.renci.org`. The config overrides are **unused**. | **Ported**: the endpoint follows `SERVER_MATURITY` |
| BL-01 | BiolinkHelper | `biolink-helper-pkg==1.0.1`, Biolink 4.2.5 (4.2.0 is forced to 4.2.1). Methods: `get_descendants` (conflations on by default), `get_ancestors`, `get_canonical_predicates`, `add_conflations`, root category/predicate. | **Ported** (1.0.1, DEC-13) |
| BL-02 | `bmt.Toolkit()` | Used by NodeSynonymizer and Infer. Note that `infer_utilities` uses **bmt's default Biolink version, not 4.2.5**. | **Ported** |

### 8. Overlay (`AQ/ARAX_overlay.py`, `AQ/Overlay/*`)

**Shared behavior:**

- **OVL-00a:** the action does nothing if the KG is empty or any referenced
  qnode is unbound.
- **OVL-00b:** if any of `virtual_relation_label`/`subject_qnode_key`/`object_qnode_key`
  is given, all three are required.
- **OVL-00c:** virtual edges carry `EDAM-OPERATION:0226` (label),
  `metatype:Datetime` (`defined_datetime`) and `EDAM-DATA:1772=True`. They are
  bound into every `infores:arax` analysis whose bound nodes cover both ends.
  The new qedge gets `.filled=True` and inherits an option group.
- **OVL-00d:** NGD and ICEES first narrow node pairs to those that co-occur in
  a Resultify pass. COHD, Jaccard and FET use the full product.
- **OVL-00e:** `check_params` stops validating after the first numeric
  parameter.

| ID | Action | Parameters (defaults) | Data / service | Output |
|---|---|---|---|---|
| OVL-01 | `compute_ngd` | `default_value` (`inf`; `'0'` stays a string), `virtual_relation_label`, `subject_qnode_key`, `object_qnode_key` | `curie_to_pmids_v1.0_<tier>.sqlite` (read-only, chunks of 20k). Curies canonicalized through NodeNorm. N = 3.5e7×20. | Virtual `biolink:occurs_together_in_literature_with` edge (arax primary, `statistical_association`/`automated_agent`) with `EDAM-DATA:2526` `normalized_google_distance` = `str(ngd)` and up to 30 PMIDs in `biolink:publications` (non-deterministic subset). **Decorate mode** (no label): an attribute on every KG edge (`ngd_publications`). Three modes; mode (a) keeps only the last qnode pair (D-7). |
| OVL-02 | `overlay_clinical_info` (COHD) | `COHD_method` ∈ {`paired_concept_frequency` (default), `observed_expected_ratio`, `chi_square`} or the equivalent boolean flags; label/subject/object | curie→OMOP via **cohd.io API** (`biolink_to_omop`); statistics from `COHDdatabase_v1.0_KG2.8.0.db` (dataset 3). Eligible categories: SmallMolecule, PhenotypicFeature, Disease, Drug. | Virtual `biolink:associated_with` edge (cohd primary, arax aggregator) with `EDAM-DATA:0951` = `str(value)`. max freq / max ln_ratio / p-value of the largest χ² (D-8). Decorate mode also exists. |
| OVL-03 | `compute_jaccard` | `start_node_key`, `intermediate_node_key`, `end_node_key`, `virtual_relation_label` (all required) | KG only | `biolink:has_jaccard_index_with` start→end, including value 0. Value is a float, `EDAM-DATA:1772`. **Not bound to results.** The value is `\|I(E)∩I(S)\|/\|I(S)\|` (asymmetric). |
| OVL-04 | `add_node_pmids` | `max_num` (100 or `all`) | curie_to_pmids sqlite (lookup-key bug, D-9), with NCBI eUtils fallback | Node attribute `EDAM-DATA:0971` `pubmed_ids` |
| OVL-05 | `fisher_exact_test` | `subject_qnode_key`, `object_qnode_key`, `virtual_relation_label` (required); `rel_edge_key`; `filter_type` ∈ {top_n, cutoff} + `value` | Background counts from `tier0-info-for-overlay_v1.0_<tier>.sqlite` (`neighbors`, `category_counts`). With a single-predicate `rel_edge_key`, counts come from a **Gandalf** query (30 s). | `biolink:has_fisher_exact_test_p_value_with`, `EDAM-DATA:1669` = `str(p)`. Two-sided `scipy.stats.fisher_exact`. Cutoff is a strict `<`. Rows with a negative cell are skipped. |
| OVL-06 | `overlay_exposures_data` (ICEES+) | label/subject/object | `icees.renci.org:16340` (likely defunct), `verify=False` | Virtual mode crashes (D-10). Decorate mode only. |
| OVL-07 | `predict_drug_treats_disease` | — | legacy GraphSage | **Dead**: disabled and not importable. |

### 9. Filter_KG (`AQ/ARAX_filter_kg.py`, `AQ/Filter_KG/*`)

**Shared behavior:**

- **FKG-00a:** parameters are `remove_connected_nodes` (false),
  `qnode_keys`/`qnode_key`, `qedge_keys`.
- **FKG-00b:** node removal un-keys nodes per qnode. A node is deleted only when
  no qnode keys are left. Every edge that touches an affected node is removed.
- **FKG-00c:** error `RemovedQueryNode` if a qnode is left empty.
- **FKG-00d:** results are **not** updated.
- **FKG-00e:** a disallowed value only produces a *warning* and the action is a
  no-op (D-11).

| ID | Action | Parameters (defaults) | Semantics |
|---|---|---|---|
| FKG-01 | `remove_edges_by_predicate` | `edge_predicate` | Exact match. |
| FKG-02 | `remove_edges_by_discrete_attribute` | `edge_attribute`, `value` | Matches a top-level edge field or any attribute (by name or type_id). Provenance aliases (`knowledge_source`, `primary_knowledge_source`, `aggregator_knowledge_source`, `supporting_data_source`, `provided_by`, …) match `sources[].resource_id`. |
| FKG-03 | `remove_edges_by_continuous_attribute` | `edge_attribute`, `direction` (above/below), `threshold` | Strict `>` / `<`. A non-numeric value makes the action error. |
| FKG-04 | `remove_edges_by_std_dev` | `edge_attribute`; `threshold` 1; heuristic `direction`/`top` | Cut = mean ± t·σ (population σ). |
| FKG-05 | `remove_edges_by_percentile` | `threshold` 95 | `np.percentile`. **For ngd-like attributes the default becomes -94 and the action errors** (D-12). |
| FKG-06 | `remove_edges_by_top_n` | `n` 50 | Sorts and keeps n. `direction` is ignored. |
| FKG-07 | Heuristic defaults | — | Attributes in {ngd, normalized_google_distance, chi_square, fisher_exact, fisher_exact_test_p-value}: direction=above, top=False (smaller is better). All other attributes: direction=below, top=True. |
| FKG-08 | `remove_nodes_by_category` | `node_category` | Nodes and incident edges. |
| FKG-09 | `remove_nodes_by_property` | `node_property`, `property_value` | Equality or list membership. |
| FKG-10 | `remove_orphaned_nodes` | `node_category` (has no effect, D-13) | Removes nodes with no incident edge, but keeps nodes bound to QG-orphan qnodes. |
| FKG-11 | `remove_general_concept_nodes` | `perform_action` (true) | Block list `general_concepts.json` (4193 curies, 360 synonyms, 1 regex) matched against name/xref/synonym/equivalent_identifiers, lowercased. It is a **single pass over edges** (edges seen before a node is flagged survive), then `remove_orphaned_nodes`. |

### 10. Filter_Results (`AQ/ARAX_filter_results.py`, `AQ/Filter_Results/sort_results.py`)

**Shared behavior:**

- **FRS-00a:** `prune_kg` **defaults to true** for every action (FRS-07).
- **FRS-00b:** `direction` ∈ {descending, d, ascending, a} is **required** for
  the sorts.
- **FRS-00c:** `max_results` must be an int ≥0.
- **FRS-00d:** no results only produces a warning.

| ID | Action | Semantics | Shepherd |
|---|---|---|---|
| FRS-01 | `limit_number_of_results(max_results)` | `results[:n]`, prune, `message.n_results = n` (requested, not actual) | **Ported** (in ARAX queries; Shepherd's own `filter_results_top_n` step is unchanged, C-11) |
| FRS-02 | `sort_by_score` | Key `analyses[0].score`, stable. A None score makes the action error. | **Ported** (in ARAX queries; Shepherd's own `sort_results_score` step is unchanged, C-11) |
| FRS-03 | `sort_by_edge_count` / `sort_by_node_count` | Total bindings. | **Ported** |
| FRS-04 | `sort_by_edge_attribute(edge_attribute, edge_relation, qedge_keys)` | Per result, the **sum** of bound edges' values. Missing values become ∓inf. The `qedge_keys` bug excludes every edge (D-14). | **Ported** |
| FRS-05 | `sort_by_node_attribute(node_attribute, node_category, qnode_keys)` | Same idea over nodes. For `pubmed_ids` it counts "PMID". Filter bugs D-14. | **Ported** |
| FRS-06 | Legacy `filter()` | — | **Removed** (DEC-5, E-6) |
| FRS-07 | **KG pruning closure** (`analyze_message_get_referenced_IDs`) | Pass 1 collects bindings and support graphs, and **drops any result with a dangling binding**. Pass 2 iterates to a fixed point: (a) **any KG edge with both endpoints referenced is kept**, even if unbound, along with its support graphs; (b) aux graphs are expanded, and an aux graph with any dangling member is **rejected whole**. Shared with Resultify and the ResultTransformer. | **Ported** (in ARAX queries; Shepherd's own `filter_kgraph_orphans` step is unchanged, C-8) |

### 11. Resultify (`AQ/ARAX_resultify.py`)

| ID | Functionality | Details |
|---|---|---|
| RES-01 | Parameters | `ignore_edge_direction` (default true), plus a hidden `debug`. No max-results option and no `knowledge_type` handling. |
| RES-02 | Scope | Only qedges with `filled=True` (the whole QG if none are filled), minus `exclude` qedges. KG restricted to items that have `qnode_keys`/`qedge_keys`. |
| RES-03 | Validation | Errors for unkeyed items, unknown keys, dangling endpoints, direction mismatch (when direction matters), optional qedge without an option group (`MissingOptionalLabel`), disconnected required QG. An unfulfilled required QG gives empty results with **no error**. |
| RES-04 | Result-graph construction | Qnode by qnode from an arbitrary start (**non-deterministic** `list(set)[0]`). `is_set` qnodes are aggregated; others fan out one graph per candidate. Adjacency requires **all parallel qedges** to be fulfilled for the pair. Iterative dead-end cleanup. Edges are added from subject×object pairs (index intersection when both sides have ≥10 nodes). Graphs with any empty qnode or qedge are discarded. |
| RES-05 | Option groups | Each group extends the required graphs. Merge key is the sorted ids of required non-set qnodes (more than one node gives `MergeKeyError`). An unfulfilled group contributes nothing. |
| RES-06 | Subclass collapse | Nodes collapse onto their best parent (`sorted(parents)[0]`) via `subclass:` qedges, then children are restored into the bindings. |
| RES-07 | Edgeless QG | A single result containing everything. |
| RES-08 | TRAPI output | `NodeBinding(id, query_id=best parent if pinned and ≠ id, attributes=[])`. **One `Analysis(resource_id="infores:arax", edge_bindings)`** per result. |
| RES-09 | Essence | Prefers a non-specific, non-set, non-optional leaf qnode (farthest from pinned leaves). The value is the node name plus " (symbol)" when they differ. Uses the parent when several nodes fill the qnode. `essence_category = str(categories)`. `description = "No description available"`. |
| RES-10 | Post-prune | Applies the FRS-07 closure to the KG, aux graphs and results. |
| RES-11 | `recompute_qg_keys` | Rebuilds qnode/qedge keys from the results of an imported message. |

**Ported (DEC-14):** `shepherd_utils/arax/ARAX_resultify.py` (import paths only), with `ARAX_response.py` and `actions_parser.py`. ARAX's 21 offline Resultify tests and its ARAXResponse tests run against the port in `tests/unit/arax/`.

### 12. Ranker (`AQ/ARAX_ranker.py`)

| ID | Functionality | Details |
|---|---|---|
| RNK-01 | Attribute statistics | Min/max over trusted attributes, skipping inf and NaN. `"no value!"` is **mutated to 0** on the edge. There is a falsy-zero bug in the min test. |
| RNK-02 | Edge confidence | A `manual_agent` edge gets 0.90. **Otherwise an existing `confidence` attribute is used.** Otherwise the base weight by `edge_key.split('--')[-1]` (semmeddb 0.5, text-mining-provider-targeted 0.85, drugcentral 0.93, drugbank 0.99, other infores 0.5, else 0), combined with normalized attribute scores via `W += (1-W)·wᵢ`. SemMedDB publications use a logistic on log n. |
| RNK-03 | Per-result graph | QG MultiDiGraph excluding `creative_*`. The qedge weight combines average confidences with duplicate KP edges merged (`id.split(':',2)[-1]`). |
| RNK-04 | Scorers | Max flow, "longest path" `A^L/L!`, Frobenius norm. Each is quantile-ranked (`rankdata(max)/n`), then averaged, with a +0.001 floor. |
| RNK-05 | Output | `row_data=[score, essence, essence_category]`, `table_column_names`. Sort descending. Ties broken with 0.001 steps. Results beyond 1000 get 0. |
| RNK-06 | Timing | Runs **while virtual overlay qedges are still QG edges** (before RTF-*). Skipped for plans with `connect`. |

### 13. ResultTransformer (`AQ/result_transformer.py`)

| ID | Functionality | Details |
|---|---|---|
| RTF-01 | Scope | Every non-KG2 plan. Skipped when there are no results, for pathfinder QGs, and for xCRG. Requires the original QG (`NoOriginalQG`). |
| RTF-02 | Virtual edges → aux graphs | For `analyses[0]` only: bound qedges not in the original QG are grouped by option group, and each group becomes an aux graph `aux_graph_{sorted ids \| count}{_group}`. `creative_*` groups are attached as `biolink:support_graphs` on the inferred edge(s); the source is `infores:arax-xdtd` for `creative_DTD`, else `infores:arax`. Other groups go to `analysis.support_graphs`. |
| RTF-03 | Binding cleanup | Deletes virtual edge/node bindings and prunes orphan subclass-parent node bindings. |
| RTF-04 | **Creative NGD-inf filter** | When any inferred qedge exists: drops inferred bindings with no support graph, or whose support graph disconnects once `ngd=='inf'` edges are removed (undirected). Drops results that lose an original-qedge binding. As a result, **plain lookup treats edges are removed in creative mode**. |
| RTF-05 | Finalization | `total_results_count`, the FRS-07 prune, and `query_graph = original_query_graph`. |

### 14. Infer: xDTD (`AQ/ARAX_infer.py`, `AQ/Infer/*`)

| ID | Functionality | Details |
|---|---|---|
| INF-01 | `infer(action=drug_treatment_graph_expansion)` | Parameters: `drug_curie` / `disease_curie` (at least one), `qedge_id`, `n_drugs` 50, `n_diseases` 50, `n_paths` 25. Values are clamped to those maximums with a warning; ≤0 is an error. With a QG, the curie must be in a qnode's ids, and **every QG edge is mutated** to `inferred`, `[biolink:treats]`. |
| INF-02 | Score and path lookup | `ExplainableDTD_v1.0_<tier>-all_with_paths.db`: `PREDICTION_SCORE_TABLE`, `PATH_RESULT_TABLE`. **No `ORDER BY`**: the top-N is taken in DB row order, and paths are cut unsorted (D-15). Empty scores give a warning ("not trained or score < 0.3"). |
| INF-03 | Node/edge mapping | The same DB's `NODE_MAPPING_TABLE` and `EDGE_MAPPING_TABLE` hold categories, publications, sources chain, knowledge_level, agent_type, qualifiers and extra attributes. |
| INF-04 | Subgraph build (`genrete_treat_subgraphs`) | Without a QG it creates `drug`/`disease` qnodes and a `treats` qedge (inferred). The open qnode's categories are overwritten (Disease+PhenotypicFeature or Drug+SmallMolecule+ChemicalEntity). Per hop length L it adds `creative_DTD_qnode_k` (`is_set`) and `creative_DTD_qedge_k` in `creative_DTD_option_group_g`. A path with a missing triple is skipped. A missing node or a `SELF_LOOP_RELATION` **stops all remaining paths for that pair** and leaves partial edges. Path-edge `qedge_keys` are overwritten, not appended. |
| INF-05 | Prediction edge | `creative_DTD_prediction_{i}`, `biolink:treats`, `probability_treats` (`EDAM-DATA:0951`) = `str(tp_score)`, `created_datetime` **hard-coded "2026-06-28"**, `computational_model`/`prediction`, primary `infores:arax-xdtd`. |
| INF-06 | Path-edge provenance | Keeps the upstream source chain (including `source_record_urls`) and appends `infores:arax-xdtd` as aggregator. |
| INF-07 | Result ordering | Internal Resultify(ignore_edge_direction). `score = essence_scores[essence]`, keyed by **name** (names can collide). Sort descending. The ranker does not run for pure `infer` DSL. |
| INF-08 | `infer(action=chemical_gene_regulation_graph_expansion)` (legacy xCRG) | **Dead**: its models are no longer managed, and there are several bugs (D-16). Replaced by XCR-*. |

### 15. Creative "treats" in Expand

| ID | Functionality | Details |
|---|---|---|
| CRT-01 | Trigger | Single-qedge inferred `treats`/`ameliorates` runs **xDTD first (INF-01)**, then the qedge is relabelled `lookup` and **a normal lookup also runs**. The object (disease) must be pinned. |
| CRT-02 | Predicate widening | Adds `treats_or_applied_or_studied_to_treat` and `applied_to_treat` to the KP query. |
| CRT-03 | Heuristic prediction edges (`trapi_querier.py:182-285`) | Every returned non-`treats` treats-descendant edge becomes **unbound** and goes into aux graph `ARAX-prediction-auxgraph-<uuid5>`. One bound `ARAX-prediction-edge-<uuid>` (`biolink:treats`, `automated_agent`/`prediction`, primary `infores:arax`) is created per subject/object pair, with `support_graphs`. |
| CRT-04 | Elevation rules (issue 2634) | SemMedDB <10 pubs dropped; CTKP `elevate_to_prediction`; FAERS/DAKP `number_of_cases>24`; TMKP `evidence_count>5`; CTD. These live in `_handle_creative_treats_predicate_answers` and are **effectively a no-op** because CRT-03 already removed the edges (D-1). |
| CRT-05 | xDTD cache | Results are written to the KP cache under `xDTD`, but reads are disabled. |

### 16. Connect: pathfinder (`AQ/ARAX_connect.py`, `catrax-pathfinder==2.4.3`)

| ID | Functionality | Details | Shepherd |
|---|---|---|---|
| CON-01 | `connect(action=connect_nodes)` | `max_path_length` 1–5 (default 4), `max_pathfinder_paths` (default 500). Exactly 2 pinned qnodes and exactly 1 path. Source/destination come from **`path.subject`/`path.object`**. At most one `intermediate_categories` constraint. | **Ported with DEC-7's changes**: ARAX's validation is kept, but the search always uses 4 hops and 500 paths |
| CON-02 | Inputs to Pathfinder | Curies normalized through NodeNorm `preferred_curie`. Block list from the **repo's** `general_concepts.json`. `category_constraints` = descendants of the constraint, **or empty**. `prune_top_k=75`, `degree_threshold=10000`, `hops_numbers = max_hops_to_explore = max_path_length`. | **Ported**, except the fixed limits (DEC-7). The block list is the vendored copy at the pinned commit. |
| CON-03 | Data | `curie_ngd_v1.0_<tier>.sqlite`, `tier0-info-for-overlay_v1.0_<tier>.sqlite`. Retriever by maturity (`retriever[.ci\|.test].transltr.io/query`), with env override `ARAX_XCRG_RETRIEVER_URL`. | **Shepherd's**: the pathfinder data files, `SYNC_KG_RETRIEVAL_URL` (DEC-4, DEC-7) |
| CON-04 | Output | Rehydrate (`/rehydrate`, tier 0, 30 s). One result `id="result"`, `essence="result"`, `PathfinderAnalysis` with `path_bindings {"p0": …}` (p0 hard-coded). The KG is **merged** into the existing KG. No ranker and no transform. **Zero paths give a warning and no results.** | **Ported**, rehydrating at `KG_REHYDRATE_URL` |
| CON-05 | Cache | Written to the KP cache, but the read is effectively never used. | **Ported** (DEC-18): xCRG results are read back; a PathFinder result only when the incoming message already has results, as upstream |

### 17. Connect: xCRG (`catrax-xcrg` @ `c97da53`)

| ID | Functionality | Details | Shepherd |
|---|---|---|---|
| XCR-01 | MVP2 detection | Exactly one qedge, `knowledge_type=inferred`, `biolink:affects`, exactly one pinned end, chemical↔gene categories, direction `increased`/`decreased`, aspect a descendant of `activity_or_abundance`. | **Ported** |
| XCR-02 | `connect(action=xcrg)` config | Retriever URL (by maturity or env), `ngd_db_path`, `curie_to_pmids_db_path`, `timeout` (`ARAX_XCRG_TIMEOUT`, 210), `tf_batch_size` (`ARAX_XCRG_TF_BATCH_SIZE`, 200), `tiers=[0]`, `resource_id=infores:arax`, TRAPI 1.6.0, Biolink 4.2.5. The envelope's `parameters`/`submitter` are **not** passed. | **Ported**, Retriever at `SYNC_KG_RETRIEVAL_URL` (DEC-4; the env override is kept) |
| XCR-03 | Algorithm | Transcription-factor list (minus TP53 and the endpoints). A direct one-hop lookup plus two-hop lookups through TFs, in batches, with sign templates. Filters subclass/direction/TP53. Sorts: direct first, then by TF degree, id, specificity, IC, NGD. Rank score `(total−i)/total`. Output: `xcrg_support_*` / `xcrg_inferred_edge_*` / `xcrg_ngd_support_*`, up to `max_results` 500. | **Ported** (the `catrax-xcrg` package) |
| XCR-04 | Envelope handling | The message is replaced. Sets `total_results_count`, `data.xcrg_connect=True` (skips RTF), query plan `arax-xcrg`. Errors give HTTP 500. | **Ported** |

### 18. Operations and state (`AQ/ARAX_query_tracker.py`, `ARAX/ResponseCache/*`, `AQ/ARAX_background_tasker.py`, `AQ/ARAX_database_manager.py`)

| ID | Functionality | Details | Shepherd |
|---|---|---|---|
| OPS-01 | Query tracker | MySQL tables `arax_query` and `arax_ongoing_query`. States: started, Running Async, Completed, Died, Reset, Denied. Dead-PID detection. | **Shepherd's own** (its query table). `/status` shows it in ARAX's shape; `pid` is null, and there are no `Running Async` / `Reset` / `Denied` states or dead-pid checks. |
| OPS-02 | Kill token | `/status?terminate_pid=&authorization=hash('Pickles'+pid)` sends SIGTERM. | **Ported with a difference**: terminating ends the query's stream (all an ARAX client sees); the work still finishes in the worker |
| OPS-03 | Recent-query listing | `get_status(last_n_hours, mode=active)`. | **Ported** |
| OPS-04 | Load log | psutil samples written each minute to `ARAX_background_tasker_loadlog.txt`. | **Not ported** (Infra; `system_load` is `[]`) |
| OPS-05 | Response store | Row in `TRAPI_1_0_0_response` (MySQL in prod, SQLite otherwise) plus S3 `arax-response-storage[-2]` (bucket chosen by a config datetime). `envelope.id = https://arax.ncats.io/api/arax/v1.4/response/{id}`. Falls back to a local file. | **Shepherd's own** (DEC-3) |
| OPS-06 | Response retrieval and validation | See API-07: `reasoner-validator` 6.0.2 (the Biolink version is hard-coded to **4.4.2** here), provenance summary, ARS proxying, attribute stripping (`X`/`Z` prefixes). | **Ported** (DEC-3), parity-tested |
| OPS-07 | Background tasker (every 60 s) | Hourly KP-info refresh (EXP-29), hourly meta-KG refresh (AUX-01), ongoing-query check, KP-cache refresh (EXP-31), load log. | **Partial**: the hourly meta-KG refresh runs in the server; the SmartAPI list (query plan only) is cached for an hour in-process; the KP-cache refresh runs every minute in the arax worker (DEC-18); the ongoing-query check and load log don't apply |
| OPS-08 | Database manager | Downloads or symlinks the managed DBs (Part B §20) by rsync from `arax-databases.rtx.ai` or SFTP (ITRB). Tracks versions in `db_versions.json`. | **Shepherd's own** (`shepherd_utils/data_download.py`, DEC-6); the URLs are placeholders |

### 19. Auxiliary services

| ID | Functionality | Details | Shepherd |
|---|---|---|---|
| AUX-01 | ARAX `/meta_knowledge_graph` | Plover/Gandalf `/meta_knowledge_graph` (30 s) merged with every KP's meta map (edges tagged with a `biolink:knowledge_source` attribute) plus the standard attribute constraints (`original_predicate`, `knowledge_level`, `agent_type`). Cached for 1 h with JSON backups (keeps 3). The `simple` format gives predicates by category. | **Ported** (DEC-11), parity-tested |
| AUX-02 | Autocomplete (`code/autocomplete`, Tornado :4999) | Only `/nodeslike` works (prefix, then substring search on `autocomplete_v1.0_<tier>.sqlite`). `/auto`, `/fuzzy` and `/autofuzzy` are dead. | **Ported** at `/arax/rtxcomplete/nodeslike`, parity-tested. The UI calls it relative to its own host, so its deployment must route `/rtxcomplete/` there. |
| AUX-03 | Interactive UI (`code/UI/interactive`) | Static SPA. Depends on API-02 streaming, `/response`, `/status`, `/meta_knowledge_graph?format=simple`, `/entity` and autocomplete. | **External** (DEC-2) |

### 20. Data files a faithful port needs

| File (tier `tier0-20260621` unless noted) | Used by | In Shepherd |
|---|---|---|
| `curie_ngd_v1.0_<tier>.sqlite` | CON-*, XCR-* | Yes (the pathfinder download, shared with the `arax` worker) |
| `tier0-info-for-overlay_v1.0_<tier>.sqlite` (= `kg2c_sqlite`) | CON-*, OVL-05 (FET) | Yes (the pathfinder download, shared with the `arax` worker) |
| `curie_to_pmids_v1.0_<tier>.sqlite` | OVL-01, OVL-04, XCR-* | Download set up (`arax` worker); placeholder URL (DEC-6) |
| `ExplainableDTD_v1.0_<tier>-all_with_paths.db` | INF-* | Download set up (`arax` worker); placeholder URL (DEC-6) |
| `COHDdatabase_v1.0_KG2.8.0.db` (old KG) | OVL-02 | Download set up (`arax` worker); placeholder URL (DEC-6) |
| `fda_approved_drugs_v1.0.pickle` (tier0-20260408) | EXP-24 | Download set up (`arax` worker); placeholder URL (DEC-6) |
| `autocomplete_v1.0_<tier>.sqlite` | AUX-02 | Download set up (server); placeholder URL (DEC-6) |
| `general_concepts.json` | FKG-11, CON-02 | Yes: vendored in the ARAX library at the pinned commit. `arax.pathfinder` still reads GitHub `master` (C-7) |
| `transcription_factors.json` (bundled in `catrax-xcrg`) | XCR-03 | n/a |
| Biolink 4.2.5 YAML / BiolinkHelper pickle | BL-* | Yes |

### 21. External services contacted at runtime

SmartAPI (hourly), every SmartAPI-registered TRAPI 1.6.0 KP at the ARAX
maturity (its `/meta_knowledge_graph` hourly, plus per-query `/query`),
Retriever (`retriever[.ci|.test].transltr.io`, `/query` and `/rehydrate`),
Gandalf/Automat (`automat.renci.org/translatorkg-gandalf`, as rtx-kg2, for FET
and for the meta-KG), NodeNorm, Name Resolver, cohd.io (`biolink_to_omop`),
NCBI eUtils (add_node_pmids fallback, `/PubmedMeshNgd`), ICEES+ (likely
defunct), the ARS instances (response retrieval), S3, MySQL, and GitHub raw
(Biolink YAML, ICEES identifiers).

### 22. Tests and parity corpus available upstream

| Source | Coverage | Parity use |
|---|---|---|
| `ARAX/test/test_ARAX_infer.py` | xDTD DSL and QG variants, path-edge provenance | High (xDTD) |
| `ARAX/test/test_ARAX_connect.py` | connect_nodes DSL and constrained TRAPI | High (pathfinder; catches C-1) |
| `ARAX/test/test_ARAX_xcrg_connect.py` | MVP2 routing, config, integration | High (xCRG) |
| `ARAX/test/test_result_transformer.py`, `test_compute_ngd.py` | Unit tests | High |
| `test_ARAX_expand/_resultify/_overlay/_filter_kg/_filter_results/_ranker/_messenger/_workflows/_json_queries/_query` | Core modules | Per area |
| `ARAX/test_async_demo_workflows/2021-12_demo/` | 19 TRAPI workflow JSONs plus a two-endpoint diff script | **Ready-made ARAX-vs-Shepherd corpus** |
| `ARAX/test/conftest.py` flags | `--runslow`, `--runexternal`, `--withdatabases` | — |

---

## Part C: Divergences in Shepherd's existing ARAX ports

These were found while comparing the native workers with upstream. **C-1 and
C-2 affect results today** and are worth fixing whether or not the full port
goes ahead.

**Since the port:** C-2 and C-10 are fixed (DEC-8). C-1 and C-3 to C-6 remain in
the `arax.pathfinder` worker, which answers TRAPI pathfinder queries; under DEC-7
they are not parity gaps and wait for the bug-fix pass. C-7 now only affects that
worker. C-8, C-9 and C-11 concern Shepherd's own workflow steps, which ARAX
queries no longer use (ARAX runs its own actions in-process). C-12 still applies.

| ID | Where | Divergence | Impact |
|---|---|---|---|
| **C-1** | `workers/arax_pathfinder/worker.py:190` | With no path constraint, Shepherd passes the descendants of `biolink:NamedThing`; ARAX passes an **empty set**. catrax-pathfinder applies `filter_with_constraint` whenever the set is non-empty (`Pathfinder.py:199`), and that filter only checks interior edges. | **Every 1-hop and 2-hop path is dropped** on unconstrained queries. Results differ from ARAX. |
| **C-2** (fixed, DEC-8) | `workers/arax_rank/ranker.py:319-327` (before) | `_extract_data_source` tries `parts[0]` before `parts[-1]`. ARAX uses only `split('--')[-1]`. For ARAX-style keys (`infores:kp:subj--…--infores:semmeddb`) this gives `infores:kp:subj`. | Wrong base weights (semmeddb/drugbank/… never applied, SemMedDB publications rule never fires) once edges carry ARAX keys. Masked today because Retriever keys fall through to `sources`. Also, non-ARAX-keyed virtual edges get base 0.5 here but 0 in ARAX. |
| C-3 | `workers/arax_pathfinder/worker.py:163-170` | Source/destination come from qgraph node order, not `path.subject`/`path.object`. `pinned_node_keys` includes unpinned nodes. | The odd-hop BFS split is direction-sensitive, so results can differ. |
| C-4 | `workers/arax_pathfinder` | `max_path_length` and `max_pathfinder_paths` from `query_options` are ignored (hard-coded 4/500). No 1–5 range check. | Missing options. |
| C-5 | `workers/arax_pathfinder` | Zero paths return one result with `analyses=[]`; ARAX returns no results. | Output difference. |
| C-6 | `workers/arax_pathfinder` | No NodeNorm canonicalization of the pinned curies. | Different start nodes for non-canonical input. |
| C-7 | `shepherd_utils/data_download.py:392` | The block list comes from GitHub `master` once and is never refreshed. ARAX uses its repo copy at the deployed commit. | Version drift. |
| C-8 | `shepherd_utils/shared.py` (`filter_kgraph_orphans`) | No "both-endpoints" edge retention. Dangling results are not dropped. Partial rather than whole-aux-graph rejection. Handles `path_bindings` (ARAX does not). | KG contents differ from FRS-07. |
| C-9 | `filter_kgraph_orphans` op vs ARAX WF-12 | ARAX's `filter_kgraph_orphans` workflow op means `remove_orphaned_nodes` (no incident edge, ignores results). Shepherd's means "prune to referenced". | Same op id, different semantics. |
| C-10 (fixed, DEC-8) | `workers/arax_rank` (before) | Missing the `confidence`-attribute override, `row_data`/`table_column_names`, and the `"no value!"`→0 mutation. Adds `edge["confidence"]` to the output KG (ARAX never serializes it). Fails soft where ARAX errors (returns the message unranked). Can create analyses without `resource_id`/`edge_bindings`. | Minor output and score differences. |
| C-11 | `workers/sort_results_score`, `filter_results_top_n` | Defaults (descending / 500), None→0, no KG pruning, no `n_results`. | See FRS-01/02. |
| C-12 | `inject_shepherd_arax_provenance.py` | Always sets `upstream_resource_ids=[infores:arax]`, even when the previous source is Retriever (pathfinder edges). | Provenance chain inaccuracy. |

---

## Part D: Latent ARAX defects (decision needed: reproduce or fix?)

**Decided (DEC-1): every defect below is reproduced in the port.** Fixes wait
for a separate pass after the port has been validated, and each one is then
recorded as a numbered deviation. D-3 and D-4 do not apply, because the
code they live in is not ported (DEC-4). D-5 applies in part (DEC-18): timeouts
are cached and aiohttp HTTP errors propagate, as upstream, but TLS is verified.

| ID | Defect | Where |
|---|---|---|
| D-1 | Creative-treats elevation rules (CRT-04) never run, so every treats-like edge is elevated. | `ARAX_expander.py:760-874` vs `trapi_querier.py:182-285` |
| D-2 | The knowledge-source post-filter `break`s after the first edge. The denylist can never match because every edge carries `infores:arax`. `primary_knowledge_source` is accepted but not enforced. A string constraint value is split into characters. | `ARAX_expander.py:697-729`, `expand_utilities.py:765-781` |
| D-3 | A KP-info cache older than 24 h makes Expand error. | `kp_info_cacher.py:132-142` |
| D-4 | `infores:retriever` is always a KP candidate even when unregistered, and an early error can halt `apply()`. | `kp_selector.py:22`, `ARAX_expander.py:1067-1141` |
| D-5 | The KP cache caches timeouts, uses `ssl=False`, and does not catch aiohttp HTTP errors. | `trapi_query_cacher.py` |
| D-6 | `inject_boolean_value_into_parameters` always overwrites DSL-supplied `return_minimal_metadata`. | `ARAX_query.py:1039-1044` |
| D-7 | NGD label-only mode keeps only the last qnode pair's qedge. | `compute_ngd.py:90-244` |
| D-8 | COHD `chi_square` reports the p-value of the largest statistic. The QEdge is created with `predicates` as a string. | `overlay_clinical_info.py`, `COHDIndex.py:2498` |
| D-9 | `add_node_pmids` looks up by the original curie, not the canonical one. | `add_node_pmids.py:221` |
| D-10 | ICEES virtual mode raises TypeError. | `overlay_exposures_data.py` |
| D-11 | A disallowed filter_kg/filter_results value is only a warning, and the action silently does nothing. | `ARAX_filter_kg.py`, `ARAX_filter_results.py` |
| D-12 | The `remove_edges_by_percentile` default becomes -94 for ngd-like attributes, and the action errors. | `ARAX_filter_kg.py:1017` |
| D-13 | `remove_orphaned_nodes(node_category)` ignores the category. | `remove_nodes.py:193` |
| D-14 | `sort_by_edge_attribute`/`sort_by_node_attribute` compare `qedge_keys`/`qnode_keys` to KG ids. `node_category` is compared to a list. | `sort_results.py` |
| D-15 | xDTD top-N and paths are not score-sorted (no `ORDER BY`). | `ExplianableDTD_db.py:286,315`, `ARAX_infer.py:570` |
| D-16 | Legacy xCRG infer: the `kp` check always fails, `int.is_float`, wrong variable, `describe` KeyError, missing models. | `ARAX_infer.py:588-876` |
| D-17 | Category-specific QGI templates can never match (the list vs string comparison), so the drug-treats-disease templates are unreachable. | `query_graph_info.py:348` |
| D-18 | `envelope.description` is stale; `/asyncquery` returns no `job_id`; `/asyncquery_status` logs are always empty. | `ARAX_query.py:893,957`, tracker |
| D-19 | Non-determinism: Resultify's start qnode is `list(set)[0]`; the NGD PMID subset uses `islice` on a set. | `ARAX_resultify.py`, `compute_ngd.py` |
| D-20 | Ranker `UnboundLocalError` with publications n=0; `KeyError`s on dangling bindings. | `ARAX_ranker.py:431` |
| D-21 | `query_return_stream` checks whether its query thread is already done *before* its loop, so a query that finishes first (an input error, say) streams **nothing**, not even the envelope. The Shepherd worker then saves the finished response's envelope, so the stream still ends with it. | `ARAX_query.py:98` |
| D-22 | `/response/{id}` returns the response with its `message` emptied whenever reasoner-validator raises (for example, when it cannot fetch the TRAPI schema from standards.ncats.io): the validator pops `message`, validates against a `{}` stub and only restores it at the end. The UI then shows an empty response. Fix: validate a copy. | `response_cache.py` `get_response` (validation block); reasoner-validator 6.0.2 `check_compliance_of_trapi_response` |
| D-23 | `filter_kg`'s `remove_edges_by_top_n`, `remove_edges_by_percentile`, `remove_edges_by_std_dev`, `remove_edges_by_continuous_attribute`, `remove_edges_by_discrete_attribute`, and `filter_results(..., prune_kg=true)`, read `edge.qedge_keys` of every KG edge. Edges the KP returned only inside a support graph are kept in the KG as unbound edges, without `qedge_keys`, so these crash (`'Edge' object has no attribute 'qedge_keys'`) whenever the answer includes support-graph edges, which Retriever returns for many queries. That covers the `filter_kgraph_*` workflow operations as CQS-style workflows use them. | `ARAX_filter_kg.py` (the `'qedge_keys': set([...])` in each of those actions), `Expand/trapi_querier.py` (unbound edges kept for aux graphs) |
| D-24 | `filter_kg(action=remove_edges_by_stats)` is defined (`command_definitions`, `describe_me`) but not in `allowable_actions`, so it is always refused. `filter_results(action=sort_by_node_attribute)` on a string attribute raises in `sort_results.py`. A workflow's `filter_results_top_n` with a string `max_results` fails an `assert` as an unhandled error. | `ARAX_filter_kg.py`, `Filter_Results/sort_results.py`, `operation_to_ARAXi.py` |
| D-25 | A TRAPI qnode's `name` is accepted but not resolved ("QueryGraph has no nodes with ids"); query by name works through ARAXi's `add_qnode(name=...)`. A set qnode's `member_ids` (and its categories, with `set_interpretation: ALL`) are not forwarded to the KP. | `ARAX_query.py` (allowed qnode attributes), `trapi_querier.py` (`_strip_empty_properties`) |
| D-26 | `overlay_connect_knodes` computes Jaccard over every node triple, which divides by zero when a pair shares no neighbors, failing the plan. The port's E-4 change removes the disabled overlay action that made the operation fail even earlier upstream. | `operation_to_ARAXi.py`, `Overlay/compute_jaccard.py` |

---

## Part E: Dead, disabled or unreachable in ARAX (confirm drop)

**Decided (DEC-5):** all of it is removed and not ported.

- **E-1:** `overlay(predict_drug_treats_disease)` plus `Overlay/predictor`,
  `Overlay/GraphSage_train`, `Expand/DTD_querier.py` and
  `Infer/scripts/model_utilities.py` (the old GraphSage/neo4j DTD stack).
- **E-2:** `infer(chemical_gene_regulation_graph_expansion)`, `creativeCRG.py`
  and `genrete_regulate_subgraphs` (legacy xCRG; its models are unmanaged).
- **E-3:** Templates `one_node_with_categories`, `one_node_with_no_categories`,
  `one_hop_classic_question_curie2ChemicalSubstance`,
  `one_hop_drug_treats_disease`, `one_hop_drug_somehow_related_to_disease`,
  `two_hop_drug_disease` and `four_node_forked_two_pinned_question`
  (unreachable; several also reference disabled actions).
- **E-4:** DSL `fetch_message`, `filter` and `remove_qedge`; workflow ops
  `sort_results_edge_attribute` and `sort_results_node_attribute`; the
  `predict_drug_treats_disease` step inside `overlay_connect_knodes`.
- **E-5:** RTXKG2 mode (no caller).
- **E-6:** `ARAX_decorator.py` (unused, stale schema), `knowledge_graph_info.py`,
  and `ARAX_filter.py`.
- **E-7:** `/translate`, `/exampleQuestions`, `/PubmedMeshNgd` (legacy), and the
  autocomplete endpoints `/auto`, `/fuzzy` and `/autofuzzy`.
- **E-8:** `overlay_exposures_data` (ICEES+ is likely defunct and virtual mode
  crashes).
- **E-9:** Expand dead helpers (`filter_response_domain_range_exclusion`,
  `remove_semmeddb_edges_and_nodes_with_low_publications`, …); config keys
  `node_normalizer_url_override` and `name_resolver_url_override`.
- **E-10:** Code in the repo that is not part of the service: `code/kg2c`,
  `code/kg2`, `code/reasoningtool`, `code/code-archive` and the build scripts
  (NGD/tier0/FDA/COHD/xDTD DB builders).

---

## Part F: Open questions for the ARAX team

1. **Scope of "faithful"** (decided: DEC-1 parity; DEC-2 Shepherd serves
   everything in the [UI contract](#ui-contract-what-the-arax-ui-requires);
   DEC-18 the KP cache is kept, in Shepherd's data store;
   DEC-7 the pathfinder options are ignored). Nothing left open.
2. **KP roster** (decided: DEC-4, queries use Retriever only, which covers
   Gandalf; SmartAPI kept for the UI and for the query-plan provider list only;
   DEC-9, no curie-prefix conversion; DEC-10, a `kp`/`fill` list is forwarded
   to Retriever as `parameters.kp`, as the infores list given; DEC-11, the
   meta-KG comes from Retriever, with ARAX's additions; DEC-12, every
   SmartAPI-listed provider except Retriever is shown as Skipped). Nothing
   left open.
3. **Part D** (decided: DEC-1, reproduce everything, fix after validation).
4. **Part E** (decided: DEC-5, all removed).
5. **Data delivery** (decided: DEC-6, same mechanism as the pathfinder DBs,
   with placeholder URLs to adjust later; COHD stays on KG2.8.0).
6. **KP response cache** (decided: DEC-18, ported, in Shepherd's data store;
   first left out by DEC-3).
7. **Result storage** (decided: DEC-3, `/response/{id}` does everything it
   does in ARAX, reading from Postgres. ARS PK/UUID paths read from Shepherd's
   hosted ARS in Postgres. `envelope.id` is
   `{settings.server_url}/arax/response/{id}`). Nothing left open.
8. **Version pins** (decided: DEC-7 makes the catrax-pathfinder pin moot;
   DEC-13 bumps biolink-helper-pkg to 1.0.1).
9. **Async queries** (decided: DEC-15, Shepherd's own async logic).
10. **Concurrency limit** (decided: DEC-16, none).
11. **TRAPI workflows for ARAX** (decided: DEC-17, passed through for ARAX to validate).
