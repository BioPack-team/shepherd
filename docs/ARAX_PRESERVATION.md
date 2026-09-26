# ARAX in Shepherd: what is preserved, and the tests that hold it there

The ARAX team asked that the port keep five things working and, if possible,
some flexible query features. This page answers each point: what the port
does, which tests guard it, and what still needs live services to check. The
decisions (DEC-*) and defects (D-*) it refers to are in
[ARAX_PORT_BASELINE.md](ARAX_PORT_BASELINE.md).

## How the checks work

Three kinds of test, all in `tests/unit/` and run in CI:

| Kind | What it compares | Where |
|---|---|---|
| **Upstream parity** | The port against goldens recorded by running **upstream ARAX's own code** (RTXteam/RTX @ 9485431) on the same inputs: a mock Retriever, synthetic data files, and deterministic stand-ins for NodeNorm, COHD's web lookup and NCBI eUtils. Every field is compared: status, message, envelope, log lines, and every request sent to the KP. The port may differ only where a recorded decision says so, and each such difference is pinned by its own test. | `arax/test_query_parity.py` (75 end-to-end `ARAXQuery` cases), `arax/test_expand_parity.py` (37), `arax/test_response_parity.py` (28 `/response/{id}` lookups), `arax/test_aux_parity.py` (meta-KG and autocomplete) |
| **Upstream's own test suite** | Upstream's `code/ARAX/test/*.py`, unmodified, run against the port through an import shim. | `arax/upstream_suite/` (321 tests) |
| **Shepherd-side tests** | The pieces that are Shepherd's own: the worker, the server's `/arax` routes, the KP cache store, the data files. | `arax/test_trapi_query_cacher.py`, `test_arax.py`, `test_arax_api.py`, `arax/test_ARAX_connect.py`, `arax/test_mock_data.py` |

Upstream's suite has 321 tests. Offline, 81 of them pass on upstream ARAX
itself, and those 81 pass on the port. Two more are the port's recorded
DEC-4 difference and run as strict xfails. The other 238 need the Translator
services or ARAX's real databases on upstream too. Running them is the live
check (see [Needs live services](#needs-live-services)).

## 1. The ARAX UI

**What the port does.** DEC-2 keeps the UI hosted outside Shepherd. Shepherd
serves every endpoint the UI calls under one base path, `/arax`, so the UI's
`config.js` points at it (`config.baseAPI = '/arax'`). The UI's host must route
`/rtxcomplete/nodeslike` to `/arax/rtxcomplete/nodeslike`. It keeps serving
`/rtxcomplete/` (the typeahead scripts and `quick_def.js`) as ARAX's host does.

**Confirmed with the real UI.** Upstream's UI (`code/UI/interactive` @ 9485431,
unmodified) was run in a browser against Shepherd's `/arax` API and the real
arax worker, set up as above. These all worked:

- ARAXi, TRAPI JSON and workflow-builder queries, with the streamed log,
  progress bar and query-plan table;
- the rendered results;
- loading a response by id and by `?r=` link;
- System Activity and KP Cache Info;
- node-name autocomplete;
- terminating a running query;
- loading an ARS parent PK, an ARA's child PK, and an attribute's detail.

Every endpoint behind these has its own tests in `test_arax_api.py`: the stream
relay, `/status` in all its modes, terminate, `/response`, `/entity`,
`/meta_knowledge_graph` and autocomplete.

**Found and fixed while checking:** the UI sends `X` + id for every id that is
not a number, meaning an ARS PK. Shepherd's response ids are hex, not ARAX's
integers, so loading a Shepherd response in the UI would always have missed.
The port now accepts that prefix on a Shepherd id (DEC-19).

**Not covered:** UI pages that call services other than ARAX: the uptime
monitor, ARS test-runner artifacts, PloverDB, and ARS submit. They don't
depend on Shepherd. The SmartAPI listing (`/status?authorization=smartapi`)
needs network access to SmartAPI.

## 2. The response and PK caching

**What the port does.** `/response/{id}` is ARAX's `get_response`, reading
Shepherd's storage (DEC-3):

- local responses: Shepherd's own ids;
- URLs;
- ARS parent and child PKs: read from Shepherd's own hosted ARS, so any ARA's
  child PK comes up, not only ARAX's;
- `X` attribute stripping with `detail_lookup`, and the `Z` component cache,
  kept in Shepherd's data store for 7 days.

The KP response cache is ported too (DEC-18). It holds every KP response
Expand gets and Connect's results, stored in Shepherd's Redis and shared by
every worker. `bypass_cache` works, the background refresh runs, and the UI's
KP cache view lists it.

**Tests.**
- `test_response_parity.py` runs 28 lookups against upstream: local, URL, ARS
  parent and child, `X` then `Z` then cached, and every error path.
- The UI check above loaded a parent PK, and a child PK of another ARA
  (Aragorn), through Shepherd's real ARS routes.
- `test_trapi_query_cacher.py` has 20 tests of the KP cache: hits, timeouts,
  errors, `bypass_cache`, expiry, the refresh rules, the listing, and a data
  store outage.
- `test_ARAX_connect.py` covers xCRG results read back from the cache.

**Found:** D-22. When reasoner-validator raises, for example because it
cannot fetch the TRAPI schema, `/response/{id}` returns the response with its
`message` emptied, and the UI shows an empty response. This is upstream
behaviour, reproduced under DEC-1. The fix is one line: validate a copy.

## 3. xDTD / MVP1

**What the port does.** It is ARAX's Infer (`drug_treatment_graph_expansion`)
and Expand's creative-treats path, reading the ExplainableDTD database from
`ARAX_DBS_DIR`.

**Tests.** Query-parity cases, all compared with upstream:
- `creative_treats` and `creative_treats_no_prediction`: TRAPI inferred
  treats.
- `mvp1_two_diseases`, and `mvp1_with_workflow` (MVP1 sent with a workflow).
- `dsl_infer_no_qg` and `dsl_infer_with_qg`.
- `xdtd_infer_params` (`n_drugs`, `n_paths`), `xdtd_infer_drug_curie` (the
  drug-first direction) and `xdtd_infer_bad_param`.

Also, `test_mock_data.py` runs Infer on the mock ExplainableDTD, and the
upstream suite's `test_ARAX_infer.py` covers the rest with `--arax-live`.

## 4. xCRG / MVP2

**What the port does.**
- **Routing:** the query graph interpreter recognizes an MVP2 query
  (`is_xcrg_mvp2_query`) and runs `connect(action=xcrg)`, which calls the
  catrax-xcrg package with Shepherd's Retriever.
- **Artifacts:** its database artifacts are `curie_ngd` (from the pathfinder
  data) and `curie_to_pmids` (from `ARAX_DBS_DIR`). The transcription-factor
  list ships inside the package.
- **Cache:** results are cached as in ARAX.

**Tests.**
- **`mvp2_xcrg_route` (query parity):** a TRAPI MVP2 query runs the real xCRG
  package against the mock Retriever, with both database artifacts present on
  both sides (487 transcription factors in 3 batches, 5 results), and matches
  upstream.
- **Upstream's `test_ARAX_xcrg_connect.py`:** 6 tests pass; 2 are xfails for
  DEC-4 (Shepherd's Retriever instead of a per-maturity URL).
- **`test_ARAX_connect.py`:** Shepherd's wiring, the cache and `bypass_cache`.
- **`test_mock_data.py`:** xCRG reads both mock artifacts.

## 5. ARAXi and workflows (including what the CQS sends)

**What the port does.**
- **Workflows:** a TRAPI query's `workflow` reaches ARAX unchanged (DEC-17).
  ARAX translates it to ARAXi (`operation_to_ARAXi`) and runs it, so CQS-style
  workflows run as in ARAX.
- **ARAXi:** every command and action is ARAX's own.

**Tests** (query parity, all compared with upstream):

| Area | Cases |
|---|---|
| ARAXi commands | `create_envelope`, `add_qnode` (ids, categories, name, `is_set`, option groups), `add_qedge` (incl. `exclude`), `add_qpath`, `expand`, `overlay`, `filter_kg`, `filter_results`, `resultify`, `scoreless_resultify`, `rank_results`, `infer`, `connect` (`xcrg` through MVP2; `connect_nodes` in `test_ARAX_connect.py`, since its search limits are Shepherd's by DEC-7), `return`; unknown commands, parse errors, a failing action stopping the plan |
| `overlay` actions | `compute_ngd` (both modes), `compute_jaccard`, `fisher_exact_test` (with and without `rel_edge_key`), `overlay_clinical_info` (paired frequency, observed/expected, chi-square), `add_node_pmids` |
| `filter_kg` actions | all 11: by predicate, continuous attribute, std dev, percentile, top n, discrete attribute, node property, node category, orphans, general concepts, and the refused `remove_edges_by_stats` |
| `filter_results` actions | all 6: by edge attribute, edge count, node count, score, limit, and by node attribute (only on a string attribute, where upstream raises: D-24) |
| Workflow operations | all 19 that `operation_to_ARAXi` implements: `lookup`, `lookup_and_score`, `fill` (plain, `qedge_keys`, `allowlist`, `denylist` error), `bind`, `complete_results`, `score`, `sort_results_score`, `filter_results_top_n`, `overlay_compute_ngd`, `overlay_compute_jaccard`, `overlay_fisher_exact_test`, `overlay_connect_knodes`, `filter_kgraph_orphans`, `filter_kgraph_top_n`, `filter_kgraph_std_dev`, `filter_kgraph_percentile`, `filter_kgraph_continuous_kedge_attribute`, `filter_kgraph_discrete_kedge_attribute`, `annotate_nodes`; plus a CQS-style lookup, score, sort, top-n; `runner_parameters`; and an unknown operation |

Upstream's `test_ARAX_translate.py` (translation) and `test_ARAX_workflows.py`
cover more with `--arax-live`.

**Found:** upstream defects that CQS-style workflows hit. The port reproduces
them (DEC-1):
- **D-23:** the `filter_kgraph_*` operations crash whenever the KP's answer
  includes support-graph edges, because those edges are kept without
  `qedge_keys`.
- **D-24:** `filter_results_top_n` with a string `max_results` fails an
  assertion.
- **D-26:** `overlay_connect_knodes` fails in Jaccard. The port's E-4 change
  removes the disabled overlay action that made it fail sooner upstream.

## Nice-to-haves

| Feature | What the port does | Cases |
|---|---|---|
| "not" (`exclude: true`) | As ARAX (Expand's kryptonite edges) | `araxi_exclude`, `trapi_exclude`, `trapi_optional_and_exclude_mixed`; Expand parity `kryptonite`, `fda_constraint_not` |
| "any" / "all" (`is_set`, `set_interpretation`, `member_ids`) | As ARAX: accepted and sent to the KP. `member_ids` and a set qnode's categories are not forwarded, upstream included (D-25) | `trapi_set_interpretation_all`, `trapi_set_interpretation_many_and_is_set`, `araxi_is_set_and_option_group` |
| Optional groups | As ARAX | `optional_group_query`, `araxi_is_set_and_option_group`, Expand parity `optional_group` |
| Query by name | ARAXi's `add_qnode(name=...)`, resolved through NameRes. A TRAPI qnode's `name` is not resolved, upstream included (D-25). The UI resolves names itself, through autocomplete and `/entity` | `araxi_create_envelope_and_name`, `araxi_unknown_name`, `trapi_qnode_name`, `dsl_infer_no_qg` |

## Needs live services

The offline checks prove the port does what upstream ARAX's code does on the
same inputs. The last step is live: real KPs, real NodeNorm, and ARAX's real
data files.

1. **Upstream's suite in full.** Where the services are reachable and the
   real data files are in place, run
   `pytest tests/unit/arax/upstream_suite --arax-live --runslow --runexternal`.
   Run the same suite in an upstream RTX checkout, then compare the two
   failure lists. See `tests/unit/arax/upstream_suite/README.md`.
2. **Side by side with production ARAX.** Send the same queries to
   arax.ncats.io and to Shepherd's `/arax`, and compare the answers.
   `scripts/test_shepherd.py` and `scripts/test_async.py` run queries against
   Shepherd.
3. **The UI against a deployed Shepherd.** Point the UI's `config.js` at
   `https://<shepherd>/arax` and route `/rtxcomplete/nodeslike` there.
