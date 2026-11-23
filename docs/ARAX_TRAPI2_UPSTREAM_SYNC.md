# ARAX port: upstream `trapi2.0` changes to port

This is the list of upstream changes Shepherd's ARAX port needs to take to
catch up with upstream's TRAPI 2.0 branch.

- **Port baseline:** `RTXteam/RTX` `master` @ `9485431` (see `shepherd_utils/arax/README.md`).
- **Target:** `RTXteam/RTX` `trapi2.0` @ `3d4ce77` (2026-11-16). It descends from
  `9485431`, with 185 commits.
- **Method:** `git diff 9485431 origin/trapi2.0` for every upstream file the
  port is based on (54 files; 46 changed), plus `config_dbs.json`,
  `requirements.txt`, the new FastAPI `openapi_server/impl/*`, the models, and the UI's
  API calls. Each hunk was checked against the port.

Most of the upstream diff needs no work. It is one of three things:
- upstream doing its own TRAPI 2.0 conversion, which the port already did (DEC-20);
- pydantic plumbing (`_qnode_keys`, `model_construct`, Flask→FastAPI);
- removal of KG2, Plover or dead code that the port never took (DEC-4, DEC-5).

What is left is below.
Tags:

- **PORT**: a bug fix or behavior change to take.
- **DATA**: a data file or version bump.
- **DECIDE**: upstream's TRAPI 2.0 choice differs from the port's. This needs a DEC
  entry before porting either way.

"Goldens" means `tests/unit/arax/*_parity` goldens, derived through
`tests/unit/arax/trapi2_goldens.py`.

---

## 1. Bug fixes and behavior changes (PORT)

### Orchestration (`ARAX_query.py`, interpreter, messenger)
1. **`asynchronous_query` catch-all** (775b97a1). It catches any `Exception`,
   records `UncaughtError` and still sets `DONE`. The port catches only
   `MemoryError` (`ARAX_query.py:318-333`). An exception outside the action loop
   (`from_dict`, the interpreter) means `DONE` is never set. `query_return_stream`
   then loops, and the `arax` worker hangs. **Highest priority.**
2. **Input QG validation** (07195cce, 43e2a643):
   - non-Biolink predicates are answered with `InvalidPredicate` (400);
   - `constraints.qualifiers` type ids that are not Biolink qualifier slots are
     answered with `InvalidQualifier` (422).
3. **`parameters.tiers != [0]` warning** (2f78aa95). The port always sends tier 0
   and stays silent.
4. **`parameters.log_level`** (2f78aa95). Filter the streamed progress lines by it.
   Delivered logs are already filtered in `workers/arax/worker.py`. This changes the
   ORC-05 note in the `ARAX_query.py` header.
5. **`data_release_versions`** on the envelope (07195cce):
   `{"infores:arax": "<tier0 build>"}`, set in `ARAX_messenger.create_envelope`
   from the tier version settings.
6. **Interpreter skips NGD under constraints** (615baff3, d83d0686). If any qedge
   has `constraints.knowledge_level`, `agent_type` or `qualifiers`, drop the
   `overlay(action=compute_ngd…)` steps from the template DSL.
7. **`overlay_connect_knodes` rewrite** in `operation_to_ARAXi.py` (2add4986):
   - one `compute_ngd` per qnode pair (`connect_knodes_ngd_{s}_{t}`);
   - **no COHD `paired_concept_frequency` step**;
   - FET only for qedge-connected pairs;
   - Jaccard only when the intermediate qnode is connected to both ends.

   Update `upstream_suite/test_ARAX_translate.py`.
8. **`query_options` → `additional_properties`** (18ae5be1). The new UI sends
   these in `additional_properties`:
   - `prune_threshold`
   - `max_pathfinder_paths`
   - `max_path_length`

   It sends `bypass_cache` in `parameters`. The port reads only `query_options`
   (`ARAX_query.py:435-447,879-893,1047`, `ARAX_query_graph_interpreter.py:87-94`).
   Read both (merged).

   See item D4 for where `query_plan` goes and how `parameters.timeout` is read.

### Expand
9. **A `sources` ALLOW constraint must not filter KPs** (2f78aa95). Apply it only to
   the returned edges. Today, DEC-20(e) feeds an ALLOW list into
   `ARAX_expander.py:429-432`, which removes Retriever, so **no KP is queried**.
10. **Knowledge-source constraint loop**: remove the stray `break`s, so that every
    offending edge is removed, not just the first (`ARAX_expander.py:~740,744`;
    2f78aa95, de6863c3).
11. **`biolink:evidence_count` qedge constraint** (`>`, `<`, `==`), applied as a
    post-filter. Add `_apply_qedge_attribute_post_filters` and
    `_edge_satisfies_numeric_constraint`. Today it is rejected as `UnsupportedConstraint` (2f78aa95).
12. **Retriever request body** (`Expand/trapi_querier.py:533-578`):
    - strip the qnode `biolink:highest_FDA_approval_status` constraint (ARAX
      applies it itself; 2add4986);
    - strip the qedge `biolink:evidence_count` constraint, dropping any
      containers left empty (2f78aa95);
    - expand `constraints.agent_type.values` with the Biolink `AgentTypeEnum`
      subtypes (2f78aa95; needs an enum source such as bmt).
13. **Re-point KP aux graphs at re-keyed edges** in `_load_kp_json_response`
    (2add4986). Today a support graph that cites a bound edge is left
    dangling.
14. **Rewrite the unbound-KG and aux-graph merge** (2add4986), with the new
    `eu.get_support_graph_ids`:
    - merge only support edges reachable through `support_graphs` from edges
      still in the KG, transitively, and only when both endpoints exist;
    - keep only the aux graphs that are referenced;
    - aux edges may be bound edges.
15. **`convert_qg_organized_kg_to_standard_kg`**: unbound nodes and edges must not
    overwrite bound ones (`expand_utilities.py:290-293`; 2add4986).
16. **`_prune_kg` / `_remove_dead_end_paths`** (2add4986):
    - add the orphan sweep after `create_results`;
    - clearing `is_set` on a COLLATE qnode must also clear `set_interpretation`.
      Today `.is_set = False` does nothing (`ARAX_expander.py:1453` together
      with `q_node.py:142`), so FET pruning treats a collated qnode as a set.
17. **`_add_subclass_of_edges`**: a missing parent node gets
    `categories=qnode.categories or ["biolink:NamedThing"]`. Today it is a bare
    `Node()`, which is invalid TRAPI 2.0.

### KP cache (`Expand/trapi_query_cacher.py`, `ARAX_connect.py`)
18. **Clear stale refresh fields on re-store** (c72c8463). Add the six
    `last_*refresh*` keys, set to None, to `fields` in `store_response`.
19. **Expire non-http (xCRG/PathFinder) entries after 6 h** (a9665495). This matters
    more in Shepherd, because the Redis TTL is 3 days.
20. **Cached timeouts** (d4585991):
    - a lookup on a `-1` entry older than 0.02 h is a miss;
    - `refresh_cache` skips timed-out entries.

    Rewrite `test_refresh_retries_a_cached_timeout_after_a_minute`.
21. **Connect: never reuse or store empty or failed xCRG/PathFinder answers**
    (a9665495; `ARAX_connect.py:304-355`).
22. **Connect reads `bypass_cache` from `parameters`** as well as `query_options`
    (`ARAX_connect.py:293-298`).
23. Adapt upstream's new `test_trapi_query_cacher.py` to the fakeredis fixture.

### Resultify, result transformer, ranker
24. **Sort binding ids**: `NodeBinding(ids=sorted(...))` and
    `EdgeBinding(ids=sorted(...))`. Today the order depends on the hash seed
    (`ARAX_resultify.py:1221,1225`).
25. **Final KG cleanup drops edges no result or support graph references**:
    - add `apply_both_endpoints_rule` to `analyze_message_get_referenced_IDs`;
    - pass `False` from `result_transformer.py:~326`.

    TRAPI 2.0 forbids unreferenced KG edges. This changes the KG in the goldens.
26. **`_prune_orphan_node_bindings` guard** (`result_transformer.py:217-226,290-300`).
    It indexes `node_bindings[...]` unguarded, so a missing binding raises `KeyError`.
27. **Ranker `row_data` must not be `null`** (2.0 schema):
    - map None to `""` for score, essence and essence_category;
    - apply this in **both** `shepherd_utils/arax/ARAX_ranker.py:776,791` and
      `workers/arax_rank/ranker.py:687-703`;
    - update `tests/unit/test_arax_rank.py`.

### Filters
28. **`remove_nodes._is_general_concept`** (56da024c, #2909). Typed
    `biolink:equivalent_identifiers`, `same_as` and `synonym` (and generic
    `same_as`) now match the blocklist. Add upstream's parametrized test. More
    Retriever nodes are removed, so the goldens change.
29. **`filter_kg` `remove_edges_by_discrete_attribute` validation accepts
    `edge.sources`** (2add4986). Update the adapted `test_provided_by_filter`.

### Connect
30. **`convert_to_trapi`**:
    - fix the `knowledge_graph = {}` dict bug;
    - guard against `edges` being None;
    - add `submitter: arax-pathfinder@{maturity}` to the rehydrate payload.

### Overlay
31. **`overlay_utilities._bind_overlay_edges`**: use `.get()` for node bindings
    (`:157-158`). A result with no binding for the qnode raises `KeyError` today.
32. **COHD virtual QEdge `predicates` must be a list**
    (`overlay_clinical_info.py:363`).
33. **NGD `defined_datetime`**: compute it once per run, not once per edge.
34. **FET**: drop the dead `kp='infores:rtx-kg2'` and its parameter, and update the
    log texts ("tier0 info-for-overlay sqlite"). This changes logs only.

### Infer / xDTD
35. **Drop NodeSynonymizer from Infer** (#2922, f7812633, a538145e). Use the curies
    as given, consistent with DEC-9. This removes one NodeNorm call per query.
36. **`infores:arax-xdtd` → `infores:arax`** (74768823) in:
    - `infer_utilities.py`
    - `result_transformer.py:~160` (the creative_DTD support-graph attribute)

    `upstream_suite/test_ARAX_expand.py:1483,1506` and the goldens need updating.
37. **Batch metadata fetch** (08fedb0e): `get_nodes_info_batch` and
    `get_edges_info_batch`. This is a performance fix (about 11k SQL queries down
    to 2) and arrives together with item 41.

### Synonymizer, meta-KG and API
38. **NodeNorm and Name Resolver by maturity** (#2913):
    - production NodeNorm becomes `nodenorm-es.transltr.io`;
    - add `NAME_RESOLVER_URL_BY_MATURITY` and `resolve_name_resolver_url()`
      (today the RENCI URL is hardcoded at `node_synonymizer.py:192`).
39. **Meta-KG**:
    - fetch Retriever's meta-KG with `params={"tier": 0}`;
    - rename the `original_predicate` constraint from "kg2 ids" to "original predicate";
    - drop empty lists inside nodes and edges (TRAPI 2.0 `minItems`);
    - set `FORCED_KP_VERSION` (`kp_selector.py:36`) to 2.0.0, so the UI's SmartAPI
      KP list shows 2.0 KPs.
40. **Model fixes**:
    - `AttributeConstraint` must require `id`, `operator` and `value`, and leave
      `name` optional. Today `name` is required (1.x), so a valid 2.0
      constraint crashes `from_dict`.
    - `KnowledgeGraph.edges` becomes optional.
41. **`/asyncquery_status`** (Shepherd's own route, `base_routes.py:1049`):
    - return at least one log entry (2.0 `minItems`);
    - set `response_url` to `{server_url}/arax/response/{id}`.
42. *(Optional, b8e2f499)* Remove the always-empty `in_kg2pre`, `name_kg2pre` and
    `category_kg2pre` fields from the synonymizer and `/entity` output.

## 2. Data and versions (DATA)

43. **Tier0 build `tier0-20260621` → `tier0-20260928`**
    (`shepherd_utils/config.py:264,282`). This is one package with items 44-46.
    - The ExplainableDTD filename has a **hyphen**:
      `ExplainableDTD_v1.0-{version}-all_with_paths.db` (`config.py:287`).
    - `fda_approved_drugs_v1.0.pickle` moves to the 20260928 directory.
    - COHD is unchanged.
    - Check that the `arax_pathfinder` worker reads the new `curie_ngd`
      (triples) before bumping the shared download.
44. **xDTD mapping-DB schema** (#2893, 8789ff4d):
    - `NodeInfo` is `(id, name, category, extra_attributes)`;
    - `EdgeInfo` is `(subject, predicate, object, id, category, extra_attributes)`.

    Path-edge attributes are built from `extra_attributes`. **The current code
    crashes on the new DB.** Also bump `created_datetime` to `2026-09-28`
    (ce5b5388). Update the `tests/unit/arax/query_parity/dbs.py` mapping tables.
45. **NGD** (#2889, d3c58cdb, b2446bf9):
    - precomputed lookup in `curie_ngd` first, falling back to on-the-fly
      computation;
    - read `curie_to_pmids` int32 binary blobs;
    - `pmid_count` uses `len(...) if not None`.

    Update the `test_compute_ngd.py` fixture. NGD values in the goldens change,
    and precomputed hits drop the 30-PMID cap.
46. **Config rename** `kg2c_sqlite`/`tier0_sqlite` → `info_for_overlay_sqlite`
    in `RTXConfiguration` and in `/status?mode=site_config`.
47. **Versions**:
    - ARAX `tool_version` 1.6.2 → **2.0.0**;
    - Biolink in `RTXConfiguration` 4.2.5 → **4.4.4**. This changes BiolinkHelper
      hierarchies, so re-check the goldens;
    - validation Biolink 4.4.2 → 4.4.4 (`response_lookup.py:55`).
48. **reasoner-validator 7.1.3** now supports TRAPI 2.0. The translator_tom stand-in
    in `response_lookup.py` could be replaced to restore API-07 parity, after a
    dependency check (linkml, bmt, click).
49. **catrax-pathfinder** 2.5.0 → 3.1.0 for the in-process Connect
    (`workers/arax/requirements.txt`). This brings the new constructor and
    `convert_to_trapi(…, maturity)` signature. The `arax_pathfinder` worker is
    already on 3.1.0.
50. **scipy** 1.15.3 → 1.16.3 upstream. Re-check the DEC-8 ranker parity pins.

## 3. Decisions needed (DECIDE)

Status (2026-11-23):
- **D1** done (DEC-25): `catrax-xcrg==1.0.1.dev2`. Its known bugs are BioPack-team/shepherd#273, #274, #275 and #276.
- **D2** done (DEC-23).
- **D3** done (DEC-22).
- **D4** decided: keep `parameters.timeout` as the whole-query timeout. It's a UI issue (DEC-29a).
- **Data (items 35-37 and 43-46)** done for the ARAX files (DEC-30). Item 35 is done only in
  `infer_utilities`; `ARAX_infer` still normalizes its curies. Item 46 (the config rename) is not
  done. The pathfinder files (`curie_ngd`, tier0-info-for-overlay) move in a separate PR.
- **D9** decided: read both sources (DEC-29b). **D10** decided: keep Shepherd's (DEC-29c). **D11** decided: keep the local offset; the heartbeat now has one (DEC-29d).
- **D5** done (DEC-24).
- **D6** done (DEC-26). A qnode `name` is still resolved (D-25).
- **D7** done (DEC-27).
- **D8** done (DEC-28).
- **Items 1-8** done (orchestration). Item 2 reads the qualifier slots from BiolinkHelper's cached
  Biolink 4.2.5 model YAML (the parity harness ships it as `expand_parity/biolink_model_4.2.5.yaml.gz`).
  Item 4 filters only the streamed log lines; the arax worker already filters the stored ones. Item 5
  is golden rule H18. Item 7 changes `wf_connect_knodes` (an expected difference, pinned by
  `test_connect_knodes_runs_upstream_trapi2_translation`). Item 8 reads `additional_properties` and
  the legacy `query_options` (merged, `additional_properties` wins), and the response still carries
  `query_options`. `bypass_cache` in Connect is item 22.
- **Items 9-16** done (Expand; item 17 was DEC-27). Item 9 keeps DEC-29b: the allow/deny lists merge
  `constraints.sources` and the knowledge-source attribute constraints, and only a DENY list removes
  KPs. Item 12 reads `AgentTypeEnum` from BiolinkHelper's cached Biolink model YAML, as item 2 does;
  the request-body stripping is golden rule H19. Items 14-16 fix D-23 and D-30: six query-parity cases
  that crashed upstream @ 9485431 on support-graph edges without `qedge_keys` now run
  (`wf_filter_kgraph_numeric` then stops at the ranker's D-20 `KeyError`), and Expand's log after an
  `exclude` qedge and the support graphs of pruned edges differ (expected differences, pinned by
  `test_filters_run_on_support_graph_edges`, `test_exclude_sweeps_orphan_nodes_and_support_graphs`
  and `test_only_reachable_support_graphs_are_kept`). New port-only Expand cases cover the 2.0-only
  constraints (`TRAPI2_PORT_ONLY_CASES`); unit tests are in `test_expand_trapi2_sync.py`.
- **Items 18-23, 30 and 49** done (KP cache and Connect). Items 18-20 are in the Redis cacher; the old
  `test_refresh_retries_a_cached_timeout_after_a_minute` is now
  `test_refresh_skips_a_cached_timeout_and_a_lookup_retries_it_after_a_minute`. Item 23's three upstream
  tests run on the fakeredis store in `tests/unit/arax/test_trapi_query_cacher.py`. Item 22 reads
  `parameters.bypass_cache` and the legacy `query_options`, as Expand does. Item 49:
  `catrax-pathfinder==3.1.0` in `workers/arax/requirements.txt` and `test-requirements.txt` (same
  numpy/pandas/xgboost pins as 2.5.0). Connect passes the maturity (`settings.server_maturity`) and
  keeps DEC-7's limits, Shepherd's Retriever and rehydrate URLs, and the tier0 info-for-overlay file
  it already used (`get_kg2c_db_path` is now `get_info_for_overlay_db_path`). Upstream's otel spans
  around `get_paths` are not taken. No goldens change (Connect is not a parity target, DEC-7).
- **Items 24-29** done (resultify, result transformer, ranker, filters). Item 24 changes no golden
  (the parity tests compare binding ids sorted); it changes the set order of ResultTransformer's aux
  graph `edges`, which `test_query_parity` now compares sorted too. Item 25 is DEC-31 and golden rule
  H21: the final KG drops every edge no result or support graph references, which removes Expand's
  `subclass_of` edges and unbound overlay and KP edges from 11 query-parity envelopes (the mock-data check
  now reads its overlay edges before the cleanup). Item 26 lets `optional_group_query` and
  `araxi_is_set_and_option_group` complete where upstream 9485431 raised `KeyError: 'n2'` (an
  expected difference, pinned by `test_optional_group_results_without_a_binding_complete`). Item 27
  is golden rule H20 and applies to both rankers (`workers/arax_rank/ranker.py` notes it departs
  from 9485431 there). Item 28 changes no golden; upstream's new
  `test_remove_general_concept_nodes_attribute_shapes` runs offline (87 offline upstream tests now).
  Item 29: upstream's `test_provided_by_filter` (live-only) and `test_prune` changes are mirrored.
  Unit tests are in `tests/unit/arax/test_results_trapi2_sync.py`.
- **Items 31, 32, 34 and 35** done (Overlay, Infer; item 33 came with DEC-30). Item 31 reads node
  bindings with `.get()` in `overlay_utilities` (`get_node_pairs_to_overlay` too, as upstream). Item 32's
  virtual QEdge is dropped from the final QG, so no golden changes. Item 34 keeps Shepherd's sqlite path
  setting (`kg2c_sqlite_path`, owned by the pathfinder-files PR) and calls `KPSelector(log=...)`; the
  stand-in still accepts `kg2_mode`. Item 35 follows DEC-9: Infer uses its curies as given, with no
  NodeNorm call; the dropped "Normalized ..." lines were DEBUG, which the goldens do not record, so no
  golden rule is needed. Tests: `test_overlay_binding.py`, `test_created_edges_trapi2.py` (FET logs, COHD
  predicates) and `test_infer_trapi2_sync.py`.

| # | Topic | Upstream `trapi2.0` | Port today |
|---|---|---|---|
| D1 | xCRG | PyPI `catrax-xcrg==1.0.1.dev2`, an official 2.0 release with a new API (`xcrg.Config`, `run_query`, `Reporter`, `is_valid_query`) | Vendored `e67f2c0`, converted by hand (DEC-21, whose "no 2.0 release" premise is now stale). Re-vendor or keep. |
| D2 | Single-node queries | Node-only QG gives `MissingQEdgeAndQPath` (501); `one_node_with_curie` template is an error; `expand(node_key=)` is `InvalidQuery` | Sent to Retriever. Affects `tpl_one_node`, `single_node` and `kp_forwarded_single_node` parity cases and 2 Shepherd tests |
| D3 | Creative-treats lumping | `_handle_creative_treats_predicate_answers` removed | Still runs |
| D4 | UI options and timeout | UI sends `parameters.timeout=30` (nothing upstream reads it); `query_plan` serialized at top level, but the UI reads `additional_properties.query_plan` | Shepherd treats `parameters.timeout` as the **whole-query** timeout, so UI queries are cut off at 30 s; `query_plan` goes under `query_options` |
| D5 | Set semantics | `is_set` removed; COLLATE everywhere; Retriever gets COLLATE only on id-less qnodes | `is_set` kept as a member and serialized in the returned QG; the Retriever body carries the 1.x `"is_set": true` (107 occurrences in the goldens) |
| D6 | QG field policy | `is_set`, `set_id`, `name`, `relation` rejected; pydantic silently drops the UI's 1.x `attribute_constraints`, `qualifier_constraints` and `intermediate_categories` | `name` resolved (D-25, keep); 1.x constraint fields answered with 400, so UI constraint queries fail |
| D7 | KL/AT of created edges | COHD `automated_agent`; `subclass_of` `knowledge_assertion`/`automated_agent` | COHD `data_analysis_pipeline`; `subclass_of` `not_provided`/`not_provided` |
| D8 | Jaccard predicate | `biolink:similar_to` (no `has_jaccard_index_with` in Biolink 4.4.4) | `biolink:has_jaccard_index_with` |
| D9 | Knowledge-source constraints | Read only from `constraints.sources` | Also from the `knowledge_source` attribute constraints |
| D10 | Multi-id qnode subclass parent | Warn and map no parent | Infer it from a `subclass_of` edge (DEC-20a; keep) |
| D11 | Log timestamps | `LogEntry`, UTC `…Z` | Local-offset ISO. Both are valid, but Shepherd's heartbeat (`arax.py:_heartbeat_line`) has no offset at all, which is a small bug |

**D1 note.** Moving to `catrax-xcrg==1.0.1.dev2` depends on two things:
- **Data.** The package reads only the new `curie_ngd` triples and the u32
  `curie_to_pmids` blobs, which ship with `tier0-20260928` (items 43-45). Our
  `tier0-20260621` files hold `[neighbor, ngd]` pairs and JSON PMID lists.
- **Fixes not in the package.** Our vendored copy fixes three problems that
  1.0.1.dev2 still has:
  - a KL/AT value with no ranking weight (2.0's `not_provided` agent type, or
    `data_analysis_pipeline`) raises `ValueError` in the ranker;
  - lookups send no `parameters.timeout` or `tiers`;
  - the whole query dict is parsed, so an ARAX envelope's empty containers can
    fail 2.0 validation.

  The direct-lookup filter bug is fixed in 1.0.1.dev2.

**D4 note.** The UI field is labelled "REMOTE API TIMEOUT DURING ARAX
EXPANSION", so it is meant as ARAX's **per-KP timeout** (`kp_timeout`), not a
whole-query timeout.
- Upstream commit 18ae5be1 moved it from `query_options.kp_timeout` to
  `parameters.timeout`, in both the UI and the tests.
- But upstream's server still reads `kp_timeout` only from
  `additional_properties`, so upstream silently drops the setting.
- Recommendation: on ARAX `/query`, map `parameters.timeout` to `kp_timeout`, and
  stop treating it as Shepherd's whole-query wait.

Keep Shepherd's version (no action):
- the `/response/{id}` real status codes and `POST /response` storage (upstream's FastAPI port broke both);
- the string `/status?id`;
- the 2.0-valid PathFinder binding cleanup in Connect;
- `bypass_cache` read from both places in Expand.

## 4. Not applicable (summary)

- pydantic and FastAPI plumbing (private-attribute renames, `model_construct`,
  routers, `request_context`, fork/rlimit);
- OTel spans (#2923);
- the background tasker and database manager (#2908);
- MySQL/S3 tracker changes and the async tracker status mapping (DEC-15);
- KG2, Plover and neo4j removal, and `kg2_mode` in KPSelector;
- deleted dead code: DTD_querier, predict_drug_treats_disease, GraphSage, old
  creativeCRG, and `ARAX_filter`/`ARAX_decorator`;
- the SmartAPI Retriever-CI hack;
- deploy configs;
- docs and comments;
- upstream test pruning (COHD, DTD and KG2 tests deleted).

## 5. Suggested order

1. Crash and validity fixes with no golden impact: items 1, 17, 26, 27, 31, 40, 41.
2. KP cache and Connect: items 18-23 and 30.
3. Expand semantics: items 9-16, which change the goldens.
4. The data bump as one package: items 43-46 (with 35-37), then re-derive the goldens.
5. API and UI: item 8, items 38-39, and D4.
6. The DECIDE items, starting with D1 (xCRG) and D2 (single-node).
