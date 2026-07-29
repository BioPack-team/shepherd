[![codecov](https://codecov.io/gh/BioPack-team/shepherd/graph/badge.svg?token=NTPV9WF7EO)](https://codecov.io/gh/BioPack-team/shepherd)

# Translator Shepherd Service

Shepherd is a shared platform for ARA implementation. Incorporated ARAs have access to a plethora of shared ARA functionality while retaining the ability to implement their own custom operations.

## Local Development

All Shepherd services are set up as docker containers. You can learn about docker and install here: https://www.docker.com/

The main entrypoint is `./compose.yml` and will spin everything up.

- In the root folder, run `docker compose up --build`

If you want to add a new operation/worker, add a new service in `compose.yml` under `services`.

### Worker data (LMDB) downloads

A couple of workers read from large, read-only LMDB datasets that are too big to
commit to git (they're gitignored and volume-mounted from the host):

- **`aragorn_omnicorp`** → `./omnicorp_lmdb/` (`curies.lmdb`, `shared_counts.lmdb`)
- **`score_paths`** → `./pathfinder_embeddings/` (a directory-style LMDB)

So a new developer doesn't have to source these by hand, each worker can fetch
its dataset on first startup. Point it at a `.tar.gz` on an external server by
adding the matching variable to your root `.env` file:

```dotenv
OMNICORP_LMDB_URL=https://example.org/path/omnicorp_lmdb.tar.gz
PATHFINDER_EMBEDDINGS_URL=https://example.org/path/pathfinder_embeddings.tar.gz
```

On startup the worker checks whether its LMDB files already exist in the
volume-mounted directory. If they're missing and a URL is set, it downloads the
archive and extracts it into that directory — which lives on the host, so the
data persists across restarts and is only downloaded once. If the files are
already present, or no URL is configured, the download is skipped (production
mounts this data out of band, so it's unaffected).

The archive for each dataset should contain the expected files at its top level:
`curies.lmdb` and `shared_counts.lmdb` for omnicorp, `data.mdb` (and
`lock.mdb`) for the embeddings.

### Aragorn creative-mode query templates

A creative (inferred-edge) query is answered by fanning out a set of templated
TRAPI queries to Gandalf in parallel and letting the ranker filter what comes
back. Two template sets exist, and which one runs is the A/B knob:

- **`census`** (default) — the portfolios in
  `workers/aragorn_lookup/query_templates.py`, derived from a census of the
  graph's metagraph rather than from biomedical intuition. 28 shapes across four
  creative question types, tiered: mechanism (qualified), broad (recall),
  associative, and a quarantined tier that reads indication/contraindication
  edges.

  | question | pinned | answered | templates |
  | --- | --- | --- | ---: |
  | `treats` — what may treat this disease | disease | chemical | 12 |
  | `affects` — what moves this gene | gene | chemical | 6 |
  | `affects` — what genes does this move | chemical | gene | 5 |
  | `contraindicated` — what is unsafe here | disease | chemical | 5 |

  The `affects` portfolios carry a **sign**. Inference through a causal chain
  multiplies directions: if a chemical decreases regulator R and R *decreases*
  gene G, the chemical *increases* G. Hops therefore declare `SAME` or
  `OPPOSITE` relative to the direction the query asked for, and
  `Hop.resolve()` binds them per query — so one template serves both the
  "increased" and "decreased" form of a question. Analogy hops (same family,
  same pathway) use `SAME`: no flip.

  One deliberate relaxation: the incoming `affects` query constrains both
  aspect and direction, but constraining both on an *intermediate* hop cuts
  coverage from 33,052 proteins to 2,487. A creative template infers the
  qualified edge rather than travelling one, so intermediate hops constrain
  direction only — 13x recall at no cost in meaning.
- **`amie`** — the mined AMIE rules in
  `rules_with_types_cleaned_finalized.json`, which is how this worked before.

Set the default with `TEMPLATE_SET`, or override per query with
`parameters.template_set` (`census` | `amie` | `both`). `both` fires both sets
and emits the direct lookup query once.

**Only `treats` uses the census portfolios by default** (`TEMPLATE_QUERY_TYPES`).
The others are written and tested but do not fire, because the benchmark splits
by question type into two very different problems:

| half | n | TopAnswer, AMIE | TopAnswer, census | NeverShow never retrieved |
| --- | ---: | ---: | ---: | ---: |
| drug/disease | 47 | 29 | **33** | 113 / 252 (45%) |
| gene/chemical | 55 | **32** | 31 | 0 / 183 (0%) |

The gene/chemical half has **no retrieval gap at all** — every one of its 183
`NeverShow` entities is already retrieved, and only 2 of 55 `TopAnswer` entities
are missed. There is nothing for a template to find, so extra templates can only
add candidates that displace true answers, which is exactly the −1. The
drug/disease half is the opposite: 45% of its `NeverShow` entities are never
retrieved at all, and that is where the census portfolio earns its +4.

Counted across both halves, 86 `TopAnswer`/`Acceptable` assertions are retrieved
but ranked too low, against 14 never retrieved. **Ranking has roughly six times
the headroom of retrieval**, so further template work is the wrong lever.

Per-query knobs, all under `parameters`, for narrowing an experiment:

| Parameter               | Effect                                              |
| ----------------------- | --------------------------------------------------- |
| `template_set`          | which set fires                                      |
| `templates`             | list of template names to restrict to                |
| `template_tiers`        | list of tiers to restrict to (the ablation knob)     |
| `exclude_leaky`         | drop templates that read `treats`-family edges       |
| `template_path_budget`  | cap on total expected paths (0 = no cap)             |
| `probe`                 | enable/disable the per-disease probe                 |

Each template carries its own `filter_config` (degree caps), which is passed
through to Gandalf per query rather than globally; anything the caller sets
explicitly wins over the template's default. The template behind each expansion
is recorded on its OTEL span and in the lookup log line, so an A/B run can
attribute callbacks back to the template that asked for them.

#### Tiers, and what fires by default

Each template declares a tier, which is its claim about how much mechanism the
shape asserts. Selection is tier-first, so a path budget sheds recall templates
before mechanism ones:

| Tier            | Templates | Paths  | Claim                                    |
| --------------- | --------: | -----: | ---------------------------------------- |
| `A-mechanism`   |         5 |  1,136 | qualified: the drug moves a disease protein |
| `B-broad`       |         3 | 15,274 | recall: binding, pathway, PPI neighbourhood |
| `C-associative` |         2 |    493 | association, no mechanism                |
| `D-branching`   |         1 |  2,689 | two independent witnesses (precision lever) |
| `D-leaky`       |         1 |  8,221 | routes through `treats` — **held back**  |

**Only `A-mechanism` fires by default** (`TEMPLATE_TIERS`), on benchmark
evidence. Scored as TopAnswer PASSED / Acceptable PASSED / NeverShow FAILED:

| Arm                    | TopAnswer | Acceptable | NeverShow FAILED |
| ---------------------- | --------: | ---------: | ---------------: |
| AMIE rules (incumbent) |        61 |     **40** |               23 |
| every census tier      |        57 |         38 |               21 |
| `A-mechanism` only     |    **65** |         34 |            **5** |

Tier A is the only arm that beats the incumbent on top-answer precision, and it
cuts badly-surfaced never-show results by 78%. It costs six Acceptable passes —
those answers are still *retrieved* (NO_RESULTS 9, better than the incumbent's
12) but rank lower without the better-connected paths the broad tiers gave them.
Adding B/C/D back is what takes NeverShow FAILED from 5 to 21, and Tier B is
15,274 of the 18,456 paths that adds.

`D-leaky` (`indication_transfer`) is additionally held back by
`TEMPLATE_EXCLUDE_LEAKY`, so it stays off even if you widen the tier list. It
reads the same indication data most ground truth is drawn from, so it scores
well for reasons that will not generalize.

#### Configuration precedence (read this before editing a default)

`Settings` resolves **environment variables first, then `.env`, then the
defaults in `shepherd_utils/config.py`** — and `compose.yml` mounts the repo's
`.env` into the worker at `/app/.env`. So a `TEMPLATE_TIERS` left over from an
earlier ablation run silently wins over an edited default, and the only visible
symptom is that the templates you expected never get sent.

Every worker start logs what is actually in effect, and says when something
overrode the code:

```
INFO Aragorn creative expansion: template_set='census',
     template_tiers='A-mechanism' [env, overriding 'A-mechanism,D-branching'],
     template_exclude_leaky=True, template_path_budget=30000,
     template_probe_enabled=True, census_dir='/app/census'
```

Check that line first whenever a config change appears to do nothing.

#### Running an ablation

Tier is the unit to ablate on, and `parameters.template_tiers` sets it per
query — no redeploy, so the arms can run against one deployment:

```jsonc
{"template_set": "amie"}                                          // incumbent
{"template_set": "census", "template_tiers": ["A-mechanism"]}     // current default
{"template_set": "census", "template_tiers": ["A-mechanism", "C-associative"]}
{"template_set": "census", "template_tiers": ["A-mechanism", "D-branching"]}
{"template_set": "census", "template_tiers": ["A-mechanism", "B-broad"]}
{"template_set": "census", "exclude_leaky": false,
 "template_tiers": ["A-mechanism", "B-broad", "C-associative",
                    "D-branching", "D-leaky"]}                    // everything
```

Read the arms on two axes, because the portfolio moves them in opposite
directions. **Recall** is `NO_RESULTS` — how often the asserted entity was not
retrieved at all. **Top-of-ranking precision** is `TopAnswer`/`Acceptable`
`PASSED`. A broad tier reliably buys the first and can cost the second.

Two traps worth naming.

On `NeverShow`, `PASSED` and `NO_RESULTS` are both successes — the entity was
not badly surfaced either way. A portfolio that retrieves more converts
`NO_RESULTS` into `PASSED` and looks like a large `NeverShow` win while nothing
improved. Compare `PASSED + NO_RESULTS`, or just watch `FAILED`.

And adding a template does not corroborate an answer. `aragorn.score` scores
each *analysis* independently (effective resistance over that analysis's
subgraph) and ranks an answer by the **maximum** over its analyses — see
`workers/aragorn_score/worker.py`. So a template lifts an answer only by giving
it a single better-connected path, never by adding a second mediocre one. The
practical consequence when choosing what to add to Tier A: a template that
introduces *new* answers can displace true ones, while a template whose answers
are a subset of what already fires can only raise scores of answers already
present. `two_witness_inhibition` is the clean case of the latter — it requires
a chemical to decrease *two* disease-associated proteins where
`target_inhibition_sm` requires one, so its answers are a strict subset on any
graph, and it offers a denser per-analysis subgraph.

#### The census

Templates are priced against the metagraph census produced by gandalf's
`scripts/metagraph_census.py` when the graph is built. Ship it as a build
artifact alongside the mmap graph, version it with the graph, and point
`CENSUS_DIR` at it (default `/app/census`; see the commented volume in
`compose.yml`). Only the rows the portfolio prices are read, so this costs about
25MB resident and a second at worker start rather than the ~870MB a full load
would take.

The census is optional. Without it the worker falls back to the baseline
estimates compiled into `query_templates.py` — the numbers that census produced
when the portfolio was built — logs a warning, and keeps serving.

**Confirming it loaded.** The baselines were derived from the census, so for an
average disease both price the portfolio identically; they only diverge in the
tail, which is exactly where the budget matters. The numbers alone will
therefore not tell you whether the mount worked, so two log lines do.

On worker start, with the census mounted:

```
INFO Loaded census from /app/census: 13 rollup rows, graph <path> (1,670,341 nodes /
     28,709,074 edges), biolink 4.3.2, query semantics, generated 2026-07-25T02:36:55Z
```

Check the graph, node/edge counts and Biolink version against the graph Gandalf
is actually serving — this is the provenance that tells you the census and the
graph are the same vintage. Without the mount you get a warning instead:

```
WARNING No census at /app/census (census_rollup.tsv missing); pricing query
        templates from baked-in baselines instead.
```

Then every creative query says which one it used, so you can confirm it long
after startup has scrolled away:

```
INFO Census portfolio for MONDO:0004979: 11 templates, ~19592 expected paths,
     priced from census, probed (causal_gene_inhibition:134, ...)
```

`priced from baselines (no census mounted)` there means the mount is missing or
unreadable. `probed`/`unprobed` reports whether the per-disease probe answered.

#### The per-disease probe

Census fan-outs are means over a heavy-tailed distribution, and disease degree
varies by orders of magnitude, so a portfolio chosen from means holds the time
budget on the average disease and blows it in the tail. Before firing the real
expansions the worker measures the pinned disease's actual degree on each entry
hop the portfolio uses (one small dehydrated, degree-capped query per distinct
hop, run concurrently), and prices the templates against that instead.

It is bounded by `TEMPLATE_PROBE_TIMEOUT` (default 2s) and fails soft: a probe
that times out or errors leaves the estimates on census means and the query
proceeds. Disable it with `TEMPLATE_PROBE_ENABLED=false`.

### Worker

Each worker is it's own separate docker container. It spins up and begins to watch a central message broker for tasks to work on. Once it gets a task, it
can do that task either synchronously, asynchronously, on a separate process etc. based on its individual resource requirements.

Each worker has access to a shared utilities library that aids in db and message broker interaction as well as other functions that are common across ARAs. Check the
shared function library before writing a new function that you think other ARAs might also want to use.

#### Worker tuning & graceful shutdown

Every worker draws its tasks through `shepherd_utils.shared.get_tasks`, so the
following behavior applies to all of them:

- **Concurrency (`TASK_LIMIT`)** — each worker declares a default in-process
  concurrency, but it can be overridden per deployment with the `TASK_LIMIT`
  environment variable (each worker is its own container, so a single
  `TASK_LIMIT` per Deployment is unambiguous). No code change or rebuild needed.
- **Graceful drain on shutdown** — on `SIGTERM`/`SIGINT` (Kubernetes sends
  `SIGTERM` on every rollout, scale-down and node drain) a worker stops pulling
  new tasks, waits up to `WORKER_DRAIN_TIMEOUT_SEC` (default 30s) for in-flight
  tasks to finish, writes a clean-shutdown marker the monitor reads (so the
  event is classified as a graceful scale-down rather than a crash), then exits.
  Tasks that don't finish in the window are left in the stream for Redis reclaim.
  Set the deployment's `terminationGracePeriodSeconds` comfortably above
  `WORKER_DRAIN_TIMEOUT_SEC`.

##### Kubernetes sizing (Helm)

Production limits live in the Helm chart, not in `compose.yml` (which is
dev-only). Recommended starting point for `finish_query`, which holds whole
decompressed TRAPI payloads in memory while POSTing async callbacks:

| Setting | Value |
| --- | --- |
| `resources.requests` | `cpu: 500m`, `memory: 1Gi` |
| `resources.limits` | `cpu: "2"`, `memory: 4Gi` |
| `TASK_LIMIT` | `32` (down from the in-code default of 100) |
| `terminationGracePeriodSeconds` | `35` |

Scale throughput with replicas / an HPA (on CPU or queue depth) rather than a
single large pod. On Kubernetes the memory `limit` (OOMKilled + restart) plus
regular rollouts already recycle pods, so leaked-resource cleanup comes for free
— add an RSS-based `livenessProbe` only if the monitor shows OOMKills in
practice. CPU-bound pool workers (`merge_message`, `score_paths`, `arax_rank`,
`aragorn_score`, `aragorn_omnicorp`) size their process/thread pools from the
in-code default, so raising `TASK_LIMIT` for those only deepens the intake queue
rather than adding parallelism.

### Message Broker Streams

Shepherd uses Redis Streams for its message broker. More info on Redis Streams can be found [here](https://redis.io/docs/latest/develop/data-types/streams/)
Each worker type listens to its own message stream/queue. The shared workers can just use their Translator workflow name for their stream name. For workers that share the same workflow but are for different ARAs need their own custom stream name. The current convention is to use the format `{ara_name}.{workflow_name}`.
Multiple workers of the same type can be in the same `GROUP`, and redis will make sure not to give out the same task to more than one worker in that group. The current convention is to call the group `consumer`.

### Creating your own ARA

Creating a brand new ARA is fairly straightforward. Here are the steps to create a basic ARA that performs all the necessary Translator operations:
- Copy the `workers/example_ara` folder. This will be the main entrypoint to your new ARA.
  - Towards the top of that file, replace the `STREAM` variable value with your ARA name.
  - Within the `example_ara` function in that file, replace the `workflow` list with your ARA's workflow. This could include analyzing the `message` to determine a pertinent workflow.
  - **Note:** If using operations not in the shared workers, your workflow operation ids need to reflect the `STREAM` name of your custom operation workers. This is how the task get passed to your operation worker.
- (If needed) Copy the `workers/example_lookup` folder. This will be your ARA's lookup operation.
  - Towards the top of that file, replace the `STREAM` variable value with `{ara_name}.lookup`.
  - Within the `example_lookup` function in that file, replace the contents with your ARA's lookup logic.
- (If needed) Copy the `workers/example_score` folder. This will be your ARA's score operation.
  - Towards the top of that file, replace the `STREAM` variable value with `{ara_name}.score`.
  - Within the `example_score` funciton in that file, replace the contents with your ARA's scoring logic.
- If you have other custom operations you want to perform that are in your `workflow` above, pick a similar folder to copy and adjust the code inside to fit your needs.
- If you want to use shared workers (i.e. `workers/sort_results_score`, `workers/filter_results_top_n`), you don't need to do anything other than include them in your workflow. They will automatically pick up your query and pass it along like your other operations.
- Open the `compose.yml` file in the root directory, and for each ARA folder you created, add a `service` (or copy an existing one), and make sure that the `container_name` and `build/dockerfile` reflect you worker names and the path to your worker Dockerfile
- Run Shepherd with `docker compose up --build`

### Testing your ARA

Shepherd uses pytest and tox for local test running and GitHub Actions. To run tests, simply activate your virtual env, run:
- `pip install tox`
and then run:
- `tox`

This will run all the tests and then also provide code coverage.


If you would like to run local integration tests, run the `scripts/test_shepherd.py` script to run a query against your ARA. Replace the `target` argument with your ARA name so the server routes the query to your worker. This script requires that Shepherd be running locally.