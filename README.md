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

- **`census`** (default) — the portfolio in
  `workers/aragorn_lookup/query_templates.py`, derived from a census of the
  graph's metagraph rather than from biomedical intuition. Twelve shapes in four
  tiers: mechanism (qualified), broad (recall), associative, and a quarantined
  tier that reads `treats`-family edges.
- **`amie`** — the mined AMIE rules in
  `rules_with_types_cleaned_finalized.json`, which is how this worked before.

Set the default with `TEMPLATE_SET`, or override per query with
`parameters.template_set` (`census` | `amie` | `both`). `both` fires both sets
and emits the direct lookup query once. Only drug-for-disease creative edges
have census templates; every other creative edge uses the AMIE rules whatever
this is set to.

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

`D-leaky` (`indication_transfer`) does not fire by default. It reads the same
indication data most ground truth is drawn from, so it scores well against any
benchmark built from that data for reasons that will not generalize, and the
census work's evaluation protocol says to score it in its own bucket. Flip it on
with `TEMPLATE_EXCLUDE_LEAKY=false` or `parameters.exclude_leaky: false` when
you want to measure what it is actually worth.

#### Running an ablation

Tier is the unit to ablate on, and `parameters.template_tiers` sets it per
query — no redeploy, so the arms can run against one deployment:

```jsonc
{"template_set": "amie"}                                    // baseline
{"template_set": "census", "template_tiers": ["A-mechanism"]}
{"template_set": "census", "template_tiers": ["A-mechanism", "C-associative"]}
{"template_set": "census"}                                  // current default
{"template_set": "census", "exclude_leaky": false}          // + the leaky tier
```

Read the arms on two axes, because the portfolio moves them in opposite
directions. **Recall** is `NO_RESULTS` — how often the asserted entity was not
retrieved at all. **Top-of-ranking precision** is `TopAnswer`/`Acceptable`
`PASSED`. A broad tier reliably buys the first and can cost the second.

One trap worth naming: on `NeverShow`, `PASSED` and `NO_RESULTS` are both
successes — the entity was not badly surfaced either way. A portfolio that
retrieves more will convert `NO_RESULTS` into `PASSED` and look like a large
`NeverShow` win while nothing actually improved. Compare `PASSED + NO_RESULTS`,
or just watch `FAILED`.

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