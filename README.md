[![codecov](https://codecov.io/gh/BioPack-team/shepherd/graph/badge.svg?token=NTPV9WF7EO)](https://codecov.io/gh/BioPack-team/shepherd)

# Translator Shepherd Service

Shepherd is a shared platform for ARA implementation. Incorporated ARAs have access to a plethora of shared ARA functionality while retaining the ability to implement their own custom operations.

## Local Development

All Shepherd services are set up as docker containers. You can learn about docker and install here: https://www.docker.com/

The main entrypoint is `./compose.yml` and will spin everything up.

- In the root folder, run `docker compose up --build`

If you want to add a new operation/worker, add a new service in `compose.yml` under `services`.

### Worker data (LMDB / sqlite) downloads

A couple of workers read from large, read-only sqlite databases and LMDB datasets that are too big to
commit to git (they're gitignored and volume-mounted from the host):

- **`aragorn_omnicorp`** → `./omnicorp_lmdb/` (`curies.lmdb`, `shared_counts.lmdb`)
- **`score_paths`** → `./pathfinder_embeddings/` (a directory-style LMDB)
- **`arax_pathfinder`** → `./arax_pathfinder_dbs/` (`curie_ngd_v1.0_<tier-version>.sqlite`, `tier0-info-for-overlay_v1.0_<tier-version>.sqlite`, `general_concepts.json`)

So a new developer doesn't have to source these by hand, each worker can fetch its dataset on first
startup. Two download mechanisms are supported, depending on where the dataset lives:

**LMDB datasets (`aragorn_omnicorp`, `score_paths`)** are fetched as a `.tar.gz` from a plain HTTP(S)
URL and extracted in place. Add the matching variable to your root `.env` file:

```dotenv
OMNICORP_LMDB_URL=https://example.org/path/omnicorp_lmdb.tar.gz
PATHFINDER_EMBEDDINGS_URL=https://example.org/path/pathfinder_embeddings.tar.gz
```

The archive for each dataset should contain the expected files at its top level: `curies.lmdb` and
`shared_counts.lmdb` for omnicorp, `data.mdb` (and `lock.mdb`) for the embeddings.

**arax_pathfinder's sqlite databases** are served as plain files over HTTPS, no credentials needed. The
filenames embed a Knowledge Graph version that changes periodically, so only one variable needs updating
when a new Knowledge Graph ships:

```dotenv
ARAX_PATHFINDER_TIER_VERSION=tier0-20260621
```

The ARAX blocked-concept list (`general_concepts.json`, fetched from GitHub) lands in this same
directory, so it is downloaded once and then persists with the databases rather than being re-fetched
by every new container. It is only fetched when absent — delete it from the volume to pick up an
updated upstream list.

On startup, each worker checks whether its files already exist in the volume-mounted directory. If
they're missing and a URL is configured, it fetches them into that directory — which lives on the
host, so the data persists across restarts and is only downloaded once. If the files are already
present, or no source is configured, the download is skipped (production mounts this data out of
band, so it's unaffected).

Downloads are bounded by `DATASET_DOWNLOAD_TIMEOUT_SEC` (default 60), which applies per socket
operation rather than to the whole transfer — a large file downloads for as long as it needs, but a
connection that opens and then stalls fails loudly instead of hanging worker startup.

#### Deploying these workers

The presence check is an exact match on the configured directory **and** the tier-versioned filenames.
A deployment that mounts the data somewhere else, or whose `ARAX_PATHFINDER_TIER_VERSION` doesn't match
the filenames on the volume, will not use the mounted copies — it will decide the dataset is missing and
download it again, into whatever path the settings do point at. If that path isn't the mount, the files
land on the container's writable layer and the pod is eventually evicted for exceeding its ephemeral
storage. So when deploying, set the directory explicitly and confirm it resolves to your mount:

```dotenv
ARAX_PATHFINDER_DBS_DIR=/data/arax_pathfinder_dbs   # default is relative: resolved against /app
```

```console
$ kubectl exec deploy/arax-pathfinder -- python -c \
    "from shepherd_utils.data_download import arax_pathfinder_sqlite_paths as p; print(*p(), sep='\n')"
```

Because `general_concepts.json` now shares that directory, the volume needs to be writable for the
first startup that fetches it — or the file can be preloaded alongside the sqlite databases, after
which the worker only ever reads it. A read-only mount with no preloaded copy fails at startup with a
permission error rather than silently continuing.

### Translator ARS

Shepherd also hosts a full port of the NCATS Translator ARS
([NCATSTranslator/Relay](https://github.com/NCATSTranslator/Relay)) at
`/ars/...` -- the same `/ars/api/submit` / `messages/<pk>?trace=y` /
`get_status` surface the Translator UI uses, backed by five workers
(`ars_fanout`, `ars_merge`, `ars_postprocess`, `ars_watchdog`, `ars_notify`)
on the shared Redis Streams fabric instead of Celery/RabbitMQ, and `ars_*`
Postgres tables instead of MySQL. Behavior is pinned against the upstream
codebase by a four-layer parity suite; see `docs/ARS_PARITY_REGISTER.md`
for the invariants, the golden-regeneration procedure, and every documented
deviation, and `tests/parity_e2e/README.md` for the side-by-side
differential harness. Deployment knobs live in `shepherd_utils/config.py`
under the "Translator ARS" block (`ARS_PUBLIC_HOST` must be reachable by
remote ARAs for their result callbacks).

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
- **Whole-query timeout budget (`QUERY_TIMEOUT_SEC`)** — see below.

##### Query timeout budget

The ARS and the other external callers stop waiting for a Shepherd query after
about five minutes, and the synchronous `/query` endpoint gives up around the
same point. Work done past that is work nobody receives — it only takes worker
slots (and process-pool children) away from queries that can still be answered.

So the server stamps each query with an absolute deadline at intake, and that
deadline travels with the task from operation to operation. Every worker checks
it as it picks a task up (in `shepherd_utils.shared.get_tasks`, on both freshly
delivered and reclaimed messages). If the budget is spent the worker does *not*
run the operation: it drops the rest of the workflow and routes the query
straight to `finish_query`, which ends it the way any other query ends — state
`COMPLETED` in Postgres with a `TIMEOUT` status, callback rows reaped, logs
saved (including the line explaining why the response is partial), and whatever
was gathered POSTed to the callback URL. A synchronous caller therefore gets a
partial response instead of waiting out its own timeout for nothing.

| Setting | Meaning |
| --- | --- |
| `QUERY_TIMEOUT_SEC` | The budget, in seconds (default 300). `0` disables deadlines entirely — tasks then run however old they are, as they did before. |

A client that explicitly asks to wait longer (TRAPI `parameters.timeout`) is not
cut short: the larger of the two wins. Two streams are exempt — `finish_query`,
which *is* the wrap-up, and `merge_message`, which folds in callbacks an
upstream service has already done the work for. Tasks with no deadline (a
payload enqueued before this shipped) are never expired, so a rollout is safe
mid-flight.

This is a fast path that settles a query at the moment it goes over. The
monitor's abandoned-query reaper (`MONITOR_ABANDONED_QUERY_SEC`, default 600s)
remains the backstop for queries that go over *without* any worker picking a
task up for them — e.g. one whose driving worker died with nothing left in a
stream.

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
`aragorn_score`, `aragorn_omnicorp`) size their process pools from the in-code
default, so raising `TASK_LIMIT` for those only deepens the intake queue rather
than adding parallelism.

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