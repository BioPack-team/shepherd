# Shepherd Scalability Analysis

**Target load:** hundreds of thousands to millions of daily active users.
**Scope:** end-to-end — ingress, FastAPI server, Redis Streams broker, Redis KV data store, Postgres, and every worker type.
**Stance:** the current defaults (`TASK_LIMIT=100`, `max_connections=10`, `maxmemory 4gb`, single-replica of everything) reflect a small-user-base prototype. This report treats them as inputs, not constraints, and recalculates what each layer actually needs.

---

## 1. Executive summary

Shepherd's logical architecture is sound for a queue-and-worker pipeline, but **nothing in the current deployment is built to scale horizontally** and several design choices will break catastrophically before the single-node hardware limits are even reached. The critical problems, in order of severity:

1. **The TRAPI message blob is rewritten end-to-end on every workflow step.** Every operation does `get_message` → decompress → mutate → compress → `save_message` against a single Redis instance. Message sizes are typically several MB compressed (tens of MB raw). At peak load this saturates Redis network + CPU long before queue throughput matters. This is the #1 scalability ceiling.
2. **Redis is configured with `maxmemory 4gb` + `allkeys-lru`.** In-flight TRAPI responses can be evicted mid-workflow, which silently corrupts queries. This is a correctness bug at scale, not just a performance issue.
3. **The sync `/query` endpoint holds a TCP connection open for up to 360s, polling Postgres every 500ms.** Each sync query consumes one FastAPI slot + hundreds of DB round-trips. 100k concurrent sync queries is not achievable on this design.
4. **The server container runs one uvicorn process** (no `--workers`, no gunicorn, no LB-friendly deployment). It is a single point of failure and a single point of throughput.
5. **Lookup workers block their own concurrency slots polling Postgres for callback completion** (sleep-1s loops, up to 210s). `TASK_LIMIT=100` is effectively 100 queries per replica, not 100 tasks/sec.
6. **Merge lock TTL (45s) is shorter than worst-case merge time.** Under heavy load two workers can hold "the lock" on the same response and corrupt the merge.
7. **Streams are never trimmed (`XADD` with no `MAXLEN`)**, Postgres has no indexes on `callbacks.callback_id` / `callbacks.query_id` and no retention, both grow forever.
8. **Single Redis, single Postgres, no cluster, no HA, no read replicas.** The blast radius of any broker or DB blip is 100% of traffic.

### Load math for the target

Assume 1,000,000 DAU, ~2 queries each on average, **peak:average ≈ 5–10×**, peak concentrated over ~4 daytime hours:

- Average throughput: `2M / 86400 ≈ 23 qps`.
- Peak throughput (conservative 5×): **≈ 115 qps sustained peak**.
- Aggressive peak (10× burst): **≈ 230 qps**.

A single creative-mode ARAGORN query fans out into roughly **6–12 worker operations** (ARA entrypoint → lookup → N expanded sub-lookups → per-callback merges → score → sort → filter × 2 → finish). Call it **~10 task hops per query**. So the broker + data store see:

- **~1,150 tasks/sec** (peak) on Streams — ~10× the query rate.
- **~2,300 blob read/write pairs per second** against the data Redis — each touching multi-MB payloads.
- **~5k–15k Postgres round-trips/sec** (mostly sync-query polling + callback bookkeeping) if the current polling designs are kept.

Those numbers shape the rest of this report.

---

## 2. Current architecture at a glance

```
             ┌────────────┐
   client ──►│ FastAPI    │── uvicorn (single process, --reload in main.py)
             │ shepherd_  │── 1 replica, port 5439
             │ server     │
             └─────┬──────┘
                   │
     ┌─────────────┼─────────────────────────────────────────┐
     ▼             ▼                                         ▼
 ┌────────┐   ┌──────────────────────────────────┐     ┌──────────┐
 │ Postgres│   │ Redis (single instance)          │     │ External │
 │ (1 node)│   │  db0 = streams (broker)          │     │ services │
 │ max_conn│   │  db1 = data (compressed TRAPI)   │     │ strider, │
 │  = 200  │   │  db2 = locks (SET NX)            │     │ arax,    │
 └─────────┘   │  db3 = logs                      │     │ bte, ... │
               │  maxmemory 4gb, allkeys-lru      │     └──────────┘
               └──────────────────────────────────┘
                              ▲
                              │ xreadgroup / xadd / get / set
                              │
    ┌──────────┬──────────┬──┴──────┬──────────┬───────────┐
    ▼          ▼          ▼         ▼          ▼           ▼
 aragorn    aragorn.   merge_   sort_ ...   finish_   score_paths
 (entry)    lookup     message  results     query    (GPU, TASK=1)
 TASK=100   TASK=100   TASK=10              TASK=100
```

Every worker is `asyncio.run(poll_for_tasks)` in one container, holding:
- **Broker Redis pool:** `max_connections=10`
- **Lock Redis pool:** `max_connections=10`
- **Data Redis pool:** `max_connections=10`
- **Logs Redis pool:** `max_connections=10`
- **Postgres pool:** `min_size=5, max_size=10`

So every worker replica reserves **~40 Redis connections and up to 10 Postgres connections**, regardless of how much work it actually does.

---

## 3. Ingress / FastAPI server

`shepherd_server/Dockerfile:35`:
```
CMD ["python", "-m", "uvicorn", "--host", "0.0.0.0", "--port", "5439", "shepherd_server.server:APP"]
```
`shepherd_server/main.py`:
```python
uvicorn.run("shepherd_server.server:APP", host="0.0.0.0", port=5439, reload=True, ...)
```

### Problems

- **Single process, no workers flag.** Uvicorn defaults to one event loop. With Python's GIL, one CPU core serves 100% of traffic. 115 qps of pure async routing is fine on modern hardware, but sync queries (below) change that dramatically.
- **`reload=True` in `main.py`** is development-only. The actual container CMD doesn't use it, but `main.py` shouldn't be the production entrypoint at all — it only exists to make `python -m shepherd_server.main` work and is misleading.
- **`docs_url=None`** is OK, but `FastAPIInstrumentor.instrument_app(APP)` is applied to every request, adding OTLP span emission to the hot path.
- **One container, no replicas, no LB.** `compose.yml` maps host port `5439` directly to one container. There is no load balancer, health probe, or rolling-update story.

### Sync query (`base_routes.py:136–172`) is pathological under load

```python
timeout = query_dict.get("parameters", {}).get("timeout", 360)
while now <= start + timeout:
    now = time.time()
    query_state = await get_query_state(query_id, logger)  # SELECT on Postgres
    ...
    await asyncio.sleep(0.5)
```

- Every sync query holds an HTTP connection + FastAPI task for **up to 360s**.
- It polls Postgres **~720 times per query**. `get_query_state` is a `SELECT * FROM shepherd_brain WHERE qid = %s` inside `pool.connection(60)`, so it round-trips on the shared pool (`max_size=10`).
- If the query finishes in 20s, that's still 40 Postgres reads wasted per query.

At the target load, even if only 20% of queries are sync:
- 23 qps sync × 720 polls × 0.2 ≈ **3,300 Postgres reads/sec just for polling**, plus the real work.
- A 360s tail of slow queries keeps a growing population of long-lived sync connections open on the FastAPI container.

### Required changes

1. **Run the API behind a proper ASGI supervisor.** Use `gunicorn -k uvicorn.workers.UvicornWorker -w <n>` or run N uvicorn replicas behind an L7 load balancer. Target ~1 worker per physical core per pod; scale pod count to N ≈ `peak_qps * avg_holding_time / target_concurrency_per_worker`.
2. **Replace sync polling with an event-driven wait.** Options:
   - Postgres `LISTEN/NOTIFY` on `shepherd_brain.state` — worker emits a notify on `set_query_completed`, the server's sync handler waits on a channel.
   - Redis pub/sub on `completed:{query_id}` emitted by `finish_query`.
   - A Redis `BRPOP`/`XREAD` on a per-query completion queue.
   - All three drop sync latency ceiling to real query time and DB polling load to zero.
3. **Strongly prefer the async endpoint for bulk clients.** Document the sync endpoint as "for interactive debugging only" and consider removing it at scale — every sync call is a liveness risk.
4. **Cap sync timeouts server-side.** 360s per request is a DoS vector; clients who want that should use async with callback.
5. **Split traffic.** `/query` vs `/asyncquery` vs `/callback` have radically different profiles. Route callbacks (inbound from strider/arax/etc.) through their own pod pool so external spikes can't starve user-facing traffic.

### How many server pods

With the event-driven sync wait in place, a single FastAPI worker process handles thousands of concurrent idle `await` points comfortably. Budget each pod at **~2000 concurrent queries** held in-flight (limited mostly by FD count, ephemeral ports to Postgres/Redis, and event-loop overhead). For **230 qps peak × 60s P95 latency = ~14,000 in-flight**, you need **~8 pods** (with 2–4 uvicorn workers each) plus headroom for a rolling deploy, so plan **10–16 pods** behind an L7 load balancer with per-pod CPU/mem requests tuned to one core each.

---

## 4. Task broker — Redis Streams (db 0)

`shepherd_utils/broker.py:11–25`:
```python
broker_redis_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host, port=settings.redis_port, db=0,
    max_connections=10, timeout=30, socket_timeout=7,
    socket_connect_timeout=10, ..., decode_responses=True, ...
)
```

Tasks are produced with `XADD` and consumed with `XREADGROUP ... COUNT=1 BLOCK=5000`. Consumer name is `str(uuid.uuid4())[:8]` per process, group name `"consumer"`.

### What's on the wire

The stream payload is small (just `query_id`, `response_id`, `workflow` JSON, `log_level`, `otel` context). That's good — **payloads are IDs, not data**. So the broker's bandwidth cost is small per task.

Throughput estimate for the target load:
- ~1,150 tasks/sec (peak, after 10× fan-out).
- Each task = 1 `XADD` + 1 `XREADGROUP` + 1 `XACK` = 3 small round-trips + `XGROUP CREATE` that is attempted on every `get_task` call.

A single Redis node handles 80–150k small ops/sec on one core. **3,500 broker ops/sec is fine** on Redis capacity. The broker is **not** the primary bottleneck — but it has several correctness problems that will bite at scale.

### Problems

1. **Streams are unbounded.** `broker.add_task` calls `xadd(queue, payload)` with no `maxlen=...`. ACK'd entries are not deleted; they remain in the stream for `XRANGE` history. Over months at 1k/s that's billions of entries. **Fix:** add `maxlen=100_000, approximate=True` (or similar) to every `XADD`, and/or run an `XTRIM MINID` on a cron.
2. **`XGROUP CREATE` is called on every `get_task`** (`broker.py:76`). It throws `ResponseError` when the group exists, which is caught. Cheap per call, but it's pointless noise in logs and metrics. Move to startup.
3. **No pending-entry recovery.** Redis Streams track pending (delivered-but-unacked) entries per consumer. When a worker crashes (OOM, rolling deploy, `KILL`), its pending entries stay pending forever, because:
   - `CONSUMER = uuid.uuid4()[:8]` — a new pod is a new consumer, never reclaiming the old one's PEL.
   - There is no `XAUTOCLAIM` / `XPENDING` sweeper anywhere in the codebase.
   - Under rolling deploys, this leaks a task per in-flight task per pod. At scale, **queries silently stop progressing** because a middle step is orphaned.
   - **Fix:** either use a stable consumer name (e.g., pod name) *and* run a periodic `XAUTOCLAIM` sweep that reassigns idle entries older than N seconds, or drop Streams for a proper broker that handles reassignment natively (RabbitMQ/NATS JetStream/Kafka all do).
4. **No dead-letter queue.** `handle_task_failure` in `shared.py:109` ACKs the failing task and enqueues a `finish_query` with `status=ERROR`. There is no place to re-drive a failed task after a bug fix and no visibility into poisoned messages.
5. **`decode_responses=True` on the broker client.** Fine for the stream (string payloads), but it means the *same* Redis connection pool cannot be shared with the data pool, which stores binary zstd. That's why `db.py` opens a separate `data_db_pool`. OK — just flagging that the split is intentional.
6. **Single Redis instance.** A Streams workload is tricky to cluster because consumer groups are per-key and XREADGROUP over multiple keys in different hash slots is not allowed. Mitigations:
   - Run Redis Streams on a dedicated, well-provisioned non-clustered primary with a replica (Sentinel or Redis-HA), separate from the data store.
   - Or migrate to a broker designed for horizontal scale: **NATS JetStream** (best fit — durable streams, consumer groups, acks, XAUTOCLAIM-equivalent built in), **RabbitMQ** quorum queues, or **Kafka**.

### Consumer-group design

The current "all replicas of a given worker share group name `consumer`" is correct for load-balancing within a worker type — Streams will deliver each message to exactly one consumer in the group. Keep this. When horizontal scaling starts, the hot question becomes "how many replicas of each worker?" — covered in section 9.

---

## 5. Data store — Redis KV for TRAPI blobs (db 1) — **THE primary bottleneck**

This is the layer most likely to melt first. Everything non-trivial about Shepherd flows through it.

### Current behavior

Every workflow step does:
1. `get_message(response_id)` — Redis `GET` of a multi-MB zstd blob → decompress → parse JSON.
2. Mutate the in-memory dict.
3. `save_message(response_id, ...)` — serialize → zstd compress → Redis `SET ... EX=1_210_000`.

`shepherd_utils/db.py:117–124`:
```python
def encode_message(obj): return zstandard.compress(orjson.dumps(obj))
def decode_message(blob): return orjson.loads(zstandard.decompress(blob))
```

`shepherd_broker/redis.conf`:
```
maxmemory 4gb
maxmemory-policy allkeys-lru
proto-max-bulk-len 1000mb
save 60 50
timeout 120
```

### Why this doesn't scale

**Message size.** A real creative-mode TRAPI response has 10k–100k knowledge graph edges plus results, aux graphs, logs, etc. Observed sizes vary but are commonly:
- Raw JSON: 30–300 MB.
- zstd-compressed: 3–30 MB.

**Per-step cost at peak (~2,300 read/write pairs/sec):**
- Redis **network**: `2,300 × (avg ~10 MB) × 2 (read+write) ≈ 46 GB/s`. A single Redis node on 25 Gbps NIC handles ~3 GB/s best case. **Orders of magnitude over line rate.**
- Redis **CPU**: single-threaded GET/SET on ~10 MB values → dominated by `memcpy` and protocol framing. Hundreds of µs each. At ~5k large-value ops/sec you saturate the Redis thread.
- **Client CPU**: zstd decompress + orjson parse + (re-)compress + orjson serialize of multi-MB JSON → **~0.5–5 seconds of CPU per step** on a single core. Multiply by 10 steps per query. That CPU is spent inside *every* worker, not on a shared service.

This cost is also why `merge_message` uses a `ProcessPoolExecutor` — merge is CPU-bound and the GIL would otherwise block the event loop. But `sort_results_score`, `filter_results_top_n`, `filter_kgraph_orphans`, and `filter_analyses_top_n` all do the same "load → mutate big dict → save" cycle **synchronously in the asyncio event loop**. With `TASK_LIMIT=100`, 100 of those on one replica will pin the event loop for seconds at a time.

**`maxmemory 4gb` + `allkeys-lru` is a correctness bug at scale.** With `redis_ttl = 1_210_000` (~14 days) and ~30 MB per response:
- 4 GB / 30 MB = ~130 in-flight responses before eviction begins.
- At 230 qps × avg 60s latency, there are ~14,000 in-flight responses simultaneously.
- LRU will evict in-flight response blobs mid-workflow. A worker reads `response_id`, returns `None`, and the query silently drops or errors.

The current config is only safe because the prototype sees a handful of concurrent queries.

### Required changes

1. **Stop passing the whole message through Redis on every step.** This is the single highest-leverage fix. Options, in increasing order of refactor cost:
   - **(a)** Keep the blob in Redis, but redesign workers so that filtering/sorting/merging operates on a stable `response_id` and touches only the sub-structure they need (e.g., store `results`, `knowledge_graph`, `auxiliary_graphs` as separate keys; a filter step rewrites only `results`). This cuts I/O 3–10× without changing storage.
   - **(b)** Move the blob to **S3 / MinIO / GCS** and put only metadata + object keys in Redis/Postgres. Object storage is designed for multi-MB payloads and scales horizontally for free. Workers fetch/put via presigned URLs; redis holds a CAS token for concurrent modification.
   - **(c)** Rework the pipeline from "shared mutable blob" to **append-only per-step deltas** that `finish_query` collapses. This matches the fan-in pattern (multiple callbacks → merge) far better than the current lock-around-the-blob approach.
2. **Move data Redis off the broker Redis.** They have radically different workloads. Run at least two Redis fleets: one for streams + locks (small, latency-sensitive), one for data (throughput + size).
3. **Never run with `allkeys-lru` on live data.** Switch to `noeviction` (and monitor + alert on `used_memory >= maxmemory`), or `volatile-lru` with extremely long TTLs reserved for truly cacheable items only. In-flight TRAPI blobs must not be evictable.
4. **Right-size memory.** With 14-day TTL retaining finished queries for replay/debug, 1M DAU × ~30 MB avg = ~30 TB of retained data. That is not a Redis workload. Either:
   - Shorten `redis_ttl` dramatically (hours, not weeks) for the in-Redis copy and archive to object storage for long retention.
   - Or decide "Redis is a scratch cache, S3 is the source of truth" (recommended).
5. **Compression.** zstd is already a great choice. Consider raising level for cold storage or switching to zstd level 1–3 for hot paths if CPU becomes the bottleneck — benchmark before changing.
6. **`proto-max-bulk-len 1000mb`** is a symptom of the "one giant blob" design. After (1), values should stay under a few MB and this can drop back to the default.

### Postgres is the wrong long-term fit for state too

Right now Postgres stores only state rows (`shepherd_brain`, `callbacks`). See section 6. But note: moving "current workflow position" out of the lock-on-one-blob pattern will also take load off Postgres because the sync-query poll goes away.

---

## 6. State store — Postgres

`compose.yml`: `postgres -c max_connections=200`, single instance, no replica.

`shepherd_db/init_db.sql`:
```sql
CREATE TABLE shepherd_brain (
  qid varchar(255) PRIMARY KEY,
  start_time TIMESTAMP, stop_time TIMESTAMP,
  submitter TEXT, remote_ip TEXT, domain TEXT, hostname TEXT,
  response_id TEXT, callback_url TEXT,
  state TEXT, status TEXT, description TEXT
);
CREATE TABLE callbacks (
  query_id varchar(255) REFERENCES shepherd_brain(qid),
  callback_id varchar(255),
  otel_trace varchar(255)
);
```

### Problems

1. **`max_connections=200` is eaten alive by worker pools.** Each worker replica reserves up to 10 PG connections (`AsyncConnectionPool(min_size=5, max_size=10)`). The current compose has ~20 workers + 1 server = 210 potential clients. At the scaled numbers in section 9 (dozens of replicas per worker type), connection demand is in the thousands. Postgres does not handle thousands of idle connections well.
   - **Fix:** put **PgBouncer in transaction-pooling mode** in front of Postgres. Reduce `max_size` in each pool to 2–4 (enough for bursts) since bouncer multiplexes; then the backend Postgres needs only ~100–200 actual connections regardless of client count.
2. **No index on `callbacks.callback_id` or `callbacks.query_id`.** Every `remove_callback_id` and `get_callback_query_id` is a full scan. At 1k/s callbacks × millions of accumulated rows, this is O(N) per call. Even with autovacuum, the callbacks table will balloon and slow everything down.
   - **Fix:** `CREATE INDEX ON callbacks (callback_id); CREATE INDEX ON callbacks (query_id);`
3. **No cleanup / retention.** `shepherd_brain` and `callbacks` grow forever. At 1M DAU with ~10 callbacks each that's ~10M rows/day into `callbacks`, ~1M into `shepherd_brain`.
   - **Fix:** partition both tables by `start_time` (monthly), and drop old partitions via cron. Or add a daily job `DELETE FROM ... WHERE stop_time < now() - interval '30 days'`.
4. **Sync-query polling hot path.** `get_query_state` is called every 500ms for every in-flight sync query. Even with an index on the PK (already there), that's 3k+ QPS of point SELECTs under modest load. Section 3 says kill this entirely and switch to event-driven notification. That single change drops Postgres QPS by ~10×.
5. **`get_running_callbacks` in lookup workers** (`aragorn_lookup.py:250`, `example_lookup.py:100`, `bte_lookup.py`, `aragorn_pathfinder.py`) polls Postgres once per second per *active lookup query* for up to 210s. At peak that is:
   - 230 qps × ~50% creative × 1 `get_running_callbacks`/s × 60s avg = **~7k PG reads/sec on this one query** alone.
   - Plus every read is `SELECT callback_id FROM callbacks WHERE query_id = %s` — unindexed.
   - **Fix:** same pattern as sync-query. Use `LISTEN/NOTIFY` keyed on query_id, or Redis pub/sub, so the lookup worker wakes when the *last* callback is removed instead of polling.
6. **Connection hold time.** Every DB op uses `async with pool.connection(60)`. That's a 60-second timeout, not a hold time, but it also means a stuck DB call blocks a pool slot for a full minute before error. Drop to 5–10s for writes, keep longer for reads only if justified.
7. **Single Postgres, no replicas, no HA.** An RDS/Cloud SQL-style managed Postgres with at least one hot standby and read replicas (for the polling reads once moved behind pgbouncer) is mandatory at scale. Plan for failover testing.
8. **`varchar(255)` for UUID-8 prefix fields.** Minor — waste of storage on variable-width indexes. Use `text` or a fixed `char(8)`; modern Postgres makes the choice marginal but the current 255 is accidental bloat.
9. **No constraints / transactions across Redis + PG.** `add_query` writes Redis twice then Postgres once; if either Redis write succeeds and PG fails (or vice versa), state diverges. For millions of queries/day, rare two-writer races become common. Either:
   - Use Postgres as the source of truth and Redis as a cache (write-through, rebuildable).
   - Or embrace eventual consistency explicitly with a reconciliation job.

### Required changes (minimum viable)

- **PgBouncer** in transaction mode, `default_pool_size=50`, in front of Postgres.
- **Indexes** on `callbacks (callback_id)`, `callbacks (query_id)`, and `shepherd_brain (state, start_time)` for housekeeping queries.
- **Partition + TTL** on both tables.
- **Drop sync-query polling and lookup callback polling** in favor of push notifications (PG LISTEN/NOTIFY or Redis pub/sub).
- Upgrade to a managed Postgres with replica + PITR, or self-host with Patroni.

---

## 7. Locks — Redis SET NX (db 2)

`shepherd_utils/broker.py:114–144`:
```python
acquired = await lock_client.set(response_id, consumer_id, ex=45, nx=True)
...
for i in range(12):
    ...
    await pubsub.get_message(ignore_subscribe_messages=True, timeout=5)
```

Unlock is a Lua CAS that also `PUBLISH`es to wake waiters. Only `merge_message` uses this (to serialize N parallel callbacks writing into the same response blob).

### Problems

1. **45s TTL is shorter than worst-case merge.** `merge_messages` deep-copies and walks large knowledge graphs; for big responses this easily exceeds 45s. When the TTL expires, Redis deletes the key and another waiter's `SET NX` succeeds — now two workers believe they own the response and both `get_message → mutate → save_message`. The last writer wins silently and results are lost.
   - **Fix:** either (a) extend the TTL with a "watchdog" task that re-`EXPIRE`s every ~15s while the merge is in progress, (b) use Redlock-style fencing tokens, or (c) eliminate the lock entirely by switching to per-callback append-then-fold (no shared mutable state).
2. **Lock waits up to 60s (12 × 5s)** then silently discards the callback (`merge_message/worker.py:732`: *"Failed to obtain lock for {query_id}. Discarding callback response."*). Under a burst of callbacks for the same query, this drops data without retrying on the stream.
   - **Fix:** NACK the stream entry instead of ACKing-and-dropping so the task is re-delivered.
3. **`pubsub.get_message(timeout=5)` holds a pooled lock connection** during the wait. With `max_connections=10` per replica and up to 12 iterations of 5s waits, a few hot queries can saturate the lock pool, blocking unrelated merges.

---

## 8. Worker concurrency model — the general picture

Every async worker uses the same scaffold (`shepherd_utils/shared.py:33`):
```python
task_limiter = asyncio.Semaphore(task_limit)
while True:
    await task_limiter.acquire()
    ara_task = await get_task(stream, group, consumer, worker_logger)
    if ara_task is not None:
        yield ara_task, ctx, task_logger, task_limiter
    else:
        task_limiter.release()
```
and:
```python
async for task, ... in get_tasks(STREAM, GROUP, CONSUMER, TASK_LIMIT):
    asyncio.create_task(process_task(task, ...))
```

So `TASK_LIMIT` is the number of tasks that can be **in-flight** per replica, not per second. A worker that blocks for 210s processes at most `TASK_LIMIT / 210` = 0.5 tasks/sec **per replica** regardless of the limit. That's the number that matters.

### Observations on the model itself

- **The limiter is per-process, in-memory.** It has no idea how loaded downstream services are, how big the queue backlog is, or how many other replicas exist. Fine for protecting a single worker from overload; useless for global flow control.
- **`CONSUMER = str(uuid.uuid4())[:8]`** is the Redis Streams consumer name. As noted in §4, this guarantees orphaned PEL entries on restart. Change to a stable name (pod name / hostname) + add an `XAUTOCLAIM` sweeper.
- **Logger creation per task** (`shared.py:59–64`) allocates a fresh `QueryLogHandler` deque per task. With unbounded `maxlen=None` and long-running tasks that log heavily, this grows. For 1,150 tasks/sec this is millions of handler objects per minute.
  - **Fix:** pool/reuse handlers or use a structured logger (e.g., `structlog`) with bounded buffers.
- **Every task triggers `save_logs`** at the end (`shared.py:106`) which **reads the existing logs, appends, and writes** — O(log_size) per append. Long queries end up doing many append-all-logs cycles.
  - **Fix:** switch to `RPUSH` on a list key or an append-only log stream; do one read at the *end* when logs are returned to the client.
- **`asyncio.create_task` with no reference retention**. If the worker is cancelled, in-flight tasks are garbage-collected without `await`ing — not catastrophic here since they also decrement the limiter in `finally`, but fragile.

---

## 9. Per-worker throughput and replica counts

Model: peak arrival **230 qps** of queries, each fanning out into ~10 task hops, with the mix of operations dependent on the ARA. We estimate **tasks/sec per worker type** at peak, then size replicas as `replicas = ceil(peak_tasks_per_sec / tasks_per_sec_per_replica) × headroom(1.5–2×)`.

Definitions:
- **effective concurrency** = `TASK_LIMIT` (or `min(cpu_count, TASK_LIMIT)` for pool-backed workers).
- **avg task latency** measured or estimated per worker type below.
- **single-replica throughput** ≈ `effective_concurrency / avg_latency_seconds`.

### 9.1 Entry ARA workers (aragorn, arax, bte, example_ara, sipr)

Behavior: `get_message` → pick workflow → enqueue first operation → ACK. Pure routing.

- Avg latency: **~50–200 ms** (one large Redis GET dominates).
- `TASK_LIMIT=100`. Throughput per replica: **~500–2000 tasks/sec** theoretical. In practice capped by Redis data-store bandwidth, not the worker.
- Peak demand per ARA ≈ `230 qps × share_of_ARA`. If ARAGORN handles 50% of traffic, 115 qps.
- **Replicas needed:** 2–3 per ARA type (for HA and headroom). This is not a bottleneck.

### 9.2 Lookup workers (aragorn_lookup, bte_lookup, example_lookup, aragorn_pathfinder)

Behavior: fan out queries to an external service (strider, etc.), register callback IDs in Postgres, then **block polling Postgres every second** until all callbacks come back, or `lookup_timeout=210s` elapses.

`aragorn_lookup.py:244–269`:
```python
MAX_QUERY_TIME = message["parameters"]["timeout"]
while time.time() - start_time < MAX_QUERY_TIME:
    running_callback_ids = await get_running_callbacks(query_id, logger)
    if len(running_callback_ids) > 0:
        await asyncio.sleep(1); continue
    ...
```

- Avg latency: **30–120s typical, 210s timeout worst case**. For this estimate: **~60s**.
- `TASK_LIMIT=100`. Throughput per replica: `100 / 60 ≈ 1.67 tasks/sec`.
- Peak demand: 230 qps × ARA share × (is a lookup for every query that uses this ARA) = ~115 qps for aragorn.lookup.
- **Replicas needed before fix:** `115 / 1.67 × 1.5 ≈ 100 replicas for aragorn.lookup alone.** Each replica sitting mostly idle holding 100 PG connections via the pool. **This is absurd.**
- **With event-driven callback completion** (replace the `get_running_callbacks` polling with LISTEN/NOTIFY or Redis pubsub, and do not hold a task slot open while waiting): avg latency drops from "wait for all callbacks" to "dispatch lookups + return"; wall-clock of the worker task drops to <1s. Throughput per replica → 100+ tasks/sec.
- **Replicas needed after fix:** ~3–5 per lookup type for HA.

Same analysis applies to `bte_lookup`, `example_lookup`, `aragorn_pathfinder`. **This is the biggest operational cost in the current design and a trivial-to-medium refactor fixes it.**

### 9.3 `merge_message` (creative mode fan-in)

`workers/merge_message/worker.py:659–665`:
```python
cpu_count = os.cpu_count()
cpu_count = min(cpu_count, TASK_LIMIT)   # TASK_LIMIT = 10
executor = _make_executor(cpu_count)
```

Each merge deserializes 3 messages (query, response, callback), walks both knowledge graphs, filters promiscuous nodes, merges aux graphs, rewrites the response. Deep-copies are used liberally in `merge_messages` and `merge_kgraph`.

- Per-merge CPU: **0.3–5s** depending on KG size; **10s+** for pathological creative queries with thousands of rules.
- Effective concurrency: `min(cpu_count, 10)` per replica. On an 8-core node → 8 in-flight merges.
- Throughput per replica: `8 / 2s ≈ 4 merges/sec` average.
- Peak demand: each creative query produces ~5–15 callbacks (see `aragorn_lookup.expand_aragorn_query`). If 50% of traffic is creative and each has 10 callbacks: `230 × 0.5 × 10 = 1,150 merges/sec peak`.
- **Replicas needed:** `1150 / 4 × 1.5 ≈ 430`. That's obviously wrong — the real constraint is that this worker shouldn't touch the full blob on every callback.
- **Fix:** redesign merge as append-only per-callback (store each callback's deltas under its own key, run a single final aggregation in `finish_query` or a new `collapse_callbacks` worker). Replicas drop to ~10–20 of a much cheaper collapse worker, sized by aggregate work, not by callback count.

Also fix the **45s lock TTL** issue from §7 before scaling this worker horizontally, or big-response queries will corrupt under concurrency.

### 9.4 Filters and sort (`sort_results_score`, `filter_results_top_n`, `filter_results_top_n`, `filter_kgraph_orphans`, `filter_analyses_top_n`)

Behavior: single-pass over results or KG.

- `get_message` + sort/filter + `save_message` cycle: **0.3–2s per task** dominated by compress/decompress of the full blob.
- `TASK_LIMIT=100` but these are **in-loop**, so they GIL-block the event loop during the orjson/zstd phases — real parallelism is maybe 3–5 at a time.
- Throughput per replica: **~2–5 tasks/sec** realistic.
- Peak demand: ~230 qps for each of sort and filter (every query does each).
- **Replicas needed before fix:** `230 / 3 × 1.5 ≈ 115` per filter type.
- **Fix:** (a) store `results` as its own key so these workers don't round-trip the entire message, and (b) run the compress/decompress step in a `ProcessPoolExecutor` so the event loop doesn't block. Expected 10–20× throughput per replica; target **5–10 replicas** of each.

### 9.5 `aragorn_score`, `arax_rank` (CPU-heavy scoring)

`TASK_LIMIT=10`, `ProcessPoolExecutor` sized to `min(cpu_count, 10)`.

- Per-score CPU: **1–10s** depending on result count (NumPy-heavy in `aragorn_score`).
- Throughput per replica (8-core): `8 / 3s ≈ 2.6 scores/sec`.
- Peak demand: one score per creative query = ~115 qps.
- **Replicas needed:** `115 / 2.6 × 1.5 ≈ 65`. Real number depends heavily on per-query result sizes — benchmark with production traces. Plan for **30–80** of each.

### 9.6 `score_paths` (GPU, `TASK_LIMIT=1`)

One task at a time, pins a GPU.

- Per-task latency: **5–30s** depending on path count (SentenceTransformer + XGBoost inference).
- Throughput per replica: **~0.05–0.2 tasks/sec**.
- Peak demand: one per pathfinder query. If pathfinder is ~5% of traffic → ~12 qps peak.
- **Replicas needed:** `12 / 0.1 × 1.5 ≈ 180 GPUs`. That's the true cost of GPU scoring at this scale and almost certainly not what you want.
- **Mitigations:**
  - **Batching**: rewrite `score_paths` to pull N tasks off the stream, run one bigger batch through the transformer, split results. Batched inference on GPU is usually 10–50× faster per sample.
  - **Quantize / distill** the sentence transformer (MiniLM-level + int8) — often a 5–10× speedup with minor quality loss.
  - **CPU-only inference** for long-tail traffic with a small GPU fleet for premium requests.
- Realistic target after batching + quantization: **3–10 GPUs**.

### 9.7 `gandalf`, `gandalf_rehydrate` (`TASK_LIMIT=1`, memory-heavy)

Large graph held in-process via `gandalf_mmap`. By design, one task per process; scale horizontally.

- Per-task latency: varies widely by query; in the 1–60s range.
- Throughput per replica: `1 / avg_latency`.
- Peak demand depends on how often the Gandalf path is chosen.
- Memory per replica: sized by the CSR graph (multi-GB). Horizontal scale is expensive.
- **Required:** the mmap file must be on a **shared read-only volume** across replicas (RO persistent volume), or built into the image. The current `./gandalf_mmap` host mount works only because there's one instance.

### 9.8 `finish_query`

`TASK_LIMIT=100`. Sends outbound callback for async queries (`httpx.AsyncClient(timeout=120)`), updates Postgres.

- Per-task latency: ~0.1s for sync completion, up to 120s for async callback HTTP.
- Assume 50% async queries with ~1s avg callback latency: **~0.5s avg**.
- Throughput per replica: `100 / 0.5 = 200 tasks/sec`.
- Peak demand: ~230 qps (one per query).
- **Replicas needed:** 2–3 for HA.

### 9.9 Summary table (post-fix targets)

| Worker | Current `TASK_LIMIT` | Effective concurrency | Needed replicas (peak, after recommended fixes) |
|---|---:|---:|---:|
| ARA entry (aragorn/arax/bte/sipr/example_ara) | 100 | 100 | 2–3 each |
| `*_lookup` (aragorn/bte/example/pathfinder) | 100 | ~5 effective (polling) | **3–5 each after push-notify fix** |
| `merge_message` | 10 (+ procpool) | 8 | **10–20 after delta redesign** |
| `sort_results_score` | 100 | ~3–5 | 5–10 |
| `filter_*` (×3) | 100 | ~3–5 | 5–10 each |
| `aragorn_score` | 10 (+ procpool) | 8 | 30–80 |
| `arax_rank` | 10 (+ procpool) | 8 | 10–30 |
| `score_paths` | 1 (GPU) | 1 | 3–10 GPUs after batching |
| `gandalf`, `gandalf_rehydrate` | 1 | 1 | Benchmark; likely 5–20 each |
| `finish_query` | 100 | 100 | 2–3 |

These numbers assume the fixes in sections 3–6 land. **Without the data-store fix in §5, no horizontal scaling saves you — Redis is the cliff.**

---

## 10. Observability, logging, and operations

- **OpenTelemetry → Jaeger** is wired through the server and every worker (`shepherd_utils/otel.py`, propagated via the `otel` stream field). Good foundation, but Jaeger `all-in-one` from compose.yml is single-node, memory-backed, and drops spans under load. At target scale use a managed tracing backend (Tempo, Honeycomb, Datadog) with tail sampling.
- **Logs.** Per-task `QueryLogger` deques have `maxlen=None`. Under long tail queries + DEBUG level this is effectively a memory leak per task. Set a sensible `maxlen` (e.g., 10k entries) and truncate.
- **No metrics.** There are no Prometheus counters, no queue-depth gauges, no task-latency histograms. Autoscaling lookup workers by `XLEN` requires metrics: expose one.
- **No health checks.** Workers have no readiness probe. In Kubernetes they'll be marked ready immediately and start accepting traffic before the DB pool or Redis pool is warm. Add `/healthz` to each worker (a tiny aiohttp server beside the poll loop is enough).
- **No per-worker resource limits in compose.yml.** At scale, declare `resources.limits` (CPU, memory) so a leak in one worker doesn't evict others on the node.
- **Reload loop writing to a mounted log directory** (`./logs:/app/logs`) ties all container logs to one host path. In production, log to stdout and rely on the cluster's log collector (Fluent Bit, Loki, etc.) — already the code path when `KUBERNETES_SERVICE_HOST` is set, so make sure deployments set it.
- **Startup order.** The compose `depends_on: condition: service_healthy` for `shepherd_db` and `shepherd_broker` is fine for a dev box, but in Kubernetes you need proper init containers or resilient retry loops — some DB calls retry (`PG_RETRIES=5`), but Redis pool open is not retried on first connect.

---

## 11. Correctness risks that appear under load (not just performance)

These are latent bugs today that become frequent at scale. Listed separately because they are not "slow" — they are "wrong".

1. **LRU eviction of in-flight TRAPI blobs** (§5). Silent data loss.
2. **Lock TTL < merge time** (§7). Two workers merge into the same response; last write wins.
3. **Lost pending entries on worker restart** (§4 / §8). Tasks in the PEL of a dead consumer are never reclaimed. Some queries hang until timeout.
4. **Dropping callbacks when the merge lock can't be acquired within 60s** (`merge_message/worker.py:732`). Partial results delivered to the user as if complete.
5. **Decoupled Redis+PG writes in `add_query`** (§6). If PG write fails after Redis writes succeed, the blob sits in Redis with no state row, TTL-expiring silently.
6. **`handle_task_failure` ACKs the failing task** (`shepherd_utils/shared.py:115`). If the failure is transient, there is no retry — the query jumps directly to `finish_query` with `status=ERROR`.
7. **`save_logs` concurrent readers** (`shepherd_utils/db.py:273`) — read-modify-write of the logs blob has no lock. Two workers logging at once will lose one of their log batches.
8. **`broker_client.xadd` is fire-and-forget.** `add_task` logs on error and returns; the caller has no way to detect "task was not actually enqueued" and will believe the query is progressing.

All of these need to be fixed before you push traffic through this system at hundreds of qps.

---

## 12. Target architecture

```
                      ┌────────────────────────┐
                      │   L7 LB + WAF          │
                      └──────────┬─────────────┘
                                 │
                  ┌──────────────┴──────────────┐
                  ▼                             ▼
          ┌───────────────┐             ┌───────────────┐
          │ FastAPI pods  │  10–16 pods │ callback pods │  dedicated pool
          │ (gunicorn +   │             │ (inbound from │
          │  uvicorn × n) │             │  strider etc.)│
          └───────┬───────┘             └───────┬───────┘
                  │                             │
                  └────────────┬────────────────┘
                               ▼
               ┌──────────────────────────────────┐
               │ Object store (S3 / MinIO)        │  TRAPI blobs
               │  keyed by response_id            │
               └──────────────────────────────────┘
               ┌──────────────────────────────────┐
               │ Broker: Redis Streams HA (or     │  small task messages
               │ NATS JetStream / RabbitMQ)       │  only
               └──────────────────────────────────┘
               ┌──────────────────────────────────┐
               │ Redis (hot KV, short-TTL cache)  │  coordination + small state
               │ + replica for HA                 │
               └──────────────────────────────────┘
               ┌──────────────────────────────────┐
               │ PgBouncer → Postgres primary     │
               │     + read replica(s)            │
               │ partitioned + indexed            │
               └──────────────────────────────────┘
                               ▲
                               │ autoscale by stream depth
                               │
         ┌────────────┬────────┴─────┬────────────┬────────────┐
         ▼            ▼              ▼            ▼            ▼
     ARA entry     lookup       merge/collapse   score     score_paths
     (HPA)        (HPA, push-    (stateless,    (HPA by     (GPU pool,
                   notified,      delta-based)   CPU +       batched)
                   no polling)                  backlog)
```

Key properties:
- **TRAPI blob lives in object storage**; Redis holds keys + small coordination state only.
- **Broker carries IDs**, never payloads.
- **Sync waits and callback waits are push-notified**, not polled.
- **Every worker type is HPA-scalable** by queue depth (`XLEN`).
- **Database is fronted by a connection pooler** and has read replicas for read-heavy ops.
- **Every stateful piece has a replica** (Redis Sentinel/Cluster, Postgres primary+standby, optional S3 cross-region).

---

## 13. Prioritized roadmap

This is the order I'd pursue, weighing impact vs. effort. Do not skip ahead — #1–#3 are prerequisites for anything above the prototype load.

### Phase 0 — stop the bleeding (days, not weeks)

1. **Switch Redis `maxmemory-policy` to `noeviction`** and add alerting on `used_memory > 75%`. Correctness fix, trivial.
2. **Add indexes** on `callbacks(callback_id)`, `callbacks(query_id)`, and `shepherd_brain(state, start_time)`.
3. **Bound streams** with `XADD ... MAXLEN ~ 100000` on every `add_task` call.
4. **Set `maxlen` on query-logger deques** (e.g., 10k).
5. **Use stable consumer names** (pod hostname) and add a small `XAUTOCLAIM` sweeper service that reclaims idle-pending entries older than 5 min.
6. **Fix the merge lock TTL**: add a watchdog that re-`EXPIRE`s the lock every 15s while the merge is in progress.

### Phase 1 — remove the polling hot paths (1–2 sprints)

7. **Replace sync-query polling** with Postgres `LISTEN/NOTIFY` (emit `NOTIFY shepherd.query_done, qid` in `set_query_completed`) or Redis pubsub. Drops thousands of PG reads/sec.
8. **Replace lookup-callback polling** (`get_running_callbacks`) with the same mechanism. This alone collapses lookup worker replica count by ~20×.
9. **Introduce PgBouncer** (transaction pooling, pool size 50). Reduce each application pool's `max_size` to 2–4.
10. **Partition `callbacks` and `shepherd_brain`** by `start_time` + daily drop job.

### Phase 2 — de-blob the data path (the real work, 1–2 quarters)

11. **Move the TRAPI blob to object storage** (S3 / MinIO). `response_id` maps to `s3://shepherd/responses/{response_id}`. Redis holds a version/etag for CAS; Postgres holds the state.
12. **Redesign per-step I/O**: split `response` into `results`, `knowledge_graph`, `auxiliary_graphs`, `query_graph` as independently-stored sub-objects so filter/sort workers don't rewrite the whole thing.
13. **Redesign `merge_message`** as append-only per-callback delta + a single fold pass (`collapse_callbacks`) that runs after all callbacks are in. This eliminates the per-callback lock entirely.
14. **Run compress/decompress in `ProcessPoolExecutor`** (or pre-split workers) so async-loop workers stop GIL-blocking on multi-MB JSON.

### Phase 3 — HA and horizontal scale (ongoing)

15. **Move Redis to a clustered / replicated topology** (Sentinel for the coordination instance; data moves to S3 per #11). Separate broker Redis from coordination Redis.
16. **Managed Postgres** (RDS / Cloud SQL / Aurora) with automatic failover and read replicas; direct polling reads to the replica.
17. **FastAPI deployment**: gunicorn + uvicorn workers, 10–16 pods behind an L7 load balancer; split inbound callback traffic into its own pod pool.
18. **HPA every worker deployment** on `XLEN` of its stream (expose Prometheus counter + use `prometheus-adapter` to feed HPA), with section 9 numbers as a floor.
19. **Batch GPU inference** in `score_paths`; quantize the sentence transformer; reduce GPU count ~10×.
20. **Distributed tracing backend** (Tempo/Honeycomb) with tail-based sampling on long queries.

### Phase 4 — operational maturity

21. Dead-letter stream + redrive tool.
22. Per-query SLO dashboards (P50/P95/P99 by ARA + workflow step).
23. Chaos tests: kill broker / DB / random worker; confirm no query silently fails.
24. Load tests that actually produce realistic TRAPI message sizes — the current compose's `test_response.json` is orders of magnitude smaller than production messages and hides the §5 problems.

---

## TL;DR

The biggest single lie in today's configuration is **`TASK_LIMIT=100` on lookup workers**. It reads as "100 tasks/sec per replica" but actually means "100 queries can sit blocked for 210 seconds each polling the DB". The second biggest lie is **4 GB of LRU Redis holding every in-flight TRAPI blob**. Fix those two (sections 5 and 9.2), plus the sync-query polling (section 3), and Shepherd is genuinely scalable — everything else becomes a matter of replica counts and HPA config. Ignore them and no amount of horizontal scale will help: Redis will evict your data and lookup workers will spend their lives sleeping on a Postgres poll.

