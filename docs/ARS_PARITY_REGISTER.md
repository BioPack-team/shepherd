# ARS Parity Register

The behavior contract for Shepherd's hosted port of the Translator ARS
([NCATSTranslator/Relay](https://github.com/NCATSTranslator/Relay)).

**Pinned upstream commit:** `dd1e71b8284de746f9d11e4fc823bf57861e081f`
(master, cloned 2026-09-01; byte-exact reference copies under
`/home/user/ncatstranslator/relay` during development, key files mirrored in
the golden-generation tooling).

**Golden regeneration:** goldens are produced by *running the upstream code*
over the corpus, never hand-written:

```console
$ python3.11 -m venv .venv-relay && .venv-relay/bin/pip install \
    django==4.2.23 celery==5.5.3 scipy==1.10.1 sympy==1.13.3 "numpy<2" \
    zstandard==0.23.0 objsize requests pyyaml pycryptodome pymysql \
    reasoner-pydantic==5.1.1 pydantic==1.10.22 opentelemetry-api \
    opentelemetry-sdk opentelemetry-instrumentation-celery "redis>=5,<6"
$ python scripts/ars_parity/build_corpus.py
$ PYTHONHASHSEED=0 .venv-relay/bin/python scripts/ars_parity/generate_goldens.py \
    --relay /path/to/relay-checkout
```

Re-pinning to a newer Relay commit means: update the SHA here and in
`generate_goldens.py`, regenerate, review the golden diff, and consciously
accept each behavioral change.

## Test layers

| Layer | What it pins | Where |
|---|---|---|
| 1. Golden function parity | merge/premerge/filters/scoring/blocklist/validation outputs, byte-compared to upstream runs | `tests/unit/ars/test_golden_parity.py` |
| 2. Lifecycle & state machine | status letters/coercion, completion arithmetic, orchestration, worker state machines | `test_statuses.py`, `test_completion.py`, `test_ars_lifecycle.py`, `test_ars_fanout.py`, `test_ars_merge_worker.py`, `test_ars_postprocess.py`, `test_ars_watchdog.py`, `test_ars_notify.py` |
| 3. API contract | paths, methods, status codes, error bodies, envelope shapes | `test_envelope.py`, `test_ars_api_contract.py` |
| 4. Differential end-to-end | both stacks against the same mocked world | `tests/parity_e2e/` (run on demand; see its README) |

## Invariant index (register rows referenced from tests)

- **P-ST-1..4** — the six status letters, terminal set `{D,S,E,U}`,
  long-name mapping, save-time code coercion (R→202, D→200) with the
  `_skip_post_save` escape hatch.
- **P-ENV-1..6** — Django-serializer envelopes: DjangoJSONEncoder datetime
  format, exact model field order, long-form statuses, inline-decompressed
  `data`, FK/pk shapes.
- **P-LC-1..6** — parent-completion counting verbatim from
  `message_post_save`: `finished` over the terminal set;
  `orig_count`/`merge_count` from result-bearing `ar*` agents; the
  `'E'/444` merge child satisfying its origin while any other merge error
  decrements; the empty-completion branch synthesizing an empty merged
  message (and, faithfully, *not* clearing subscriptions there).
- **P-NT-1..5** — `notify_subscribers` field overrides ('D'→admin,
  'E'→ars_error), stats attachment, the `{pk, timestamp, code}` payload
  base, `last_merged_completed` forcing code 200, per-client HMAC-SHA256
  over compact sorted-key JSON.
- **Callback guard order** — dup-Done → 200 text; repeated results → 409;
  errored child → 400; decode failure → 500 `Can not decode json...`;
  validation failure → 422 `Problem with TRAPI Validation` with the child
  E/422; header `tr_ars.message.status` override; `results: null` → 
  `result_count = 0` while `results: []` leaves it None.
- **Timeouts** — 15-minute scan window on *creation* time; parents exempt;
  merge children 8 min; everything else **5 min including pathfinder**
  (upstream's code, not its log message); code 598.
- **Known-broken endpoints reproduced** — `POST /ars/api/messages` (500),
  `POST /ars/api/actors` (creates the actor, then 400
  `Not a valid json format`), `GET /ars/api/merge/<pk>` (creates the shell
  merge child, then 500), `timeoutTest`/`post_process` debug (500).
- **Upstream error-behavior parity** — `decorate_edges_with_infores` raises
  (UnboundLocalError) on non-empty sources with no primary;
  `normalizeScores` raises IndexError on mixed scored/unscored results;
  `mergeDicts` qualifier lists never merge (swallowed KeyError); the
  `node_bindings` for/else; the `attributes`/`analyses` early returns; a
  failed merge fold leaves the shell merge child Running for the watchdog.

## Documented deviations (all consciously accepted)

Infrastructure substitutions (behavior-preserving by definition):

| Upstream | Port |
|---|---|
| Celery on RabbitMQ (+beat) | Redis Streams workers (`ars.fanout`, `ars.merge`, `ars.postprocess`, `ars.notify`) + the `ars_watchdog` loop |
| MySQL rows with inline zstd blobs | Postgres `ars_*` rows; blobs in Redis (hot) + `ars_message.data` bytea (durable, written at terminal status) |
| `merge_semaphore` + `select_for_update` + celery retry | broker lock per parent (semaphore column still maintained for envelope parity) |
| `expensive_gate` 12-token redis ZSET | per-worker `TASK_LIMIT` / pool sizing |
| self-proxy views `/ara-*/api/runquery` | direct POST to the SmartAPI-resolved remote (same body; proxy endpoints not served) |

Behavioral deviations:

1. **reasoner-pydantic replaced with Shepherd pydantic-v2 models**
   (`shepherd_utils/ars/trapi.py`), per project direction. Field
   requirements were dumped from the installed upstream package and verdict
   parity is golden-tested over valid + broken corpora.
2. **Annotator transport**: HTTP API (`TR_ANNOTATOR`) instead of the
   `biothings_annotator` package (a git dependency pinning conflicting
   libs). Same service, same resulting `biothings_annotations` attributes.
3. **Watchdog intent fixes**: upstream indexes the Agent table with the
   *actor's* pk (outcome depends on row-id coincidence) — the port joins
   actor→agent properly; and `ars-workflow-agent` parents are exempted
   alongside `ars-default-agent` (upstream would 598 workflow parents at 5
   minutes).
4. **Watchdog cadence**: 60s sweep vs. 3-minute beat. A timed-out message is
   marked *sooner after* its threshold; the thresholds themselves are
   identical.
5. **Callback micro-paths**: the async-200 self-GET race probe is dropped
   (upstream persists nothing on that path); the dead branch that upstream
   hits when a child already has stored `data` (a `str in bytes` TypeError
   → 500) is not reproduced.
6. **merge failure retries**: upstream celery-retries `merge_and_post_process`
   up to 20× (creating a fresh merge child per attempt); the port fails once
   and leaves the shell merge child for the watchdog — the terminal outcome
   (E/598 merge child decrementing `orig_count`) is the same shape.
7. **notify stats on a custom-fields-less call**: upstream raises TypeError
   when `result_count` is set and no fields dict exists; the port carries
   the stats in a fresh dict.
8. **Empty-completion robustness**: when the parent's payload is missing
   (Redis TTL + no durable copy), the port synthesizes the empty merged
   message from `{"message": {}}` instead of upstream's KeyError.
9. **404 body for non-UUID pks**: Django returns its HTML 404 page (URL
   resolution fails); the port returns the endpoint's own
   `Unknown message: <pk>` text. Status code identical.
10. **Retention**: upstream never purges (out-of-band cleanup honors
    `retain`); the port nulls durable payload copies after
    `ars_data_retention_days` for non-retained terminal messages, keeping
    row metadata.
11. **`GET /ars/api/messages` payload inclusion** and other list endpoints
    load payloads from the blob store; a payload evicted from Redis with no
    durable copy renders `fields.data: null` (upstream MySQL always had it
    inline).
12. **Notification delivery retries** run in-process with upstream's backoff
    envelope (cap 300s, jitter, 8 attempts) instead of celery re-delivery.
13. **normalized_score is a plain float**: upstream stores rankdata's
    numpy.float64 through stdlib json (which accepts it as a float
    subclass); Shepherd's orjson blob codec rejects numpy scalars, so the
    port casts via ``.tolist()`` at the production site. Identical numeric
    values; regression-tested against the blob codec round-trip.

## Not ported (documented drops)

- Django admin, the websocket echo consumer, the HTML status/answers pages
  (`/ars/app/*`, `/ars/answer/<pk>`), and the tr_kp proxy views. The JSON
  APIs those pages consume are all served.
- `ara-explanatory` special-case remains as the callback-injection skip
  only, as upstream.
