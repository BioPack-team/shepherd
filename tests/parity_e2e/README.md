# Differential parity harness (Layer 4)

Runs the pinned original ARS (NCATSTranslator/Relay @
`dd1e71b8284de746f9d11e4fc823bf57861e081f`) and Shepherd's port side by
side against one mocked world, and diffs everything observable. This is
the release gate described in `docs/ARS_PARITY_REGISTER.md`; layers 1-3
run in per-PR CI, this layer runs on demand where Docker and the Relay
checkout are available.

## Topology

- **mockworld** (`mockworld/app.py`): stub ARAs (sync + async-with-callback,
  scriptable per scenario: happy/empty/error/503/garbage/silent/slow), a
  stub smart-api.info registry resolving every armed infores to itself,
  stub Appraiser (zstd, deterministic ordering components), Annotator,
  NodeNorm, and a notification sink. Journals every inbound request.
- **Shepherd stack**: `compose.yml` + this directory's overlay
  (`compose.parity.yml`) repointing external URLs at the mockworld.
- **Relay stack**: the pinned checkout's own `docker-compose.yml`, with env
  `TR_ANNOTATOR/TR_APPRAISE/TR_NORMALIZER` pointed at the mockworld and its
  `tr_smartapi_client.smart_api_discover.urlSmartapi` patched to the stub
  registry (a one-line settings override in the pinned checkout; both
  stacks must resolve ARAs to the same mockworld URLs).

## Running

```console
# 1. Shepherd stack + mockworld
docker compose -f compose.yml -f tests/parity_e2e/compose.parity.yml up --build

# 2. Relay stack (from the pinned checkout)
cd /path/to/relay && docker compose up

# 3. Drive the corpus
python tests/parity_e2e/run_parity.py \
    --relay-url http://localhost:8000 \
    --shepherd-url http://localhost:5439 \
    --mockworld-url http://localhost:8099
```

`--scenario <name>` runs a single scenario; `--include-slow` adds the
timeout scenarios (each waits out the 5-minute 598 sweep). Exit code 0 =
full parity; a nonzero exit writes field-level differences to
`parity_report.json`.

## What is compared

1. the submit envelope status,
2. the terminal `?trace=y` tree summary (per-child terminal
   agent/status/code/result_count multiset, parent state, merged-version
   bookkeeping),
3. the final merged message content (normalized: uuids/timestamps/hosts
   masked, order-insensitive fallback for the documented set-union spots),
4. the mockworld journal of outbound side effects (which ARAs were called
   and how, appraiser/annotator traffic, notifications).

The normalizer lives in `normalize.py` and is unit-tested in
`tests/unit/ars/test_parity_harness.py`; scenario definitions in
`scenarios.py` reuse the layer-1 corpus fixtures so merge inputs are the
same TRAPI the golden tests pin.
