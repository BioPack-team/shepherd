# Upstream ARAX's test suite, run against the port

The `test_*.py` files here are upstream ARAX's own tests
(`RTXteam/RTX @ 9485431`, `code/ARAX/test/`), copied unmodified (MIT, see
`LICENSE.RTX`). `upstream_alias.py` resolves their imports (`ARAX_query`,
`Expand.expand_utilities`, `openapi_server.models.*`, `node_synonymizer`, ...)
to the port's modules under `shepherd_utils/arax/`, so the same tests exercise
the port.

## In CI (offline)

`pytest tests/unit/arax/upstream_suite` runs the 81 tests in
`offline_passing.txt`. These are every upstream test that passes on upstream
ARAX itself with no network and no ARAX data files. The rest are skipped
because they fail offline on upstream too. The two xfails are the port's
recorded difference DEC-4 (xCRG uses Shepherd's Retriever).

## Against live services (`--arax-live`)

The other upstream tests need the Translator services (Retriever, NodeNorm,
NameRes, NCBI eUtils) and ARAX's real data files. Run them where those are
reachable:

```bash
export SYNC_KG_RETRIEVAL_URL=https://retriever.ci.transltr.io/query   # the Retriever to test against
export ARAX_DBS_DIR=$PWD/arax_dbs                                     # the real ARAX data files
export ARAX_PATHFINDER_DBS_DIR=$PWD/arax_pathfinder_dbs
export ARAX_KP_CACHE_ENABLED=false                                    # no data store needed
PYTHONHASHSEED=0 pytest tests/unit/arax/upstream_suite --arax-live --runslow --runexternal
```

Upstream's own options work as in ARAX (`--runslow`, `--runexternal`,
`--runbroken`, `--runonly*`). Many of these tests assert on real biology, for
example "acetaminophen has more than N results". So they need the real
ExplainableDTD, curie_to_pmids and COHD files, not the mock ones from
`shepherd_utils.arax_mock_data`. They also depend on what the live Retriever
returns on the day they run.

To see which differences are the port's, run the same command in an upstream
RTX checkout (`cd RTX/code/ARAX/test && pytest --runslow --runexternal`) and
compare the two lists of failures. A test that fails on both sides because of
the services is not a port difference.
