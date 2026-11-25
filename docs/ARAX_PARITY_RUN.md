# Live parity run: the ARAX port vs. upstream ARAX

`scripts/arax_compare.py` sends the same TRAPI 2.0 query to Shepherd's ARAX port
and to a deployed upstream ARAX at the same time, and diffs the answers. It uses
the query cases of `scripts/test_arax_queries.py` (lookups, creative mode,
ARAXi, workflows, validation errors), except pathfinder: Shepherd answers path
queries with its own `arax.pathfinder` worker (DEC-7), and upstream's pathfinder
is no longer used. Run it from a machine that can reach both
instances.

Upstream's development instance runs the `trapi2.0` code the port follows:
`https://arax.ncats.io/devED/api/arax/v2.0` (the script's default).

## 1. Make the two sides answer from the same inputs

Most differences in a live run come from the inputs, not the port. Before
comparing:

- **Retriever.** Each side queries its own Retriever. Set Shepherd's
  `SYNC_KG_RETRIEVAL_URL` to the Retriever upstream's dev instance uses. Both
  responses record the URL each one used in their query plans, so check one
  pair of saved responses (`<case>.port.json` / `<case>.upstream.json`) first.
  Upstream points its development maturity at `https://dev.retriever.biothings.io/query`.
- **ARAX data files.** `ARAX_TIER_VERSION=tier0-20260928` (the default), with
  the real files rather than mock data (DEC-30). The pathfinder files
  (`curie_ngd`, tier0-info-for-overlay) are the same build
  (`ARAX_PATHFINDER_TIER_VERSION=tier0-20260928`); with the older
  tier0-20260621 `curie_ngd`, NGD values differ slightly and NGD edges have no
  publications.
- **In-process ARAX.** The script sets `parameters.arax_internal: true` on
  Shepherd's queries, so they run the port whatever `ARAX_INTERNAL_DEFAULT` is.
  `--no-arax-internal` turns that off.
- **KP cache.** `parameters.bypass_cache: true` is sent to both sides by default,
  so neither answers from a cache the other doesn't have
  (`--no-bypass-cache` turns it off).

## 2. Run

```bash
python scripts/arax_compare.py --port http://localhost:5439/arax
python scripts/arax_compare.py --upstream https://arax.ncats.io/beta/api/arax/v2.0   # another upstream instance
```

Other options:

```bash
python scripts/arax_compare.py --list                       # the cases, by group
python scripts/arax_compare.py --group lookup creative      # some groups
python scripts/arax_compare.py --only mvp1 lookup_two_hop   # cases by name
python scripts/arax_compare.py --workers 1                  # gentler on the dev instance
```

Every pair of responses, and the diff of each case, is saved under `--out`
(default `responses/arax-compare/`). `summary.md` is the table to read first.

The saved responses can be large (the full answers, a couple of GB for all
cases). To share a run, pack it:

```bash
python scripts/arax_compare.py --from-saved --pack    # re-diff the saved responses, no new queries
```

`--from-saved` recomputes every diff from the saved responses (useful after
the script's comparison changes), and `--pack` writes
`responses/arax-compare/arax-compare-pack.zip`: the summary, every case's diff,
and a digest of each response (outcome, ERROR / WARNING logs, query plan, the
top 25 results, and only the KG edges and support graphs they use).

## 3. Reading the results

Each case is:

- **SAME**: same HTTP status, ARAX status and error code, the same results with
  the same scores in the same order, the same KG nodes and edges, the same
  support-graph sizes and the same WARNING / ERROR log codes;
- **SIMILAR**: the same outcome, and results and KG edges overlap at least 90%
  (`--similar-at`). Ranks or scores may differ;
- **EXPECTED**: the answers differ, for a reason recorded in the script's
  `EXPECTED` table (a decision in `docs/ARAX_PORT_BASELINE.md`, or how upstream's
  deployment behaves). The reason is in the summary;
- **DIFF**: anything else. Open `<case>.diff.json` to see which layer differs,
  with examples on each side.

For each case the diff also records each side's first ERROR log messages
(`errors`), the results with the largest score differences, and the KG edges
both sides have but with other attribute types or knowledge level / agent type
(`edge_details`; the ranker scores on those).

Results are matched by their node bindings, and KG edges by subject, predicate,
object and primary knowledge source, so edge keys don't matter.

### Differences expected by design

These come from recorded decisions in `docs/ARAX_PORT_BASELINE.md` and are not
port defects:

- **A qnode `name`** is resolved to ids by Shepherd (D-25). Upstream `trapi2.0`
  rejects it as an unknown property.
- **Knowledge-source constraints** are read from both `constraints.sources` and
  the knowledge-source attribute constraints (DEC-29b). Upstream reads only
  `constraints.sources`.
- **A multi-id qnode's subclass parent** is inferred from a `subclass_of` edge
  in the KP answer (DEC-29c). Upstream maps no parent.
- **A `kp` list** is forwarded to Retriever as `parameters.kp` (DEC-10).
- **xCRG** runs the same package as upstream (DEC-25), so its known bugs
  (BioPack-team/shepherd#273, #274, #275 and #276) affect both sides alike.
- **NGD values** and NGD publications differ if the pathfinder files are not
  the tier0-20260928 build (see section 1).

### Ties between results

ARAX gives results with equal raw scores positional scores (0.001 apart, 0
past the 1000th) in the order resultify creates them, which is Python set order
and so follows the process's string-hash seed. Tied results can therefore swap
scores, and a result cap can keep different tied results, between Shepherd and
upstream, though each is repeatable on its own: the `arax` worker runs with
`PYTHONHASHSEED=0`, and upstream forks every query from one server process.
Expect this in one-hop answers with many equally supported edges (text-mining
answers, for example). To measure it, restart the `arax` worker with another
`PYTHONHASHSEED` and compare the port with itself.

The comparison ignores where each side puts its query plan, the envelope `id`,
and log texts and timestamps.

## 4. Node normalization: can the calls go?

The port's Node Normalizer calls take seconds on large answers (DEC-34).
Two ways to measure whether ARAX needs them:

**Per call site, in one run (`compare`).** Run the port with
`ARAX_NODE_NORMALIZATION=compare` on the `arax` worker, or pass the parameter
to the port side only:

```bash
python scripts/arax_compare.py --port-params '{"arax_node_normalization": "compare"}'
```

Each response then logs a `NodeNormalizationComparison` entry: for every call
site, how many CURIEs it looked up and how many would have answered
differently if they were taken as already normalized. The script sums them
over all cases into `normalization.md` (and `normalization.json`) under
`--out`, with the cases and examples behind each count. Kinds:

- `id`: the Node Normalizer's preferred id is another CURIE (a query id that
  isn't the preferred one, for example);
- `unknown`: the Node Normalizer doesn't know the CURIE;
- `category` / `no_category`: the preferred category differs, or no KP
  returned the node, so `off` has no category for it;
- `name`: the name differs;
- `equivalents`: the Node Normalizer knows other equivalent ids. This only
  matters where the caller uses them (Expand's synonym lookups).

**End to end (`off` against `on`).** Compare Shepherd with itself:

```bash
python scripts/arax_compare.py \
    --upstream http://localhost:5439/arax --upstream-params '{"arax_internal": true}' \
    --port-params '{"arax_node_normalization": "off"}' --no-expected \
    --out responses/nodenorm-off
```

The "port" side runs with normalization off, the "upstream" side with it on.
`--no-expected` stops the port-vs-upstream EXPECTED table from hiding
differences. Ties between results still apply (section 3), so check SIMILAR
cases against a control: the same command without `--port-params`.

**Only the query's own nodes (`query_nodes`).** `off` for the lookups of
knowledge-graph nodes (NGD, FET's neighbor counts, `add_node_pmids`, COHD,
thousands of CURIEs each), `on` for every other lookup: the query's own ids and
names, where the Node Normalizer's category changes what ARAX does (the
category FET counts neighbors of, for a query node without one). Run it as the
`off` run above, with `"query_nodes"` for `"off"`; `normalization.md` then says
which call sites asked the Node Normalizer.

The calls can go when the `compare` report shows nothing but counts that don't
change an answer, and the `off` run is SAME or SIMILAR within the control's
noise. The report goes in the response's INFO logs, so don't set
`parameters.log_level` above INFO.

