# Live parity run: the ARAX port vs. upstream ARAX

`scripts/arax_compare.py` sends the same TRAPI 2.0 query to Shepherd's ARAX port
and to a deployed upstream ARAX at the same time, and diffs the answers. It uses
the 67 query cases of `scripts/test_arax_queries.py` (lookups, creative mode,
ARAXi, workflows, validation errors). Run it from a machine that can reach both
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
  the real files rather than mock data (DEC-30). Until the pathfinder files move
  to tier0-20260928 (their own PR), NGD values read from `curie_ngd` have no
  publications attribute, so NGD edges will differ.
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
- **`connect(action=connect_nodes)`** uses Shepherd's pathfinder limits, 4 hops
  and 500 paths (DEC-7).
- **A `kp` list** is forwarded to Retriever as `parameters.kp` (DEC-10).
- **xCRG** runs the same package as upstream (DEC-25), so its known bugs
  (BioPack-team/shepherd#273, #274, #275 and #276) affect both sides alike.
- **NGD publications** are missing on the port's side until the pathfinder
  files move to tier0-20260928 (see section 1).

The comparison ignores where each side puts its query plan, the envelope `id`,
and log texts and timestamps.
