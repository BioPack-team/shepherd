# xCRG (vendored)

The xCRG package (MVP2 inferred chemical→gene activity/abundance queries),
vendored from [Translator-CATRAX/xCRG](https://github.com/Translator-CATRAX/xCRG)
at commit `e67f2c082d53edf186e1feb609d563cea340eee0` (the commit RTX master
pins) and converted to TRAPI 2.0 (DEC-21 in `docs/ARAX_PORT_BASELINE.md`).
ARAX's `connect(action=xcrg)` runs it (`ARAX_connect.py`), and the query
graph interpreter uses its `is_xcrg_mvp2_query`.

The upstream package is TRAPI 1.x (`translator_tom` 1.2.1); this copy uses
Shepherd's `translator_tom` 2.2 models. Each file's header lists its changes
from upstream; in short:

- **TRAPI 2.0:** qualifier sets are `constraints.qualifiers` dicts; bindings
  are one `{ids}` object per qnode/qedge; edges xCRG makes carry
  `knowledge_level`/`agent_type` members; auxiliary graphs have no attributes;
  the ranker reads KL/AT from the edge members.
- **Shepherd:** lookups send `parameters.timeout`/`tiers`; a query dict is
  read for its query graph, parameters and submitter only; the NGD and PMID
  readers accept Shepherd's v1.0 artifact formats too; KL/AT values without a
  ranking weight count as unknown; NaN values survive into the returned dict;
  a direct lookup filters on its own qedge.

Like the rest of the ARAX port it is excluded from Black, to stay diffable
against upstream. Tests: `tests/unit/arax/xcrg_suite/` (upstream's unit
tests, adapted), `tests/unit/arax/test_xcrg.py` (Shepherd's changes) and the
`mvp2_xcrg_route` query parity case.
