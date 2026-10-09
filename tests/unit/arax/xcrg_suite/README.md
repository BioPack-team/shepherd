# Upstream xCRG's unit tests, run against the vendored package

The `test_*.py` files here are xCRG's own unit tests
(`Translator-CATRAX/xCRG @ e67f2c0`, `tests/unit/`), run against the copy of
xCRG vendored at `shepherd_utils/arax/xcrg/`. `utilities.py` and `conftest.py`
are the parts of upstream's `tests/utilities.py` and `tests/conftest.py` that
these tests use (the DB-file builders and the `config` fixture, with
upstream's command-line options replaced by their defaults).

**They are no longer verbatim.** Upstream xCRG speaks TRAPI 1.x
(`translator_tom` 1.2) and the vendored package speaks TRAPI 2.0, so where a
test builds or checks a TRAPI 1.x shape it is adapted to the 2.0 one, and
nothing else: one `NodeBinding(ids=[...])` / `EdgeBinding(ids=[...])` per
qnode / qedge instead of lists of `id` bindings; a QEdge's `constraints`
object instead of `qualifier_constraints` (a qualifier set is a
`{type: value}` dict); `knowledge_level` / `agent_type` as required edge
members instead of attributes; no `attributes` on aux graphs; no empty lists
or dicts where 2.0 requires at least one item. Each change is local, keeps
upstream's structure and style, and is marked with a `# TRAPI 2.0:` comment,
so `diff` against upstream's files still shows exactly what changed. The only
other change is the import paths (`xcrg.` -> `shepherd_utils.arax.xcrg.`,
`tests.utilities` -> `tests.unit.arax.xcrg_suite.utilities`), which are not
marked.

Upstream's `tests/arax/` (ARAX compliance) and `tests/integration/` suites
are not vendored: they query a live Retriever and need the real NGD and
curie_to_pmids data files.
