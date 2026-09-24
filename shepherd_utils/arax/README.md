# ARAX port library

A module-for-module port of the ARAX reasoner (`RTXteam/RTX`, pinned to commit
`9485431`), run in-process by Shepherd's ARAX worker (DEC-14 in
`docs/ARAX_PORT_BASELINE.md`).

Rules for this directory:

- **Upstream layout and names.** Each ported file keeps its ARAX filename
  (`ARAX_resultify.py`, `Expand/trapi_querier.py`, ...) and its code, so it can
  be diffed against the pinned upstream commit.
- **Header on every ported file.** It names the upstream path and lists every
  change from upstream. "Import paths only" means the code is otherwise
  identical.
- **Parity first (DEC-1).** Upstream bugs and quirks are kept. Changes are only
  the ones a recorded decision requires (for example, DEC-4 Retriever-only,
  DEC-3 no KP cache), and each is listed in the file header.
- **Not reformatted.** The directory is excluded from Black (`pyproject.toml`)
  to keep the upstream diff small. Shepherd-specific glue lives in
  `workers/arax/`, which is formatted as usual.
- **TRAPI models** are ARAX's own generated classes, vendored in
  `openapi_server/` (see its README).
