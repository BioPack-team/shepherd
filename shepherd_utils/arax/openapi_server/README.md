# Vendored ARAX TRAPI models

These are the OpenAPI-generated TRAPI 1.6 model classes from ARAX
(`RTXteam/RTX`, `code/UI/OpenAPI/python-flask-server/openapi_server/`), MIT
licensed (Copyright (c) 2017-2024 Oregon State University).

- **Pinned upstream commit:** `9485431` (2026-09-21)
- **Files:** `__init__.py`, `util.py`, `typing_utils.py`, `models/`
- **Only change:** absolute imports `from openapi_server...` were rewritten to
  `from shepherd_utils.arax.openapi_server...`. Nothing else is edited.

The ARAX port (`shepherd_utils/arax/`) runs on these classes rather than on
plain dicts so that ARAX's behavior carries over unchanged: its extra in-memory
attributes (`qnode_keys`, `qedge_keys`, `filled`, `query_ids`, ...), its
`from_dict` validation, and its `to_dict` serialization. See
`docs/ARAX_PORT_BASELINE.md` (DEC-14).

It is excluded from coverage
(`.coveragerc`), and like the rest of `shepherd_utils/arax/` from Black, so it can be
re-synced from upstream with a plain copy plus the
same `sed` import rewrite:

```sh
cp -r RTX/code/UI/OpenAPI/python-flask-server/openapi_server/{__init__.py,util.py,typing_utils.py,models} \
    shepherd_utils/arax/openapi_server/
grep -rl "from openapi_server" shepherd_utils/arax/openapi_server \
    | xargs sed -i 's/^from openapi_server\b/from shepherd_utils.arax.openapi_server/'
```
