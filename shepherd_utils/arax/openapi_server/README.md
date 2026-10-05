# Vendored ARAX TRAPI models (TRAPI 2.0)

These started as the OpenAPI-generated TRAPI 1.6 model classes from ARAX
(`RTXteam/RTX`, `code/UI/OpenAPI/python-flask-server/openapi_server/`), MIT
licensed (Copyright (c) 2017-2024 Oregon State University), pinned at `9485431`
(2026-09-21), with absolute imports `from openapi_server...` rewritten to
`from shepherd_utils.arax.openapi_server...`.

They are now in the **TRAPI 2.0** shape (DEC-20 in
`docs/ARAX_PORT_BASELINE.md`). Changes from upstream:

- Regenerated, in the same generated style, by `scripts/arax_trapi2_models.py`:
  `NodeBinding`, `EdgeBinding`, `PathBinding` (one per qnode/qedge/qpath, with
  `ids`), `AuxiliaryGraph` (`edges` only), `Edge` (required `knowledge_level`,
  `agent_type`), `QEdge` (`constraints`), new `QEdgeConstraints`,
  `AllowDenyConstraint`, `SourceConstraint`, `PathConstraint`
  (`required_intermediate_categories`), `Analysis` (one model), `Result`,
  `QueryGraph` (one model), `Message`, `Query` / `AsyncQuery` / `Response`
  (`parameters`), `MetaEdge` (`knowledge_levels`, `agent_types`, `sources`).
  ARAX's own extensions on these models are kept.
- Removed: `BaseAnalysis`, `PathfinderAnalysis`, `BaseQueryGraph`,
  `PathfinderQueryGraph`, their `*AllOf` / `OneOf*` unions, and
  `QualifierConstraint` (a qualifier set is a plain dict in 2.0).
- Hand edits: `base_model_.Model.to_dict` omits unset (None) members, since
  2.0 has no nulls; `QNode.set_interpretation` accepts `COLLATE`, which
  `QNode.is_set` reports as `True`; `QNode.member_ids` added.

The ARAX port (`shepherd_utils/arax/`) runs on these classes rather than on
plain dicts so that ARAX's behavior carries over unchanged: its extra in-memory
attributes (`qnode_keys`, `qedge_keys`, `filled`, `query_ids`, ...), its
`from_dict` validation, and its `to_dict` serialization. See
`docs/ARAX_PORT_BASELINE.md` (DEC-14).

It is excluded from coverage (`.coveragerc`), and like the rest of
`shepherd_utils/arax/` from Black. Re-syncing from upstream is no longer a plain
copy: copy upstream's files and rewrite the imports as before, then re-run
`scripts/arax_trapi2_models.py`, delete the removed models, and re-apply the
hand edits listed above.

```sh
cp -r RTX/code/UI/OpenAPI/python-flask-server/openapi_server/{__init__.py,util.py,typing_utils.py,models} \
    shepherd_utils/arax/openapi_server/
grep -rl "from openapi_server" shepherd_utils/arax/openapi_server \
    | xargs sed -i 's/^from openapi_server\b/from shepherd_utils.arax.openapi_server/'
python scripts/arax_trapi2_models.py shepherd_utils/arax/openapi_server/models
```
