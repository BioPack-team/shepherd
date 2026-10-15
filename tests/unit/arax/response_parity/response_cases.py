"""/response/{id} parity cases (API-07).

Each case is (name, steps, data). A step is a response id to look up, or
"$Z" -- the detail_lookup id of the first node of the previous result (the X
prefix's component cache). data holds what the lookups can reach: "local"
responses (by the port's id, with the integer id upstream uses), "urls" and
"ars" messages ((pk, trace) -> (status, body)).
"""

import gzip
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
PK = "0b6a7f2c-7c3e-4ab0-9a55-1d2c3e4f5a6b"
CHILD = "5e1d9a70-2f1b-4c8e-8b1a-6f7e8d9c0b1a"
PK_404 = "9d8c7b6a-5f4e-4d3c-8b2a-1f0e9d8c7b6a"


def _arax_envelope():
    """A real ARAX response: one from the query parity goldens."""
    path = os.path.join(HERE, "..", "query_parity", "goldens.json.gz")
    with gzip.open(path, "rt") as f:
        env = json.load(f)["tpl_one_hop_classic"]["envelope"]
    env["logs"] = [
        {
            "timestamp": "2026-01-01T00:00:00",
            "level": "INFO",
            "code": None,
            "message": "m",
        }
    ]
    return env


def _kg_with_support_graph():
    return {
        "schema_version": "1.6.0",
        "biolink_version": "4.2.5",
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["CHEBI:1"]},
                    "n1": {"categories": ["biolink:Disease"]},
                },
                "edges": {
                    "e0": {
                        "subject": "n0",
                        "object": "n1",
                        "predicates": ["biolink:treats"],
                    }
                },
            },
            "knowledge_graph": {
                "nodes": {
                    "CHEBI:1": {
                        "name": "one",
                        "categories": ["biolink:SmallMolecule"],
                        "attributes": [
                            {"attribute_type_id": "biolink:description", "value": "d"}
                        ],
                    },
                    "MONDO:1": {
                        "name": "two",
                        "categories": ["biolink:Disease"],
                        "attributes": [],
                    },
                },
                "edges": {
                    "k0": {
                        "subject": "CHEBI:1",
                        "object": "MONDO:1",
                        "predicate": "biolink:treats",
                        "sources": [
                            {
                                "resource_id": "infores:ctd",
                                "resource_role": "primary_knowledge_source",
                            }
                        ],
                        "attributes": [
                            {
                                "attribute_type_id": "biolink:support_graphs",
                                "value": ["ag1"],
                            },
                            {
                                "attribute_type_id": "biolink:knowledge_level",
                                "value": "knowledge_assertion",
                            },
                        ],
                    },
                    "k1": {
                        "subject": "CHEBI:1",
                        "object": "MONDO:1",
                        "predicate": "biolink:related_to",
                        "attributes": [
                            {
                                "attribute_type_id": "primary_knowledge_source",
                                "value": "infores:semmeddb",
                            }
                        ],
                    },
                    "k2": {
                        "subject": "MONDO:1",
                        "object": "CHEBI:1",
                        "predicate": "biolink:treated_by",
                    },
                },
            },
            "auxiliary_graphs": {"ag1": {"edges": ["k1"], "attributes": []}},
            "results": [
                {
                    "node_bindings": {
                        "n0": [{"id": "CHEBI:1", "attributes": []}],
                        "n1": [{"id": "MONDO:1", "attributes": []}],
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:arax",
                            "edge_bindings": {"e0": [{"id": "k0", "attributes": []}]},
                        }
                    ],
                },
                {
                    "resource_id": "infores:other",
                    "node_bindings": {
                        "n0": [{"id": "CHEBI:1", "attributes": []}],
                        "n1": [{"id": "MONDO:1", "attributes": []}],
                    },
                    "analyses": [],
                },
            ],
        },
    }


def _ars_message(pk, name, data, **fields):
    return {
        "model": "tr_ars.message",
        "pk": pk,
        "fields": dict(
            {"name": name, "status": "Done", "code": 200, "data": data}, **fields
        ),
    }


def _dump(obj):
    return json.dumps(obj).encode()


TRACE = {
    "message": PK,
    "status": "Done",
    "children": [{"message": CHILD, "actor": {"agent": "ara-arax"}}],
}

CASES = [
    (
        "local_arax_response",
        ["LOCAL1"],
        {"local": {"LOCAL1": (41651, _arax_envelope())}},
    ),
    (
        "local_kg_response",
        ["LOCAL2"],
        {"local": {"LOCAL2": (41652, _kg_with_support_graph())}},
    ),
    (
        "local_trapi_1_5",
        ["LOCAL3"],
        {
            "local": {
                "LOCAL3": (
                    41653,
                    dict(_kg_with_support_graph(), schema_version="1.5.0"),
                )
            }
        },
    ),
    (
        "local_no_results",
        ["LOCAL4"],
        {
            "local": {
                "LOCAL4": (
                    41654,
                    {
                        "message": {
                            "query_graph": {"nodes": {}, "edges": {}},
                            "results": [],
                        }
                    },
                )
            }
        },
    ),
    (
        "local_no_query_graph",
        ["LOCAL5"],
        {"local": {"LOCAL5": (41655, {"message": {}, "description": None})}},
    ),
    (
        "local_validator_crash",
        ["LOCAL6"],
        {"local": {"LOCAL6": (41656, dict(_arax_envelope(), description="boom"))}},
    ),
    ("local_missing", ["LOCAL9"], {"local": {}}),
    (
        "url_ok",
        ["https:$$example.org$resp.json"],
        {
            "urls": {
                "https://example.org/resp.json": (200, _dump(_kg_with_support_graph()))
            }
        },
    ),
    (
        "url_dollar",
        ["$$example.org$resp.json"],
        {"urls": {"https://example.org/resp.json": (200, _dump(_arax_envelope()))}},
    ),
    (
        "url_boom",
        ["https:$$example.org$boom.json"],
        {
            "urls": {
                "https://example.org/boom.json": (
                    200,
                    _dump(dict(_kg_with_support_graph(), description="boom")),
                )
            }
        },
    ),
    (
        "url_error",
        ["https:$$example.org$gone.json"],
        {"urls": {"https://example.org/gone.json": (500, b"no")}},
    ),
    (
        "url_not_json",
        ["https:$$example.org$x.json"],
        {"urls": {"https://example.org/x.json": (200, b"<html>")}},
    ),
    (
        "ars_parent",
        [PK],
        {
            "ars": {
                (PK, False): (
                    200,
                    _dump(_ars_message(PK, "ars-default-agent", {"message": {}})),
                ),
                (PK, True): (200, _dump(TRACE)),
            }
        },
    ),
    (
        "ars_parent_by_actor",
        [PK],
        {
            "ars": {
                (PK, False): (200, _dump(_ars_message(PK, "", None, actor=9))),
                (PK, True): (200, _dump(TRACE)),
            }
        },
    ),
    (
        "ars_child",
        [CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(_ars_message(CHILD, "ara-arax", _kg_with_support_graph())),
                )
            }
        },
    ),
    (
        "ars_child_by_actor",
        [CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(_ars_message(CHILD, "", _kg_with_support_graph(), actor=3)),
                )
            }
        },
    ),
    # a finished child response is cached under its id: the second lookup is served from it
    (
        "ars_child_twice",
        [CHILD, CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(_ars_message(CHILD, "ara-aragorn", _kg_with_support_graph())),
                )
            }
        },
    ),
    (
        "ars_child_x_then_z_then_cached",
        ["X" + CHILD, "$Z", "X" + CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(_ars_message(CHILD, "ara-bte", _kg_with_support_graph())),
                )
            }
        },
    ),
    (
        "ars_child_validator_crash",
        [CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(
                        _ars_message(
                            CHILD,
                            "ara-arax",
                            dict(
                                _kg_with_support_graph(), logs=None, description="boom"
                            ),
                        )
                    ),
                )
            }
        },
    ),
    # Shepherd's ARS agent name: labelled only by the port (see test_response_parity.py)
    (
        "ars_child_shepherd_agent",
        [CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(
                        _ars_message(
                            CHILD, "ara-shepherd-arax", _kg_with_support_graph()
                        )
                    ),
                )
            }
        },
    ),
    (
        "ars_child_no_data",
        [CHILD],
        {"ars": {(CHILD, False): (200, _dump(_ars_message(CHILD, "ara-arax", None)))}},
    ),
    (
        "ars_child_not_trapi",
        [CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(
                        _ars_message(
                            CHILD, "ara-arax", {"status": "Error", "detail": "boom"}
                        )
                    ),
                )
            }
        },
    ),
    (
        "ars_child_logs_are_strings",
        [CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(
                        _ars_message(
                            CHILD,
                            "ara-arax",
                            {
                                "message": {},
                                "logs": [
                                    json.dumps(_kg_with_support_graph()),
                                    "second",
                                ],
                            },
                        )
                    ),
                )
            }
        },
    ),
    (
        "ars_child_data_is_a_list",
        [CHILD],
        {
            "ars": {
                (CHILD, False): (
                    200,
                    _dump(_ars_message(CHILD, "ara-arax", ["a", "b"])),
                )
            }
        },
    ),
    ("ars_no_fields", [CHILD], {"ars": {(CHILD, False): (200, _dump({"pk": CHILD}))}}),
    ("ars_not_found", [PK_404], {"ars": {}}),
    ("ars_not_json", [CHILD], {"ars": {(CHILD, False): (200, b"<html>")}}),
    ("z_missing", ["Z" + PK_404], {}),
]
