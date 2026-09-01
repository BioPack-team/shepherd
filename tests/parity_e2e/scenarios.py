"""Scenario corpus for differential parity runs.

Each scenario arms the mockworld and defines the query submitted to BOTH
stacks. ARA responses come from the layer-1 corpus so the merge inputs are
the same TRAPI the golden tests already pin.
"""

import json
import pathlib

CORPUS = pathlib.Path(__file__).resolve().parents[1] / "fixtures/ars_corpus"


def corpus(name):
    return json.loads((CORPUS / name).read_text())


STANDARD_QUERY = {
    "message": {
        "query_graph": corpus("response_aragorn.json")["message"]["query_graph"]
    }
}

PATHFINDER_QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"n0": {"ids": ["MONDO:0005148"]}, "n1": {"ids": ["CHEBI:6801"]}},
            "edges": {},
            "paths": {"p0": {"subject": "n0", "object": "n1"}},
        }
    }
}


def happy(infores, response_name, delay=0.2):
    return {
        "mode": "happy",
        "delay_sec": delay,
        "response": corpus(response_name),
    }


def _broken():
    """Survives pre-merge processing but fails TRAPI validation."""
    broken = corpus("response_aragorn.json")
    del broken["message"]["results"][0]["node_bindings"]
    return broken


SCENARIOS = {
    "single_ara_happy": {
        "query": STANDARD_QUERY,
        "aras": {"infores:aragorn": happy("infores:aragorn", "response_aragorn.json")},
    },
    "two_aras_merge": {
        "query": STANDARD_QUERY,
        "aras": {
            "infores:aragorn": happy("infores:aragorn", "response_aragorn.json"),
            "infores:arax": happy("infores:arax", "response_arax.json", delay=0.6),
        },
    },
    "two_aras_reversed_order": {
        "query": STANDARD_QUERY,
        "aras": {
            "infores:aragorn": happy(
                "infores:aragorn", "response_aragorn.json", delay=0.8
            ),
            "infores:arax": happy("infores:arax", "response_arax.json", delay=0.1),
        },
    },
    "all_empty": {
        "query": STANDARD_QUERY,
        "aras": {
            "infores:aragorn": {"mode": "empty", "delay_sec": 0.1},
            "infores:arax": {"mode": "empty", "delay_sec": 0.1},
        },
    },
    "one_errors_one_succeeds": {
        "query": STANDARD_QUERY,
        "aras": {
            "infores:aragorn": happy("infores:aragorn", "response_aragorn.json"),
            "infores:arax": {"mode": "error"},
        },
    },
    "unavailable_503": {
        "query": STANDARD_QUERY,
        "aras": {
            "infores:aragorn": happy("infores:aragorn", "response_aragorn.json"),
            "infores:arax": {"mode": "unavailable"},
        },
    },
    "garbage_response": {
        "query": STANDARD_QUERY,
        "aras": {
            "infores:aragorn": happy("infores:aragorn", "response_aragorn.json"),
            "infores:arax": {"mode": "garbage"},
        },
    },
    "silent_ara_times_out": {
        # 5-minute wait: run with --include-slow
        "query": STANDARD_QUERY,
        "slow": True,
        "aras": {
            "infores:aragorn": happy("infores:aragorn", "response_aragorn.json"),
            "infores:arax": {"mode": "silent"},
        },
    },
    "blocklist_removal": {
        "query": STANDARD_QUERY,
        "aras": {
            "infores:aragorn": happy("infores:aragorn", "response_blocklist.json"),
        },
    },
    "appraiser_down": {
        "query": STANDARD_QUERY,
        "appraiser": {"mode": "error"},
        "aras": {"infores:aragorn": happy("infores:aragorn", "response_aragorn.json")},
    },
    "annotator_down": {
        "query": STANDARD_QUERY,
        "annotator": {"mode": "error"},
        "aras": {"infores:aragorn": happy("infores:aragorn", "response_aragorn.json")},
    },
    "pathfinder_shape": {
        "query": PATHFINDER_QUERY,
        "aras": {
            "infores:aragorn": {"mode": "empty", "delay_sec": 0.1},
        },
    },
    "validation_failure": {
        "query": STANDARD_QUERY,
        "aras": {
            "infores:aragorn": {
                "mode": "happy",
                "delay_sec": 0.1,
                "response": _broken(),
            },
        },
    },
}
