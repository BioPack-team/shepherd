from datetime import datetime
import httpx
import json
from typing import Any, Dict


def generate_query(curie: str):
    # return {
    #     "message": {
    #         "query_graph": {
    #             "edges": {
    #                 "t_edge": {
    #                     "predicates": ["biolink:treats"],
    #                     "subject": "sn",
    #                     "object": "on",
    #                     "knowledge_type": "inferred"
    #                 }
    #             },
    #             "nodes": {
    #                 "on": {
    #                     "ids": ["MONDO:0009061"],
    #                     "categories": ["biolink:Disease"]
    #                 },
    #                 "sn": {
    #                     "categories": ["biolink:ChemicalEntity"]
    #                 }
    #             }
    #         },
    #         "knowledge_graph": {
    #             "nodes": {},
    #             "edges": {},
    #         },
    #         "results": []
    #     }
    # }
    # return {
    #     "message": {
    #         "query_graph": {
    #             "nodes": {
    #                 "ON": {
    #                     "ids": [curie],
    #                     "categories": ["biolink:Disease"],
    #                     "set_interpretation": "BATCH",
    #                 },
    #                 "SN": {
    #                     "categories": ["biolink:ChemicalEntity"],
    #                     "set_interpretation": "BATCH",
    #                 }
    #             },
    #             "edges": {
    #                 "t_edge": {
    #                     "knowledge_type": "inferred",
    #                     "predicates": ["biolink:treats"],
    #                     "subject": "SN",
    #                     "object": "ON",
    #                 }
    #             }
    #         },
    #         "knowledge_graph": {
    #             "nodes": {},
    #             "edges": {}
    #         },
    #         "results": []
    #     }
    # }
    # return {
    #     "message": {
    #         "query_graph": {
    #             "edges": {
    #                 "e01": {
    #                     "object": "ConditionID",
    #                     "subject": "DrugID",
    #                     "predicates": ["biolink:treats"],
    #                     "knowledge_type": "inferred",
    #                     "attribute_constraints": [],
    #                     "qualifier_constraints": []
    #                 }
    #             },
    #             "nodes": {
    #                 "DrugID": {
    #                     "categories": ["biolink:ChemicalEntity"],
    #                 },
    #                 "ConditionID": {
    #                     "ids": [curie],
    #                     "categories": ["biolink:Disease"],
    #                 }
    #             }
    #         }
    #     },
    #     "log_level": "DEBUG",
    #     "parameters": {
    #         "override_cache": True,
    #         "kp_timeout": 10,
    #         "timeout_seconds": 300
    #     }
    # }
    return {
    "message": {
        "query_graph": {
            "nodes": {
                "n1": {"categories": ["biolink:ChemicalEntity"]},
                "n0": {"categories": ["biolink:Disease"], "ids": [curie]}
            },
            "edges": {
                "edge_0": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:treats"],
                    "knowledge_type": "inferred"
                }
            }
        },
        "knowledge_graph": {
            "nodes": {},
            "edges": {}
        },
        "results": []
    },
    # "parameters": {
    #     "overwrite_cache": True,
    # }
}


def single_lookup(curie: str, target: str):
    """Run a single query lookup synchronously."""
    query = generate_query(curie)
    start_time = datetime.now()
    try:
        with httpx.Client(timeout=600000) as client:
            response = client.post(
                f"http://localhost:5439/{target}/query",
                json=query,
            )
            response.raise_for_status()
            response = response.json()
            num_results = len((response.get("message") or {}).get("results") or [])
    except Exception as e:
        num_results = 0
        response = {
            "Error": e,
        }

    stop_time = datetime.now()
    print(f"{curie} took {stop_time - start_time} seconds and gave {num_results} results")
    with open(f"{target}_{('_').join(curie.split(':'))}_response.json", "w") as f:
        json.dump(response, f)


if __name__ == "__main__":
    single_lookup("MONDO:0011399", "example")
