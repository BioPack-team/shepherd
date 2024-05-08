"""BTE expansion submodule"""

import json
from pathlib import Path
from typing import Any


def get_params(query_graph) -> tuple[str, str, str, str, str, dict[str, str]]:
    edge = next(iter(query_graph["edges"].values()))
    q_subject = query_graph["nodes"][edge["subject"]]
    subject_type = q_subject["categories"][0].removeprefix("biolink:")
    q_object = query_graph["nodes"][edge["object"]]
    object_type = q_object["categories"][0].removeprefix("biolink:")
    predicate = edge["predicates"][0].removeprefix("biolink:")
    subject_curie = q_subject.get("ids", [None])[0]
    object_curie = q_object.get("ids", [None])[0]
    qualifiers = {}

    qualifier_constraints = edge.get("qualifier_constraints")
    if qualifier_constraints is not None and len(qualifier_constraints) > 0:
        qualifiers = {
            qualifier["qualifier_type_id"]: qualifier["qualifier_value"]
            for qualifier in qualifier_constraints[0]["qualifier_set"]
        }

    return (
        subject_type,
        subject_curie,
        object_type,
        object_curie,
        predicate,
        qualifiers,
    )


def match_templates(subject_type, object_type, predicate, qualifiers) -> list[Path]:

    # TODO: expand subject/object types by descending the biolink hierarchy
    subject_types, object_types, predicates = {subject_type}, {object_type}, {predicate}

    with open(Path(__file__).parent / "templateGroups.json", "r") as file:
        templateGroups = json.load(file)

    template_paths = {
        path.name: path
        for path in (Path(__file__).parent / "templates").rglob("*.json")
    }

    # TODO: strict typing
    matched_paths = set()
    for group in templateGroups:
        conditions = []
        conditions.append(len(subject_types.intersection(group["subject"])) > 0)
        conditions.append(len(object_types.intersection(group["object"])) > 0)
        conditions.append(len(predicates.intersection(group["predicate"])) > 0)
        conditions.append(  # Qualifiers (if they exist) are satisfied
            all(
                group.get("qualifiers", {}).get(qualifier_type, False) == value
                for qualifier_type, value in qualifiers.items()
            )
        )

        if all(conditions):
            for template in group["templates"]:
                matched_paths.add(template_paths[template])

    return list(matched_paths)


def fill_templates(
    paths: list[Path], subject_curie, object_curie
) -> list[dict[str, Any]]:
    filled_templates = []
    for path in paths:
        with open(path, "r") as file:
            template = json.load(file)
        if subject_curie is not None:
            template["message"]["query_graph"]["nodes"]["creativeQuerySubject"][
                "ids"
            ] = [subject_curie]
        if object_curie is not None:
            template["message"]["query_graph"]["nodes"]["creativeQueryObject"][
                "ids"
            ] = [object_curie]
        filled_templates.append(template)

    return filled_templates


def expand_bte_query(query_body):
    """Expand a given query into the appropriate templates."""
    # Contract:
    # 1. there is a single edge in the query graph
    # 2. The edge is marked inferred.
    # 3. Either the source or the target has IDs, but not both.
    # 4. The number of ids on the query node is 1.

    query_graph = query_body["message"]["query_graph"]
    subject_type, subject_curie, object_type, object_curie, predicate, qualifiers = (
        get_params(query_graph)
    )

    matched_template_paths = match_templates(
        subject_type, object_type, predicate, qualifiers
    )

    return fill_templates(matched_template_paths, subject_curie, object_curie)
