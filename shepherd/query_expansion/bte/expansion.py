"""BTE expansion submodule"""

import json
from pathlib import Path
from typing import Any, Optional, cast
from pydantic import BaseModel, parse_obj_as
from reasoner_pydantic import (
    CURIE,
    BiolinkEntity,
    BiolinkPredicate,
    BiolinkQualifier,
    HashableSequence,
    QEdge,
    Query,
    QueryGraph,
)


class TemplateGroup(BaseModel):
    """A group of templates to be matched by given criteria."""

    name: str
    subject: list[str]
    predicate: list[str]
    object: list[str]
    templates: list[str]
    qualifiers: Optional[dict[str, str]]


def get_params(
    query_graph: QueryGraph,
) -> tuple[
    Optional[BiolinkEntity],
    Optional[CURIE],
    Optional[BiolinkEntity],
    Optional[CURIE],
    Optional[BiolinkPredicate],
    dict[BiolinkQualifier, str],
]:
    """Obtain some important parameters from the query graph."""
    edge = cast(QEdge, next(iter(query_graph.edges.values())))

    q_subject = query_graph.nodes[edge.subject]
    subject_type = next(iter(q_subject.categories or []), None)

    q_object = query_graph.nodes[edge.object]
    object_type = next(iter(q_object.categories or []), None)

    predicate = next(iter(edge.predicates or []), None)

    subject_curie = next(iter(q_subject.ids or []), None)
    object_curie = next(iter(q_object.ids or []), None)
    qualifiers: dict[BiolinkQualifier, str] = {}

    qualifier_constraints = edge.qualifier_constraints
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


def match_templates(
    subject_type: Optional[BiolinkEntity],
    object_type: Optional[BiolinkEntity],
    predicate: Optional[BiolinkPredicate],
    qualifiers: dict[BiolinkQualifier, str],
) -> list[Path]:
    """Match a given set of parameters to a number of templates."""

    # TODO: expand subject/object types by descending the biolink hierarchy
    subject_types: set[str] = set()
    object_types: set[str] = set()
    predicates: set[str] = set()
    if subject_type is not None:
        subject_types.add(subject_type.removeprefix("biolink:"))
    if object_type is not None:
        object_types.add(object_type.removeprefix("biolink:"))
    if predicate is not None:
        predicates.add(predicate.removeprefix("biolink:"))

    with open(Path(__file__).parent / "templateGroups.json", "r") as file:
        templateGroups = parse_obj_as(list[TemplateGroup], json.load(file))

    template_paths = {
        path.name: path
        for path in (Path(__file__).parent / "templates").rglob("*.json")
    }

    matched_paths: set[Path] = set()
    for group in templateGroups:
        conditions: list[bool] = []
        conditions.append(len(subject_types.intersection(group.subject)) > 0)
        conditions.append(len(object_types.intersection(group.object)) > 0)
        conditions.append(len(predicates.intersection(group.predicate)) > 0)
        conditions.append(  # Qualifiers (if they exist) are satisfied
            all(
                (group.qualifiers or {}).get(qualifier_type, False) == value
                for qualifier_type, value in qualifiers.items()
            )
        )

        if all(conditions):
            for template in group.templates:
                matched_paths.add(template_paths[template])

    return list(matched_paths)


def fill_templates(
    paths: list[Path], subject_curie: Optional[CURIE], object_curie: Optional[CURIE]
) -> list[Query]:
    filled_templates: list[Query] = []
    for path in paths:
        with open(path, "r") as file:
            template = Query.parse_obj(json.load(file))
        if subject_curie is not None:
            cast(QueryGraph, template.message.query_graph).nodes[
                "creativeQuerySubject"
            ].ids = HashableSequence(__root__=[subject_curie])
        if object_curie is not None:
            cast(QueryGraph, template.message.query_graph).nodes[
                "creativeQueryObject"
            ].ids = HashableSequence(__root__=[object_curie])
        filled_templates.append(template)

    return filled_templates


def expand_bte_query(query_body: Query) -> list[Any]:
    """Expand a given query into the appropriate templates."""
    # Contract:
    # 1. there is a single edge in the query graph
    # 2. The edge is marked inferred.
    # 3. Either the source or the target has IDs, but not both.
    # 4. The number of ids on the query node is 1.

    query_graph = query_body.message.query_graph
    if query_graph is None:
        return []
    subject_type, subject_curie, object_type, object_curie, predicate, qualifiers = (
        get_params(query_graph)
    )

    matched_template_paths = match_templates(
        subject_type, object_type, predicate, qualifiers
    )

    return fill_templates(matched_template_paths, subject_curie, object_curie)
