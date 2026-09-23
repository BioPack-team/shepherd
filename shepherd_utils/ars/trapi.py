"""TRAPI 2.0 response validation for the ARS, on translator_tom (TOM) models.

Upstream Relay validated each ARA response with reasoner-pydantic 5.1.1
(TRAPI 1.5), and this module used to carry a pydantic-2 mirror of those
models. Shepherd now speaks TRAPI 2.0 end to end, so the verdict comes from
TOM's own 2.0 ``Response`` model -- the same object model the rest of the
Translator (and ``shepherd_utils.trapi``) uses -- rather than a hand-kept copy.

What the verdict enforces (TOM's pydantic models, i.e. TRAPI 2.0 structure):
  - required members: ``Response.message``; ``Edge.subject/object/predicate/
    sources/knowledge_level/agent_type``; ``Node.categories``;
    ``Result.node_bindings``; ``Analysis.resource_id``;
    ``AuxiliaryGraph.edges``; ``LogEntry.timestamp`` (RFC 3339, zoned) and
    ``message``; binding ``ids``.
  - ``minItems`` 1 on ``ids``, ``Node.categories``, ``Edge.sources``,
    ``Edge.qualifiers``, ``Result.analyses``, ``Analysis.edge_bindings`` /
    ``path_bindings`` / ``support_graphs``, ``Message.auxiliary_graphs``,
    ``Response.logs``, ``RetrievalSource.upstream_resource_ids`` ...
  - ``additionalProperties: false`` on Message, Node, Edge, Attribute and
    Qualifier; everything else (Result, Analysis, bindings, ...) allows
    extras, which is what lets the ARS hang ``normalized_score`` and
    ``ordering_components`` on a Result.
  - ``RetrievalSource.resource_role`` and ``LogEntry.level`` enums.
  - the two ``anyOf`` rules the pydantic models cannot express (TOM checks
    them only in its much heavier semantic validation, which also checks
    referential integrity -- more than upstream's verdict ever did): an
    Analysis binds ``edge_bindings`` and/or ``path_bindings``, and a query
    graph has ``edges`` and/or ``paths``. These replace 1.5's separate
    Analysis / PathfinderAnalysis and QueryGraph / PathfinderQueryGraph.

Explicit nulls: TRAPI 2.0 has no ``nullable`` anywhere, but TOM's models
type optional members as ``X | None`` and so accept an explicit ``null``.
The ARS treats a null exactly as an absent member: ``strip_nulls`` removes
them before validation (premerge runs it on every ARA response, validated
or not, so nothing downstream -- and nothing the ARS emits -- carries a
null). A null in a REQUIRED member therefore fails validation as a missing
member, and a null attribute ``value`` (required, never null in 2.0) does
too. Forbidden EMPTY containers are not repaired: they fail the verdict.
"""

import logging
from typing import Any

from pydantic import ValidationError
from translator_tom import Response

logger = logging.getLogger(__name__)

__all__ = ["strip_nulls", "validate"]

# Members whose content is free-form JSON (Attribute.value, a constraint's
# value): a null nested inside them is data, not an absent TRAPI member.
_OPAQUE_MEMBERS = frozenset({"value"})


def strip_nulls(obj: Any) -> Any:
    """Remove every null-valued object member, in place, recursively.

    Does not descend into free-form ``value`` members; a ``"value": null``
    member itself is still removed (an attribute value may not be null).
    Iterative so a deeply nested response cannot hit the recursion limit.
    Returns ``obj`` for convenience.

    >>> strip_nulls({"a": None, "b": [{"c": None, "value": {"d": None}}]})
    {'b': [{'value': {'d': None}}]}
    """
    stack = [obj]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            nulls = [k for k, v in node.items() if v is None]
            for key in nulls:
                del node[key]
            for key, value in node.items():
                if key in _OPAQUE_MEMBERS:
                    continue
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            for value in node:
                if isinstance(value, (dict, list)):
                    stack.append(value)
    return obj


class _AnyOfError(ValueError):
    pass


def _check_any_of(message: dict) -> None:
    """The schema's anyOf rules, on a dict the models already accepted."""
    query_graph = message.get("query_graph")
    if query_graph is not None and not (
        query_graph.get("edges") or query_graph.get("paths")
    ):
        raise _AnyOfError("query_graph has neither edges nor paths")
    for i, result in enumerate(message.get("results") or []):
        for j, analysis in enumerate(result.get("analyses") or []):
            if not (analysis.get("edge_bindings") or analysis.get("path_bindings")):
                raise _AnyOfError(
                    f"results[{i}].analyses[{j}] has neither edge_bindings "
                    "nor path_bindings"
                )


def validate(response: Any) -> bool:
    """utils.validate: True when the response is a TRAPI 2.0 Response.

    Pure: ``response`` is not modified. Callers that want nulls read as
    absent run ``strip_nulls`` first (see the module docstring).
    """
    try:
        Response.model_validate(response)
        _check_any_of(response["message"])
        return True
    except _AnyOfError as e:
        logger.debug(f"Validation problem found {e}")
        return False
    except ValidationError as e:
        logger.debug(f"Validation problem found {e}")
        return False
    except Exception as e:
        logger.debug(f"error: {e}")
        return False
