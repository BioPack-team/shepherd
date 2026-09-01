"""TRAPI response validation with Shepherd-native pydantic v2 models.

Replaces upstream's reasoner-pydantic 5.1.1 (pydantic 1.x, incompatible with
Shepherd's pydantic 2). The models below mirror reasoner-pydantic's field
requirements exactly -- required/optional flags and extra-field policies were
dumped from the installed upstream package and verdict parity over a corpus
of valid + deliberately broken responses is enforced by
tests/unit/ars/test_golden_parity.py::test_validate_verdict_parity.

Notable upstream semantics reproduced here:
  - Message/Node/Edge/Attribute forbid unknown fields; most others allow.
  - KG nodes require categories AND attributes; edges require subject,
    object, predicate, sources AND attributes.
  - RetrievalSource.resource_role is a closed enum of the three roles.
  - Result.analyses is a homogeneous union: all edge-bound Analysis or all
    path-bound PathfinderAnalysis.
"""

import logging
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, ValidationError

logger = logging.getLogger(__name__)


class _Allow(BaseModel):
    model_config = ConfigDict(extra="allow")


class _Forbid(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ResourceRoleEnum(str, Enum):
    aggregator_knowledge_source = "aggregator_knowledge_source"
    primary_knowledge_source = "primary_knowledge_source"
    supporting_data_source = "supporting_data_source"


class Attribute(_Forbid):
    attribute_type_id: str
    value: Any
    value_type_id: Optional[str] = None
    original_attribute_name: Optional[str] = None
    value_url: Optional[str] = None
    attribute_source: Optional[str] = None
    description: Optional[str] = None
    attributes: Optional[List["Attribute"]] = None


class Qualifier(_Allow):
    qualifier_type_id: str
    qualifier_value: str


class RetrievalSource(BaseModel):
    model_config = ConfigDict(extra="ignore")

    resource_id: str
    resource_role: ResourceRoleEnum
    upstream_resource_ids: Optional[List[str]] = None
    source_record_urls: Optional[List[str]] = None


class Node(_Forbid):
    categories: List[str]
    name: Optional[str] = None
    attributes: List[Attribute]
    is_set: Optional[bool] = None


class Edge(_Forbid):
    subject: str
    object: str
    predicate: str
    sources: List[RetrievalSource]
    qualifiers: Optional[List[Qualifier]] = None
    attributes: List[Attribute]


class KnowledgeGraph(_Allow):
    nodes: Dict[str, Node]
    edges: Dict[str, Edge]


class QNode(_Allow):
    ids: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    set_interpretation: Optional[str] = "BATCH"
    constraints: Optional[List[Dict[str, Any]]] = None
    member_ids: Optional[List[str]] = None


class QEdge(_Allow):
    subject: str
    object: str
    knowledge_type: Optional[str] = None
    predicates: Optional[List[str]] = None
    attribute_constraints: Optional[List[Dict[str, Any]]] = None
    qualifier_constraints: Optional[List[Dict[str, Any]]] = None


class QueryGraph(_Allow):
    nodes: Dict[str, QNode]
    edges: Dict[str, QEdge]


class NodeBinding(_Allow):
    id: str
    query_id: Optional[str] = None
    attributes: List[Attribute]


class EdgeBinding(_Allow):
    id: str
    attributes: List[Attribute]


class PathBinding(_Allow):
    id: str


class Analysis(_Allow):
    resource_id: str
    score: Optional[float] = None
    support_graphs: Optional[List[str]] = None
    scoring_method: Optional[str] = None
    attributes: Optional[List[Attribute]] = None
    edge_bindings: Dict[str, List[EdgeBinding]]


class PathfinderAnalysis(_Allow):
    resource_id: str
    score: Optional[float] = None
    support_graphs: Optional[List[str]] = None
    scoring_method: Optional[str] = None
    attributes: Optional[List[Attribute]] = None
    path_bindings: Dict[str, List[PathBinding]]


class Result(_Allow):
    node_bindings: Dict[str, List[NodeBinding]]
    analyses: Union[List[Analysis], List[PathfinderAnalysis]]


class AuxiliaryGraph(_Allow):
    edges: List[str]
    attributes: List[Attribute]


class Message(_Forbid):
    query_graph: Optional[QueryGraph] = None
    knowledge_graph: Optional[KnowledgeGraph] = None
    results: Optional[List[Result]] = None
    auxiliary_graphs: Optional[Dict[str, AuxiliaryGraph]] = None


class LogEntry(_Allow):
    timestamp: Optional[str] = None
    level: Optional[str] = None
    code: Optional[str] = None
    message: Optional[str] = None


class Response(_Allow):
    message: Message
    logs: List[LogEntry] = []
    status: Optional[str] = None
    workflow: Optional[Any] = None


def validate(response: dict) -> bool:
    """utils.validate: True when the response parses as a TRAPI Response."""
    try:
        Response.model_validate(response)
        return True
    except ValidationError as e:
        logger.debug(f"Validation problem found {e}")
        return False
    except Exception as e:
        logger.debug(f"error: {e}")
        return False
