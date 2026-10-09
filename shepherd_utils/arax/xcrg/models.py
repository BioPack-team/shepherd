# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/models.py (DEC-21).
# Changes from upstream:
#   - TRAPI 2.0: KnowledgeGraph.edges is optional (edges_dict)
# See README.md.
"""This module contains disparate classes and structures used throughout the library."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from translator_tom import (
    CURIE,
    KnowledgeGraph,
    Query,
    Response,
)


@dataclass
class Batch_Summary:
    batch_index : int
    tf_ids      : list[CURIE]
    tf_count    : int
    response    : Message_Statistics
    # "raw_response": trapi.get_message_statistics(response),


class Direction(Enum):
    INCREASED = "increased"
    DECREASED = "decreased"


Direction_Template = tuple[Direction, Direction]


# Sign-compatible two-hop templates for desired final direction
DIRECTION_TEMPLATES: dict[Direction, tuple[Direction_Template, Direction_Template]] = {
    Direction.INCREASED: (
        (Direction.INCREASED, Direction.INCREASED),
        (Direction.DECREASED, Direction.DECREASED)
    ),
    Direction.DECREASED: (
        (Direction.INCREASED, Direction.DECREASED),
        (Direction.DECREASED, Direction.INCREASED)
    )
}


@dataclass
class Message_Statistics:
    result_count : int
    node_count   : int
    edge_count   : int

    @staticmethod
    def zero():
        return Message_Statistics(0, 0, 0)

    @staticmethod
    def get_from(entity: Query | Response) -> Message_Statistics:
        """Return compact counts for a TRAPI response."""
        message = entity.message
        knowledge_graph = message.knowledge_graph or KnowledgeGraph.new()
        return Message_Statistics(
            result_count = len(message.results_list),
            node_count = len(knowledge_graph.nodes),
            edge_count = len(knowledge_graph.edges_dict),
        )


@dataclass
class Template_Summary:
    template_index   : int
    first_direction  : Direction
    second_direction : Direction
    batches          : list[Batch_Summary] = field(default_factory = list)


@dataclass
class Debug_Summary:
    query_id        : str
    debug_run_dir   : str
    final_direction : str
    tf_count        : int
    batch_size      : int
    batch_count     : int
    direct_response : Message_Statistics     = field(default_factory = Message_Statistics.zero)
    merged_response : Message_Statistics     = field(default_factory = Message_Statistics.zero)
    templates       : list[Template_Summary] = field(default_factory = list)
