# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/utilities.py (DEC-21).
# Changes from upstream:
#   - TRAPI 2.0: XCRGResult holds one NodeBinding per qnode and direct edge ids; a result without analyses has none (not [])
# See README.md.

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from io import TextIOWrapper
from pathlib import Path
from typing import (
    Callable,
    TypeVar
)

from translator_tom import (
    Analysis,
    EdgeID,
    NodeBinding,
    QNodeID,
    Result,
    TOMBase
)


MISSING_SORT_VALUE = float("inf")

T = TypeVar("T")


class OrderedEnum(Enum):
    """Base class for Enums with an ordinal field that represents declaration order."""
    ordinal: int

    def __new__(cls, value):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.ordinal = len(cls.__members__)
        return obj

    # __eq__ already taken care of by Enum class

    def __lt__(self, other: OrderedEnum) -> bool:
        if not isinstance(other, OrderedEnum):
            return NotImplemented
        return self.ordinal < other.ordinal

    def __le__(self, other: OrderedEnum) -> bool:
        if not isinstance(other, OrderedEnum):
            return NotImplemented
        return self.ordinal <= other.ordinal

    def __gt__(self, other: OrderedEnum) -> bool:
        if not isinstance(other, OrderedEnum):
            return NotImplemented
        return self.ordinal > other.ordinal

    def __ge__(self, other: OrderedEnum) -> bool:
        if not isinstance(other, OrderedEnum):
            return NotImplemented
        return self.ordinal >= other.ordinal


# TODO: Temporary until types are untangled
#  This class is effectively a TRAPI Result + additional custom properties
#  The original code would push + pop these xcrg properties
@dataclass
class XCRGResult:
    # TRAPI Result properties
    node_bindings : dict[QNodeID, NodeBinding] = field(default_factory = dict)
    analyses      : list[Analysis]             = field(default_factory = list)
    # Custom xCRG properties
    xcrg_direct_bindings    : list[EdgeID]      = field(default_factory = list)
    xcrg_direct_binding_ids : set[EdgeID]       = field(default_factory = set)
  # xcrg_support_edges      : list[EdgeBinding] = field(default_factory = list)
    xcrg_support_edge_ids   : set[EdgeID]       = field(default_factory = set)
    xcrg_score              : float             = field(default = float("inf"))
    ngd_score               : float | None      = field(default = None)

    def to_trapi_result(self):
        return Result(node_bindings = self.node_bindings, analyses = self.analyses or None)


class XcrgJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle special cases with serializing classes."""
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        elif isinstance(o, Path):
            return str(o)
        elif isinstance(o, TOMBase):
            return o.to_dict()
        elif is_dataclass(o) and not isinstance(o, type):
            return asdict(o)
        else:
            return super().default(o)


def path_or_none(path: str | Path | None) -> Path | None:
    if isinstance(path, Path):
        return path
    elif isinstance(path, str):
        return Path(path)
    else:
        return None


def partition(items: list[T], predicate: Callable[[T], bool]) -> tuple[list[T], list[T]]:
    """Return a list (first) with items that pass predicate, and a list (second) that fail."""
    passed = list[T]()
    failed = list[T]()

    for item in items:
        if predicate(item):
            passed.append(item)
        else:
            failed.append(item)

    return passed, failed


# TODO: Reintroduce when we move to Python 3.11 in October
# def throw(exception: Exception) -> Never:
#     raise exception
#

def as_type(value: object, cls: type[T]) -> T | None:
    """Return value if it matches type T. Return None otherwise."""
    if isinstance(value, cls):
        return value
    else:
        return None


def chunk_values(values: list[str], chunk_size: int) -> list[list[str]]:
    """Split values into non-empty batches."""
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive.")
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def make_stable_id(prefix: str, payload: object) -> str:
    """Return a deterministic compact id for generated KG/support entries."""
    key = json.dumps(payload, cls=XcrgJsonEncoder, sort_keys=True)
    suffix = uuid.uuid5(uuid.NAMESPACE_URL, key).hex[:16]
    return f"{prefix}_{suffix}"


def desc_optional(value: float | int | None) -> float:
    """Convert optional descending values into ascending sort components."""
    return -float(value) if value is not None else MISSING_SORT_VALUE


def asc_optional(value: float | int | None) -> float:
    """Convert optional ascending values into sort components."""
    return float(value) if value is not None else MISSING_SORT_VALUE


def format_json_for_log(value: object | TOMBase) -> str:
    """Return compact JSON for diagnostic logs."""
    if isinstance(value, TOMBase):
        data = value.to_dict()
    else:
        data = value
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def serialize_json_to_file(obj: object, file: TextIOWrapper):
    json.dump(obj, file, cls = XcrgJsonEncoder, indent = 4)
