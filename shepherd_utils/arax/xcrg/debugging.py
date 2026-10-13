# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/debugging.py (DEC-21).
# Changes from upstream:
#   - unchanged
# See README.md.
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from translator_tom import (
    QEdgeID,
    QNode,
    Query,
    QueryGraph,
    Response,
    TOMBase
)

from . import trapi
from .models import Message_Statistics
from .utilities import OrderedEnum, serialize_json_to_file


class DebugLevel(OrderedEnum):
    """The debug level represents how much data will be saved during xCRG runs."""
    value: str

    NONE = "none"
    """Do not save any debug data."""
    BASIC = "basic"
    """Save some debugging data, including final response."""
    ALL = "all"
    """Save all debug data."""


@dataclass(frozen = True)
class DebugContext:
    """Assorted data for debugging xCRG runner."""
    level: DebugLevel
    query_id: str
    created_at: datetime
    run_name: str
    run_dir: Path
    query_edge_id: QEdgeID
    source_qnode: str # TODO: CURIE?
    target_qnode: str # TODO: CURIE?
    source_label: str
    target_label: str
    direction: str | None
    artifacts: list[dict]

    @staticmethod
    def new(
        debug_dir: Path,
        debug_level: DebugLevel | str | None,
        run_name: str | None,
        query_id: str,
        query: Query
    ) -> DebugContext:
        """Create human-readable debug path metadata for one xCRG query."""
        debug_dir.mkdir(exist_ok = True)

        level: DebugLevel
        match debug_level:
            case DebugLevel(): level = debug_level
            case str():        level = DebugLevel(debug_level)
            case None:         level = DebugLevel.NONE

        created_at = datetime.now(timezone.utc)
        qnodes = cast(QueryGraph, query.message.query_graph).nodes
        edge_id, edge = trapi.get_single_query_edge(query.message.query_graph)
        direction = trapi.get_qualifier_value(edge, "biolink:object_direction_qualifier")
        source_label = describe_qnode_for_debug(qnodes.get(edge.subject))
        target_label = describe_qnode_for_debug(qnodes.get(edge.object))
        direction_label = safe_debug_token(direction)

        run_name = run_name or (
            f"{created_at.strftime('%Y%m%d_%H%M%S')}_{query_id}_"
            f"{source_label}_to_{target_label}_{direction_label}"
        )

        return DebugContext(
            level = level,
            query_id = query_id,
            created_at = created_at,
            run_name = run_name,
            run_dir = debug_dir / run_name,
            query_edge_id = edge_id,
            source_qnode = edge.subject,
            target_qnode = edge.object,
            source_label = source_label,
            target_label = target_label,
            direction = direction,
            artifacts = []
        )

    def write_debug_manifest(self) -> None:
        """Write or refresh the human-readable debug manifest for one query."""
        try:
            # HACK: Some of this is leftovers from when the code was untyped
            manifest = { k: v for k, v in vars(self).items() if k not in {"run_dir", "level"} }
            manifest_file = self.run_dir / "000.manifest.json"
            with open(manifest_file, "w", encoding = "utf-8") as f:
                serialize_json_to_file(manifest, f)
        except Exception as exc:
            raise Exception(f"Failed to write xCRG debug manifest: {exc}")

    def dump_json(self, label: str, payload: object | TOMBase, level: DebugLevel) -> None:
        """Best-effort debug JSON dump for inferred xCRG runs."""
        try:
            if level > self.level:
                return
            self.run_dir.mkdir(parents=True, exist_ok=True)

            # step keeps debug files sorted by emission time
            # +1 because manifest ought to always be the first file
            step = len(self.artifacts) + 1
            readable_path = self.run_dir / f"{step:03d}.{label}.json"

            with open(readable_path, "w", encoding="utf-8") as f:
                serialize_json_to_file(payload, f)

            match payload:
                case Query() | Response() as entity:
                    summary = Message_Statistics.get_from(entity)
                case _:
                    summary = ""

            self.artifacts.append({
                "label": label,
                "path": readable_path.relative_to(self.run_dir),
                "written_at": datetime.now(timezone.utc),
                "summary": summary,
            })

            self.write_debug_manifest()
        except Exception as exc:
            raise Exception(f"Failed to write debug JSON {label}: {exc}")


def safe_debug_token(value: str | None) -> str:
    """Return a filesystem-friendly token for debug run names."""
    if not value:
        return "unbound"
    token = "".join(char if char.isalnum() else "_" for char in value)
    token = "_".join(part for part in token.split("_") if part)
    return token[:80] or "unbound"


def describe_qnode_for_debug(qnode: QNode | None) -> str:
    """Return a compact qnode label for human-readable debug paths."""
    if qnode is None:
        return "unbound"
    ids = qnode.ids or []
    if ids:
        return safe_debug_token(ids[0])
    categories = qnode.categories_list
    if categories:
        return safe_debug_token(categories[0].removeprefix("biolink:"))
    return "unbound"
