# Ported from RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/Infer/scripts/build_mapping_db.py,
# synced to upstream trapi2.0 @ 3d4ce77 (DEC-30).
# Changes from upstream:
#   - import paths / sys.path hacks only
#   - run mode only: the build mode (JSONL loading, table/index creation), the argparse CLI
#     and main() are removed; they build the database, which Shepherd downloads (E-10,
#     DEC-5, DEC-6), so tqdm and argparse are no longer imported. The constructor keeps
#     its signature; mode='build' raises ValueError
#   - from upstream trapi2.0 (#2893, 08fedb0e, 8789ff4d): the tier0-20260928 schema (NodeInfo is
#     id/name/category/extra_attributes, EdgeInfo subject/predicate/object/id/category/
#     extra_attributes), get_edge_info decodes the category, and the batch lookups
#     get_nodes_info_batch / get_edges_info_batch
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
"""
xDTD (Explainable Drug-Treat-Disease) Node/Edge Mapping Database Interface
================================

SQLite interface for mapping nodes and edges from Translator KG JSONL files
(nodes.jsonl, edges.jsonl) used by the xDTD prediction model.

Tables:
  NODE_MAPPING_TABLE:
    id, name, category (JSON list), extra_attributes (JSON dict of all
    remaining properties discovered in the JSONL)
  EDGE_MAPPING_TABLE:
    subject, predicate, object, id, category, extra_attributes (JSON dict of
    all remaining properties discovered in the JSONL)

Author: Chunyu Ma
"""

import os
import sys
import json
import collections
import sqlite3
from typing import Optional, List, Dict


# Named tuples returned by get_node_info / get_edge_info
# Only core fields are separate columns; all other node properties are stored
# in a single extra_attributes JSON column, so the schema adapts automatically
# when the upstream KG adds or removes node properties.
NodeInfo = collections.namedtuple('NodeInfo', [
    'id', 'name', 'category', 'extra_attributes'
])

EdgeInfo = collections.namedtuple('EdgeInfo', [
    'subject', 'predicate', 'object', 'id', 'category', 'extra_attributes'
])


class xDTDMappingDB:
    """SQLite interface for the xDTD node/edge mapping database.

    Attributes:
        database_name: Filename of the SQLite database.
        conn: Active sqlite3.Connection (set after construction).
    """

    def __init__(self, database_name: str = 'ExplainableDTD.db', outdir: Optional[str] = None,
                 mode: str = 'build', db_loc: Optional[str] = None):
        """
        Args:
            database_name: Database filename (default: ExplainableDTD.db).
            outdir: Output directory for build mode (default: ./).
            mode: 'build' to create from scratch, 'run' to open existing.
            db_loc: Directory of an existing database (required for mode='run').
        """
        self.database_name = database_name

        if mode == 'run':
            if db_loc is None:
                raise ValueError("db_loc is required for mode='run'")
            db_path = os.path.join(db_loc, database_name)
        else:
            raise ValueError(f"Unknown mode '{mode}'. Only 'run' is supported.")

        self.conn = sqlite3.connect(db_path)
        print(f"INFO: Connected to database: {db_path}", flush=True)

    def __del__(self):
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.commit()
                self.conn.close()
                print("INFO: Disconnected from database", flush=True)
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────────
    #  Run mode: query methods
    # ──────────────────────────────────────────────────────────────────────

    def get_node_info(self, node_id: Optional[str] = None, node_name: Optional[str] = None) -> Optional[NodeInfo]:
        """Look up a node by ID or name.

        Args:
            node_id: Exact node CURIE, e.g. "CHEBI:10".
            node_name: Case-insensitive name match, e.g. "Nalidixic acid".
        Returns:
            NodeInfo namedtuple, or None if not found.
        """
        cursor = self.conn.cursor()
        if node_id is not None:
            cursor.execute("SELECT * FROM NODE_MAPPING_TABLE WHERE id = ?", (node_id,))
        elif node_name is not None:
            cursor.execute("SELECT * FROM NODE_MAPPING_TABLE WHERE name = ? COLLATE NOCASE", (node_name,))
        else:
            return None
        result = cursor.fetchone()
        if not result:
            return None
        # `category` is stored as a JSON-encoded list string by _insert_nodes;
        # decode it back to a list so callers (Node.categories expects a list)
        # don't have to handle the encoding themselves.
        values = list(result)
        cat_idx = NodeInfo._fields.index('category')
        if values[cat_idx]:
            try:
                values[cat_idx] = json.loads(values[cat_idx])
            except (json.JSONDecodeError, TypeError):
                pass
        return NodeInfo._make(values)

    def get_edge_info(self, subject: Optional[str] = None, predicate: Optional[str] = None,
                      object_id: Optional[str] = None, triple_id: Optional[tuple] = None) -> List[EdgeInfo]:
        """Look up edges by (subject, predicate, object) triple.

        Supports both explicit arguments and a legacy triple_id=(s, p, o) tuple
        for backward compatibility with infer_utilities.py.

        Args:
            subject: Subject node CURIE.
            predicate: Biolink predicate string.
            object_id: Object node CURIE.
            triple_id: Legacy (subject, predicate, object) tuple.
        Returns:
            List of EdgeInfo namedtuples. Empty list if not found.
        """
        cursor = self.conn.cursor()

        if triple_id is not None and isinstance(triple_id, tuple):
            subject, predicate, object_id = triple_id

        if subject is None or predicate is None or object_id is None:
            return []

        # SELF_LOOP_RELATION is a synthetic edge used by the xDTD model for flexible path lengths
        if predicate == 'SELF_LOOP_RELATION':
            return [EdgeInfo._make((
                subject, predicate, object_id,
                None, None, None
            ))]

        cursor.execute(
            "SELECT * FROM EDGE_MAPPING_TABLE WHERE subject = ? AND predicate = ? AND object = ?",
            (subject, predicate, object_id)
        )
        return [self._make_edge_info(record) for record in cursor.fetchall()]

    # ──────────────────────────────────────────────────────────────────────
    #  Batch query methods (performance optimization for xDTD path lookups)
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _decode_category(raw_value):
        """Decode a JSON-encoded category string back to a list."""
        if raw_value is None:
            return None
        try:
            return json.loads(raw_value)
        except (json.JSONDecodeError, TypeError):
            return raw_value

    _EDGE_CAT_IDX = EdgeInfo._fields.index('category')

    def _make_edge_info(self, row: tuple) -> EdgeInfo:
        """Construct an EdgeInfo from a raw SQL row, decoding the category JSON."""
        values = list(row)
        values[self._EDGE_CAT_IDX] = self._decode_category(values[self._EDGE_CAT_IDX])
        return EdgeInfo._make(values)

    def get_nodes_info_batch(self, node_ids: List[str]) -> Dict[str, NodeInfo]:
        """Look up multiple nodes by ID in a single query.

        Args:
            node_ids: List of node CURIEs to look up.
        Returns:
            Dict mapping node_id -> NodeInfo. Missing IDs are omitted.
        """
        if not node_ids:
            return {}

        unique_ids = list(set(node_ids))
        result_map: Dict[str, NodeInfo] = {}
        cat_idx = NodeInfo._fields.index('category')

        CHUNK = 500
        cursor = self.conn.cursor()
        for start in range(0, len(unique_ids), CHUNK):
            chunk = unique_ids[start:start + CHUNK]
            placeholders = ','.join('?' * len(chunk))
            cursor.execute(
                f"SELECT * FROM NODE_MAPPING_TABLE WHERE id IN ({placeholders})", chunk
            )
            for row in cursor.fetchall():
                values = list(row)
                values[cat_idx] = self._decode_category(values[cat_idx])
                info = NodeInfo._make(values)
                result_map[info.id] = info

        return result_map

    def get_edges_info_batch(self, triples: List[tuple]) -> Dict[tuple, List[EdgeInfo]]:
        """Look up multiple edges by (subject, predicate, object) triples in a single query.

        Args:
            triples: List of (subject, predicate, object) tuples.
        Returns:
            Dict mapping (subject, predicate, object) -> list of EdgeInfo.
            Missing triples map to an empty list.
        """
        if not triples:
            return {}

        unique_triples = list(set(triples))
        result_map: Dict[tuple, List[EdgeInfo]] = {t: [] for t in unique_triples}

        db_triples = []
        for t in unique_triples:
            if t[1] == 'SELF_LOOP_RELATION':
                result_map[t] = [EdgeInfo._make((
                    t[0], t[1], t[2],
                    None, None, None
                ))]
            else:
                db_triples.append(t)

        if not db_triples:
            return result_map

        CHUNK = 200
        cursor = self.conn.cursor()
        for start in range(0, len(db_triples), CHUNK):
            chunk = db_triples[start:start + CHUNK]
            parts = []
            params = []
            for s, p, o in chunk:
                parts.append("SELECT * FROM EDGE_MAPPING_TABLE WHERE subject = ? AND predicate = ? AND object = ?")
                params.extend([s, p, o])
            sql = " UNION ALL ".join(parts)
            cursor.execute(sql, params)
            for row in cursor.fetchall():
                edge = self._make_edge_info(row)
                key = (edge.subject, edge.predicate, edge.object)
                result_map[key].append(edge)

        return result_map
