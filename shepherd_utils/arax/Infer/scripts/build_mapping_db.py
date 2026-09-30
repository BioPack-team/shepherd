# Ported from RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/Infer/scripts/build_mapping_db.py.
# Changes from upstream:
#   - import paths / sys.path hacks only
#   - run mode only: the build mode (JSONL loading, table/index creation), the argparse CLI
#     and main() are removed; they build the database, which Shepherd downloads (E-10,
#     DEC-5, DEC-6), so tqdm and argparse are no longer imported. The constructor keeps
#     its signature; mode='build' raises ValueError
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
"""
xDTD (Explainable Drug-Treat-Disease) Node/Edge Mapping Database Interface
================================

SQLite interface for mapping nodes and edges from Translator KG JSONL files
(nodes.jsonl, edges.jsonl) used by the xDTD prediction model.

Tables:
  NODE_MAPPING_TABLE:
    id, name, category (JSON list), equivalent_identifiers, description,
    synonym, xref, chembl_natural_product, chembl_availability_type, chembl_black_box_warning
  EDGE_MAPPING_TABLE:
    subject, predicate, object, id, category, qualifier, publications, sources,
    resource_id (pipe-delimited), resource_role (pipe-delimited), knowledge_level,
    agent_type, stage_qualifier, original_subject, original_object, extra_attributes (JSON)

Author: Chunyu Ma
"""

import os
import sys
import json
import collections
import sqlite3
from typing import Optional, List


# Named tuples returned by get_node_info / get_edge_info
NodeInfo = collections.namedtuple('NodeInfo', [
    'id', 'name', 'category', 'equivalent_identifiers', 'description',
    'synonym', 'xref', 'chembl_natural_product', 'chembl_availability_type',
    'chembl_black_box_warning'
])

EdgeInfo = collections.namedtuple('EdgeInfo', [
    'subject', 'predicate', 'object', 'id', 'category', 'qualifier',
    'publications', 'sources', 'resource_id', 'resource_role',
    'knowledge_level', 'agent_type', 'stage_qualifier',
    'original_subject', 'original_object', 'extra_attributes'
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
        # don't have to handle the encoding themselves. Other JSON-encoded
        # fields (equivalent_identifiers/synonym/xref) currently have no live
        # consumers; leaving them as-is to avoid scope creep (#2671).
        values = list(result)
        cat_idx = NodeInfo._fields.index('category')
        if values[cat_idx]:
            try:
                values[cat_idx] = json.loads(values[cat_idx])
            except json.JSONDecodeError:
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
                None, None, None, None, None, None, None, None, None, None, None, None, None
            ))]

        cursor.execute(
            "SELECT * FROM EDGE_MAPPING_TABLE WHERE subject = ? AND predicate = ? AND object = ?",
            (subject, predicate, object_id)
        )
        return [EdgeInfo._make(record) for record in cursor.fetchall()]

