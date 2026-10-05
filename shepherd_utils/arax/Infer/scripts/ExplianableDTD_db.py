# Ported from RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/Infer/scripts/ExplianableDTD_db.py.
# Changes from upstream:
#   - import paths / sys.path hacks only
#   - run mode only: the build mode (TSV loading, table/index creation), the argparse CLI
#     and main() are removed; they build the database, which Shepherd downloads (E-10,
#     DEC-5, DEC-6), so tqdm and argparse are no longer imported
#   - the database is read from RTXConfig.explainable_dtd_db_path's directory (Shepherd's
#     ARAX data dir) instead of code/ARAX/KnowledgeSources/Prediction
#   - no ARAXDatabaseManager: a missing database raises FileNotFoundError instead of
#     being fetched here (the ARAX worker downloads it at startup, ensure_arax_dbs)
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
"""
xDTD (Explainable Drug-Treat-Disease) Prediction Database Interface
=================================

SQLite interface for operating the pre-computed xDTD prediction results.

Tables:
  PREDICTION_SCORE_TABLE:
    drug_id, drug_name, disease_id, disease_name, tn_score, tp_score, unknown_score
  PATH_RESULT_TABLE:
    drug_id, drug_name, disease_id, disease_name, path, path_score

Author: Chunyu Ma
"""

import os
import sys
import sqlite3
from typing import Optional, Union, List, Dict, Tuple

import pandas as pd
import numpy as np


# import internal modules
from shepherd_utils.arax.RTXConfiguration import RTXConfiguration #noqa: E402
RTXConfig = RTXConfiguration()

# Default output directory for the database
_DEFAULT_OUTDIR = os.path.dirname(RTXConfig.explainable_dtd_db_path)

# Column definitions for the two tables
_SCORE_COLUMNS = ["drug_id", "drug_name", "disease_id", "disease_name", "tn_score", "tp_score", "unknown_score"]
_PATH_COLUMNS = ["drug_id", "drug_name", "disease_id", "disease_name", "path", "path_score"]


class ExplainableDTD:
    """SQLite interface for the xDTD prediction score and path result database.

    Attributes:
        database_name: Filename of the SQLite database.
        outdir: Directory containing the database file.
        conn: Active sqlite3.Connection (set after connect()).
        is_connected: Whether a live connection exists.
    """

    # ──────────────────────────────────────────────────────────────────────
    #  Initialization
    # ──────────────────────────────────────────────────────────────────────

    def __init__(
        self,
        database_name: Optional[str] = None,
        outdir: Optional[str] = _DEFAULT_OUTDIR,
    ):
        """
        Args:
            database_name: SQLite filename. Defaults to the RTXConfig value.
            outdir: Directory for the database file.
        """
        self.is_connected = False
        self.conn: Optional[sqlite3.Connection] = None

        self._init_run_mode(database_name, outdir)

        self.connect()

    def _init_run_mode(self, database_name, outdir):
        """Set attributes for query/run mode using RTXConfig defaults."""
        self.database_name = database_name or RTXConfig.explainable_dtd_db_path.split("/")[-1]
        self.outdir = outdir or "./"
        os.makedirs(self.outdir, exist_ok=True)

    # ──────────────────────────────────────────────────────────────────────
    #  Connection management
    # ──────────────────────────────────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        """Return the active connection, raising RuntimeError if not connected."""
        if self.conn is None:
            raise RuntimeError("Not connected to the database. Call connect() first.")
        return self.conn

    def connect(self) -> bool:
        """Open a connection to the SQLite database.

        Returns True on success.
        """
        if self.is_connected:
            return True

        db_path = os.path.join(self.outdir, self.database_name)

        if not os.path.exists(db_path):
            # Shepherd's ARAX worker downloads the database at startup (ensure_arax_dbs)
            raise FileNotFoundError(f"Database '{db_path}' not found")

        self.conn = sqlite3.connect(db_path)
        self.is_connected = True
        print(f"INFO: Connected to database: {db_path}", flush=True)
        return True

    def disconnect(self):
        """Commit pending changes and close the database connection."""
        if not self.is_connected or self.conn is None:
            print("INFO: No active database connection to close", flush=True)
            return
        try:
            self.conn.commit()
            self.conn.close()
        except sqlite3.ProgrammingError:
            print("INFO: Database connection was already closed", flush=True)
        self.is_connected = False

    # ──────────────────────────────────────────────────────────────────────
    #  Run mode: query methods
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_curie_ids(curie_ids: Union[str, List[str], None]) -> Optional[List[str]]:
        """Ensure curie_ids is a deduplicated list, or None if empty/None."""
        if curie_ids is None:
            return None
        if isinstance(curie_ids, str):
            return [curie_ids]
        return list(set(curie_ids))

    @staticmethod
    def _build_where_clause(
        drug_ids: Optional[List[str]],
        disease_ids: Optional[List[str]],
    ) -> Tuple[str, list]:
        """Build a parameterized WHERE clause for drug/disease CURIE filtering.

        Returns (where_sql, params) where where_sql starts with ' WHERE ...' and
        params is the flat list of bind values.

        Examples:
            (' WHERE drug_id IN (?,?)', ['CHEMBL:1', 'CHEMBL:2'])
            (' WHERE drug_id IN (?) AND disease_id IN (?)', ['CHEMBL:1', 'MONDO:1'])
        """
        clauses = []
        params = []
        if drug_ids:
            clauses.append(f"drug_id IN ({','.join('?' * len(drug_ids))})")
            params.extend(drug_ids)
        if disease_ids:
            clauses.append(f"disease_id IN ({','.join('?' * len(disease_ids))})")
            params.extend(disease_ids)
        if not clauses:
            return "", []
        return " WHERE " + " AND ".join(clauses), params

    def get_score_table(
        self,
        drug_curie_ids: Union[str, List[str], None] = None,
        disease_curie_ids: Union[str, List[str], None] = None,
    ) -> pd.DataFrame:
        """Query the PREDICTION_SCORE_TABLE for matching drug/disease CURIEs.

        Args:
            drug_curie_ids: Single CURIE string or list, e.g. "CHEMBL.COMPOUND:CHEMBL55643" or ["CHEMBL.COMPOUND:CHEMBL55643","CHEBI:6908"].
            disease_curie_ids: Single CURIE string or list, e.g. "MONDO:0008753" or ["MONDO:0008753","MONDO:0005148","MONDO:0005155"].

        Returns:
            DataFrame with columns: drug_id, drug_name, disease_id, disease_name,
            tn_score, tp_score, unknown_score.  Empty DataFrame if no IDs provided.
        """
        drug_ids = self._normalize_curie_ids(drug_curie_ids)
        disease_ids = self._normalize_curie_ids(disease_curie_ids)

        if not drug_ids and not disease_ids:
            print("WARNING: get_score_table called with no drug or disease CURIEs", flush=True)
            return pd.DataFrame([], columns=_SCORE_COLUMNS)

        where_sql, params = self._build_where_clause(drug_ids, disease_ids)
        query = f"SELECT {', '.join(_SCORE_COLUMNS)} FROM PREDICTION_SCORE_TABLE{where_sql}"

        cursor = self._get_conn().cursor()
        cursor.execute(query, params)
        return pd.DataFrame(cursor.fetchall(), columns=_SCORE_COLUMNS)

    def get_top_path(
        self,
        drug_curie_ids: Union[str, List[str], None] = None,
        disease_curie_ids: Union[str, List[str], None] = None,
    ) -> Dict[Tuple[str, str], List[list]]:
        """Query the PATH_RESULT_TABLE for explanation paths matching drug/disease CURIEs.

        Args:
            drug_curie_ids: Single CURIE string or list, e.g. "CHEMBL.COMPOUND:CHEMBL55643" or ["CHEMBL.COMPOUND:CHEMBL55643","CHEBI:6908"].
            disease_curie_ids: Single CURIE string or list, e.g. "MONDO:0008753" or ["MONDO:0008753","MONDO:0005148","MONDO:0005155"].

        Returns:
            Dict mapping (drug_id, disease_id) -> list of [path_string, path_score].
            Empty dict if no IDs provided.
        """
        drug_ids = self._normalize_curie_ids(drug_curie_ids)
        disease_ids = self._normalize_curie_ids(disease_curie_ids)

        if not drug_ids and not disease_ids:
            print("WARNING: get_top_path called with no drug or disease CURIEs", flush=True)
            return {}

        where_sql, params = self._build_where_clause(drug_ids, disease_ids)
        query = f"SELECT drug_id, disease_id, path, path_score FROM PATH_RESULT_TABLE{where_sql}"

        cursor = self._get_conn().cursor()
        cursor.execute(query, params)

        top_paths: Dict[Tuple[str, str], List[list]] = {}
        for drug_id, disease_id, path, path_score in cursor.fetchall():
            top_paths.setdefault((drug_id, disease_id), []).append([path, path_score])
        return top_paths

