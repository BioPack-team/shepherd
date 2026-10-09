# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/retriever.py (DEC-21).
# Changes from upstream:
#   - TRAPI 2.0: one EdgeBinding per qedge (ids); bypass_cache is read from parameters; optional logs/edges read through *_list/*_dict
#   - logs the lookup's parameters
#   - a direct lookup is filtered on its own qedge (upstream read `direct`, dropping every result of a direct lookup)
# See README.md.
from __future__ import annotations

import asyncio
import sqlite3
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast

import httpx

from translator_tom import (
    Analysis,
    AuxiliaryGraphsDict,
    Edge,
    EdgeID,
    KnowledgeGraph,
    Query,
    QEdgeID,
    QNodeID,
    QueryGraph,
    Response,
    Result,
)

from . import trapi
from .constants import (
    TF_QNODE_ID,
    TP53_CURIE
)
from .context import RunContext
from .models import Message_Statistics
from .reporting import Reporter
from .utilities import format_json_for_log, make_stable_id


# TODO: Import datetime.UTC in Python 3.11+
UTC = timezone.utc

# The expires_at value when TTL is not set.
# The value needs to be implausible, but also not extreme.
# This is because systems like Windows can have hard limits on their datetime data structures.
NO_TTL = timedelta(weeks = 52 * 100)

_CACHE_DB_FILENAME = "retriever_cache.db"
_CACHE_LOCK = asyncio.Lock() # Avoid race conditions with asyncio
_CACHE: Retriever_Cache | None = None


@dataclass
class Retriever_Cache:
    reporter         : Reporter
    cache_dir        : Path
    connection       : sqlite3.Connection = field(init = False)
    ttl              : timedelta | None   = field(default = None)
    cleanup_interval : timedelta          = field(default = timedelta(days = 1))
    next_cleanup     : datetime           = field(init = False)

    def __post_init__(self):
        self.connection = sqlite3.connect(
            database = self.cache_dir / _CACHE_DB_FILENAME,
            check_same_thread = False
            # TODO Python 3.12+: autocommit = False,
        )
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS Cache_Entry (
                filename   TEXT PRIMARY KEY,
                expires_at INTEGER NOT NULL
            )
        """)
        self.connection.commit()
        self.next_cleanup = datetime.now(UTC) + self.cleanup_interval

    def get_entries(self) -> list[tuple[Path, datetime]]:
        entries = list[tuple[Path, datetime]]()
        try:
            rows = self.connection.execute("SELECT * FROM Cache_Entry").fetchall()
            for (filename, expires_at) in rows:
                entries.append((self.cache_dir / filename, datetime.fromtimestamp(expires_at, UTC)))
        except Exception as e:
            self.reporter.warning("Failed to get entries: %s", e)
        return entries

    def remove_all_files(self):
        try:
            self.reporter.debug("Removing all cache files: %s", self.cache_dir)
            self.connection.execute("DELETE FROM Cache_Entry")
            self.connection.commit()
            for pattern in ("*.json", "*.tmp"):
                for file in self.cache_dir.glob(pattern):
                    file.unlink()
                    self.reporter.debug("Removed cache file: %s", file)
            self.reporter.debug("All cache files removed OK")
        except Exception as e:
            self.reporter.warning("Failed to clear cache: %s", e)

    def remove_expired_files(self):
        try:
            self.next_cleanup = datetime.now(UTC) + self.cleanup_interval
            self.reporter.debug("Removing expired cache files: %s", self.cache_dir)
            rows = self.connection.execute("""
                DELETE FROM Cache_Entry
                WHERE expires_at <= UNIXEPOCH()
                RETURNING filename
            """).fetchall()
            self.connection.commit()
            for (filename,) in rows:
                file = self.cache_dir / filename
                if not file.exists():
                    self.reporter.warning("Skipping missing cache file: %s", file)
                    continue
                file.unlink()
                self.reporter.debug("Removed cache file: %s", file)
        except Exception as e:
            self.reporter.warning("Failed to remove expired cache files: %s", e)

    def write_file(self, filename: str, payload: str):
        file = self.cache_dir / filename
        try:
            expires_at = datetime.now(UTC) + (self.ttl or NO_TTL)
            self.connection.execute("""
                INSERT INTO Cache_Entry (filename, expires_at)
                VALUES (?, ?)
                ON CONFLICT (filename) DO UPDATE SET
                    expires_at = EXCLUDED.expires_at
            """, (filename, expires_at.timestamp()))
            self.connection.commit()
            tmp_file = self.cache_dir / f"{uuid.uuid4()}.tmp"
            with open(tmp_file, "w", encoding = "utf-8") as f:
                f.write(payload)
                f.flush()
            tmp_file.replace(file) # ought to be atomic
            self.reporter.debug("Wrote cache file: %s", file)
        except Exception as e:
            self.reporter.warning("Failed to write cache file %s: %s", file, e)

    def read_file(self, filename: str) -> str | None:
        file = self.cache_dir / filename
        try:
            row = self.connection.execute("SELECT * FROM Cache_Entry WHERE filename = ?", (filename,)).fetchone()
            if not row: return None
            (filename, expires_at) = row
            if not file.exists(): return None
            if expires_at <= datetime.now(UTC).timestamp(): return None
            with open(file, "r", encoding = "utf-8") as f:
                return f.read()
        except Exception as e:
            self.reporter.warning("Failed to read cache file %s: %s", file, e)
            return None
        finally:
            if self.next_cleanup <= datetime.now(UTC):
                self.remove_expired_files()

def _result_has_edge_predicate(
    result: Result,
    edges: dict[EdgeID, Edge],
    predicate: str
) -> bool:
    """Return True when any bound knowledge graph edge has the given predicate."""
    for analysis in result.analyses_list:
        if not isinstance(analysis, Analysis):
            continue
        for binding in analysis.edge_bindings_dict.values():
            for edge_id in binding.ids:
                edge = edges.get(edge_id)
                if not edge:
                    continue
                if edge.predicate == predicate:
                    return True
    return False


def _result_preserves_direct_direction(
    result: Result,
    edges: dict[EdgeID, Edge],
    subject_qid: QNodeID,
    object_qid: QNodeID,
    qedge_id: QEdgeID,
) -> bool:
    """Check that a direct result preserves the original source->target direction."""
    source_id = trapi.get_bound_node_curie(result, subject_qid)
    target_id = trapi.get_bound_node_curie(result, object_qid)
    if not source_id or not target_id:
        return False

    for analysis in result.analyses_list:
        if not isinstance(analysis, Analysis):
            continue
        direct_binding = analysis.edge_bindings_dict.get(qedge_id)
        if not direct_binding:
            return False

        for edge_id in direct_binding.ids:
            edge = edges.get(edge_id)
            if not edge or edge.subject != source_id or edge.object != target_id:
                return False

    return True


def _result_preserves_direction(
    result: Result,
    edges: dict[EdgeID, Edge],
    subject_qid: QNodeID,
    object_qid: QNodeID,
) -> bool:
    """Check that the result preserves source->tf and tf->target edge directions."""
    source_curie = trapi.get_bound_node_curie(result, subject_qid)
    tf_curie = trapi.get_bound_node_curie(result, TF_QNODE_ID)
    target_curie = trapi.get_bound_node_curie(result, object_qid)
    if not source_curie or not tf_curie or not target_curie:
        return False

    for analysis in result.analyses_list:
        if not isinstance(analysis, Analysis): # TODO
            continue

        e0_binding = analysis.edge_bindings_dict.get("e0")
        e1_binding = analysis.edge_bindings_dict.get("e1")
        if not e0_binding or not e1_binding:
            return False

        for edge_id in e0_binding.ids:
            edge = edges.get(edge_id)
            if not edge or edge.subject != source_curie or edge.object != tf_curie:
                return False

        for edge_id in e1_binding.ids:
            edge = edges.get(edge_id)
            if not edge or edge.subject != tf_curie or edge.object != target_curie:
                return False

    return True


def _filter_direct_response(ctx: RunContext, response: Response, qedge_id: QEdgeID) -> Response:
    """Filter subclass and wrong-direction results from a direct Retriever response.

    The direct edge is the lookup's own qedge: "direct" in an inferred run's one-hop
    lookup, the query's edge in a direct lookup (upstream always read "direct", which
    filtered out every result of a direct lookup).
    """
    message = response.message

    edges: dict[EdgeID, Edge] = {}
    if message.knowledge_graph:
        edges = message.knowledge_graph.edges_dict

    filtered_results: list[Result] = []
    for result in message.results_list:
        if _result_has_edge_predicate(result, edges, "biolink:subclass_of"):
            continue
        if not _result_preserves_direct_direction(result, edges, ctx.subject_qid, ctx.object_qid, qedge_id):
            continue
        filtered_results.append(result)

    filtered_message = deepcopy(message)
    filtered_message.results = filtered_results
    if not filtered_message.knowledge_graph:
        filtered_message.knowledge_graph = KnowledgeGraph.new()
    if not filtered_message.auxiliary_graphs:
        filtered_message.auxiliary_graphs = AuxiliaryGraphsDict()

    # Stamp response with version information if upstream omitted it
    return Response(
        schema_version = response.schema_version or ctx.trapi_schema_version,
        biolink_version = response.biolink_version or ctx.config.biolink_version,
        message = filtered_message,
    )


def _filter_inferred_response(ctx: RunContext, response: Response) -> Response:
    """Filter subclass and wrong-direction results from a two-hop Retriever response."""
    message = response.message

    edges = dict[EdgeID, Edge]()
    if message.knowledge_graph:
        edges = message.knowledge_graph.edges_dict

    filtered_results = []
    for result in message.results_list:
        tf_id = trapi.get_bound_node_curie(result, TF_QNODE_ID)
        if tf_id == TP53_CURIE:
            continue
        if _result_has_edge_predicate(result, edges, "biolink:subclass_of"):
            continue
        if not _result_preserves_direction(result, edges, ctx.subject_qid, ctx.object_qid):
            continue
        filtered_results.append(result)

    filtered_message = deepcopy(message)
    filtered_message.results = filtered_results
    if not filtered_message.knowledge_graph:
        filtered_message.knowledge_graph = KnowledgeGraph.new()
    if not filtered_message.auxiliary_graphs:
        filtered_message.auxiliary_graphs = AuxiliaryGraphsDict()

    # Stamp response with version information if upstream omitted it
    return Response(
        schema_version = response.schema_version or ctx.trapi_schema_version,
        biolink_version = response.biolink_version or ctx.config.biolink_version,
        message = filtered_message
    )


async def _get_trapi_response_from_retriever(
    ctx: RunContext,
    cache: Retriever_Cache | None,
    query: Query
) -> tuple[int, Response]:
    """Make HTTP query to Retriever and return HTTP status code + TRAPI Response"""

    # Try and return a cached TRAPI Response if appropriate options are set
    cache_filename: str | None = None
    if cache:
        cache_filename: str = make_stable_id("retriever_trapi_response", query) + ".json"
        bypass_cache = query.parameters.bypass_cache if query.parameters else False
        if not bypass_cache and (text := cache.read_file(cache_filename)):
            ctx.reporter.info(f"Returning cached TRAPI response: {cache_filename}")
            return 200, Response.from_json(text)

    # TODO: We need to clarify the correct behavior for timeout
    # TODO: timeout = httpx.Timeout(ctx.timeout - ctx.elapsed_time().seconds)
    timeout = httpx.Timeout(timeout = ctx.timeout)

    async with httpx.AsyncClient(timeout = timeout) as client:
        try:
            http_response = await client.post(ctx.config.retriever_url, json = query.to_dict())
            http_response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            ctx.reporter.error(
                "xCRG Retriever HTTP error %s from %s: %s",
                exc.response.status_code,
                ctx.config.retriever_url,
                exc.response.text[:2000],
            )
            raise

        # Write HTTP response to cache if appropriate options are set
        if cache and cache_filename:
            ctx.reporter.debug(f"Writing TRAPI Response to cache file: {cache_filename}")
            cache.write_file(cache_filename, http_response.text)

        return http_response.status_code, Response.from_dict(http_response.json())


async def run_sync_lookup(ctx: RunContext, query: Query) -> Response:
    """Run a sync Retriever lookup and return its TRAPI response."""
    ctx.reporter.info("Sending xCRG lookup query to %s", ctx.config.retriever_url)
    ctx.reporter.debug(
        "xCRG Retriever query graph: %s",
        format_json_for_log(query.message.query_graph),
    )

    # Initialize cache singleton to manage TRAPI Responses from Retriever
    global _CACHE
    async with _CACHE_LOCK:
        if not _CACHE and (cache_dir := ctx.get_cache_dir()):
            _CACHE = Retriever_Cache(ctx.reporter, cache_dir, ctx.config.cache_ttl)
            if _CACHE and ctx.config.cache_clear_on_start:
                _CACHE.remove_all_files()

    ctx.reporter.debug(
        "xCRG Retriever parameters: %s",
        format_json_for_log(query.parameters),
    )
    http_status_code, response = await _get_trapi_response_from_retriever(ctx, _CACHE, query)

    message = response.message
    if not message:
        raise ValueError("Retriever response did not contain a TRAPI message.")

    if not message.knowledge_graph:
        message.knowledge_graph = KnowledgeGraph.new()
    if not message.results:
        message.results = list[Result]()
    if not message.auxiliary_graphs:
        message.auxiliary_graphs = AuxiliaryGraphsDict()

    counts = Message_Statistics.get_from(response)

    ctx.reporter.info(
        "xCRG Retriever response HTTP %s; status=%s; results=%s; nodes=%s; edges=%s; description=%s",
        http_status_code,
        response.status,
        counts.result_count,
        counts.node_count,
        counts.edge_count,
        response.description,
    )

    if response and response.status != "Success":
        ctx.reporter.warning(
            "xCRG Retriever returned non-complete status %s: %s",
            response.status,
            response.description,
        )
    if counts.result_count == 0 or response.status != "Success":
        for entry in response.logs_list[:5]:
            ctx.reporter.info("xCRG Retriever log [%s] %s", entry.level or "INFO", entry.message)

    # ctx.debug_dump_json("raw_response", response)

    # Filter response to ensure we have results with correct subclasses, directions, etc.
    qgraph = cast(QueryGraph, query.message.query_graph)
    if trapi.is_two_hop_query(qgraph):
        response = _filter_inferred_response(ctx, response)
    else:
        response = _filter_direct_response(ctx, response, trapi.get_single_query_edge(qgraph)[0])

    return response
