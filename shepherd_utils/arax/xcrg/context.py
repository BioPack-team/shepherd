# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/context.py (DEC-21).
# Changes from upstream:
#   - the bundled TF list is read from this package's resources
# See README.md.
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from dataclasses import dataclass, field
from functools import cached_property
from importlib import resources
from pathlib import Path
from typing import cast

from translator_tom import (
    CURIE,
    QEdge,
    QEdgeID,
    QNode,
    QNodeID,
    Query,
    QueryGraph,
    TOMBase
)

from . import trapi
from .config import DebugLevel, XCRGConfig as Config # TODO
from .constants import DEFAULT_TF_FILE, TP53_CURIE
from .debugging import DebugContext
from .reporting import Reporter
from .utilities import path_or_none

@dataclass
class RunContext:
    """
    A RunContext maintains and provides convenient access to state
    for a particular query request through the xCRG runner.

    Attributes:
        query_id  : the unique identifier for this run
        query     : the original query submitted to the runner
        config    : the configuration for this run
        reporter  : the reporter for this run
        debug_ctx : the debugging context for this run

        query_edge_id : a reference to the ID for the single edge in the original query
        query_edge    : a reference to the single edge in the original query

        start_time : the time when the xCRG run began
    """

    query_id  : str
    query     : Query
    config    : Config
    reporter  : Reporter
    debug_ctx : DebugContext | None = None

    query_edge_id : QEdgeID = field(init = False)
    query_edge    : QEdge   = field(init = False)

    start_time = datetime.now(timezone.utc)

    def __post_init__(self):
        self.query_edge_id, self.query_edge = trapi.get_single_query_edge(self.query.message.query_graph)

    @property
    def query_graph(self) -> QueryGraph:
        return cast(QueryGraph, self.query.message.query_graph)

    @property
    def subject_qid(self) -> QNodeID:
        """The subject reference in the original query."""
        return self.query_edge.subject

    @property
    def subject_qnode(self) -> QNode:
        """The subject node in the original query."""
        return self.query_graph.nodes[self.subject_qid]

    @property
    def object_qid(self) -> QNodeID:
        """The object reference in the original query."""
        return self.query_edge.object

    @property
    def object_qnode(self) -> QNode:
        """The object node in the original query."""
        return self.query_graph.nodes[self.object_qid]

    @property
    def biolink_version(self):
        return self.config.biolink_version

    @property
    def trapi_schema_version(self):
        return self.config.trapi_schema_version

    @property
    def ngd_db_file(self) -> Path | None:
        return path_or_none(self.config.ngd_db_path)

    @property
    def curie_to_pmids_db_file(self) -> Path | None:
        return path_or_none(self.config.curie_to_pmids_db_path)

    @property
    def scoring_method(self) -> str:
        return self.config.scoring_method

    @property
    def resource_id(self) -> str:
        return self.config.resource_id

    @property
    def num_max_results(self) -> int:
        return self.config.max_results

    @property
    def timeout(self) -> int:
        return self.config.timeout

    def use_cache(self) -> bool:
        return self.config.cache_dir is not None

    @staticmethod
    def new(
        query_id: str,
        query: Query,
        config: Config,
        reporter: Reporter,
    ) -> RunContext:
        """Instantiate a new RunContext."""
        debug_ctx: DebugContext | None = None
        if debug_dir := path_or_none(config.debug_dir):
            debug_ctx = DebugContext.new(
                debug_dir = debug_dir,
                debug_level = config.debug_level,
                run_name = config.debug_run_name,
                query = query,
                query_id = query_id,
            )
        else:
            reporter.info("debug_dir does not exist; debugger will not be used for this run.")

        return RunContext(
            query_id = query_id,
            query = query,
            config = config,
            reporter = reporter,
            debug_ctx = debug_ctx
        )

    def elapsed_time(self) -> timedelta:
        """The amount of time that has progressed in the run."""
        return datetime.now(timezone.utc) - self.start_time

    def debug_dump_json(
        self,
        label: str,
        payload: object | TOMBase,
        level: DebugLevel = DebugLevel.ALL
    ) -> None:
        if not (debug := self.debug_ctx): return
        try:
            debug.dump_json(label, payload, level)
        except Exception as e:
            self.reporter.warning(str(e))

    @cached_property
    def tf_list(self) -> list[CURIE]:
        """Get transcription factors from config or bundled package resources."""

        def try_loading_config_tf_file() -> list[CURIE] | None:
            if not (tf_file := path_or_none(self.config.tf_path)):
                return None
            if not tf_file.exists():
                self.reporter.warning(f"Transcription factors file does not exist: {tf_file}")
                return None
            try:
                self.reporter.debug(f"Loading transcription factors from file: {tf_file}")
                with tf_file.open(encoding = "utf-8") as f:
                    return json.load(f).get("tf")
            except Exception:
                pass
            self.reporter.warning(f"Transcription factors could not be loaded from file: {tf_file}")
            return None

        def try_loading_default_tf_file() -> list[CURIE] | None:
            try:
                tf_file = resources.files(f"{__package__}.resources").joinpath(DEFAULT_TF_FILE)
                self.reporter.debug(f"Loading transcription factors from default file: {DEFAULT_TF_FILE}")
                with tf_file.open(encoding = "utf-8") as f:
                    return json.load(f).get("tf")
            except Exception:
                pass
            self.reporter.warning(f"Transcription factors could not be loaded from file: {DEFAULT_TF_FILE}")
            return None

        tf_list = try_loading_config_tf_file()
        if not tf_list:
            tf_list = try_loading_default_tf_file()
        if not tf_list:
            raise ValueError("Transcription factors failed to load.")

        endpoint_ids = set(self.subject_qnode.ids_list) | set(self.object_qnode.ids_list)

        tf_list = [
            tf_id
            for tf_id in tf_list
            if tf_id != TP53_CURIE and tf_id not in endpoint_ids
        ]
        if not tf_list:
            raise ValueError("No transcription factors remain after TP53/target filtering.")

        return tf_list

    def get_answer_qid(self) -> QNodeID:
        return trapi.get_answer_qid(self.query_graph, self.subject_qid, self.object_qid)

    def get_cache_dir(self) -> Path | None:
        if not (cache_dir := path_or_none(self.config.cache_dir)): return None
        cache_dir.mkdir(exist_ok = True)
        return cache_dir
