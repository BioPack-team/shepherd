# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/config.py (DEC-21).
# Changes from upstream:
#   - TRAPI 2.0: trapi_schema_version defaults to 2.0.0
# See README.md.
"""Configuration for reusable xCRG execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Sequence

from .debugging import DebugLevel


# TODO: XCRGConfig -> Config
@dataclass(frozen=True)
class XCRGConfig:
    """Runtime inputs supplied by ARAX, Shepherd, or local tests."""

    retriever_url          : str
    ngd_db_path            : str | Path | None       = None
    curie_to_pmids_db_path : str | Path | None       = None
    tf_path                : str | Path | None       = None
    timeout                : int                     = 210
    tiers                  : Sequence[int]           = field(default_factory=lambda: [0])
    tf_batch_size          : int                     = 50
    resource_id            : str                     = "infores:arax"
    scoring_method         : str                     = "xcrg-result-filtering-v2" # TODO: StrEnum?
    max_results            : int                     = 500
    trapi_schema_version   : str                     = "2.0.0"
    biolink_version        : str                     = "4.3.2"
    cache_dir              : Path | None             = None
    cache_ttl              : timedelta | None        = None
    cache_clear_on_start   : bool                    = True
    debug_dir              : str | Path | None       = None
    debug_run_name         : str | None              = None
    debug_level            : str | DebugLevel | None = DebugLevel.BASIC
