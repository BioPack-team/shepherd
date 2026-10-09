# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/__init__.py (DEC-21).
# Changes from upstream:
#   - unchanged
# See README.md.
"""Reusable xCRG package."""

from .config import XCRGConfig
from .debugging import DebugLevel
from .runner import async_run_xcrg, is_xcrg_mvp2_query, run_xcrg

__all__ = [
    "DebugLevel",
    "XCRGConfig",
    "async_run_xcrg",
    "is_xcrg_mvp2_query",
    "run_xcrg",
]

__version__ = "0.1.0"
