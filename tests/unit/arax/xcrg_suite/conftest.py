"""Fixtures for xCRG's unit tests.

Port of the part of upstream xCRG's ``tests/conftest.py`` the unit tests use:
the session ``config`` fixture. Upstream builds it from command-line options
(``--retriever_url``, ``--ngd_db_file``, ``--curie_to_pmids_db_file``,
``--debug_level``, ``--use_cache``) that only its integration tests change;
here it uses those options' defaults, and writes debug output to a pytest temp
directory instead of the project's ``output/debug``.
"""

import pytest

from shepherd_utils.arax.xcrg import DebugLevel, XCRGConfig


@pytest.fixture(scope="session")
def config(tmp_path_factory) -> XCRGConfig:
    debug_dir = tmp_path_factory.mktemp("xcrg_debug")

    return XCRGConfig(
        retriever_url="https://retriever.ci.transltr.io/query",
        ngd_db_path=None,
        curie_to_pmids_db_path=None,
        debug_dir=debug_dir,
        debug_level=DebugLevel.NONE,
        cache_dir=None,
        cache_ttl=None,
        cache_clear_on_start=False,
    )
