"""Upstream ARAX's own test suite (RTXteam/RTX @ 9485431, code/ARAX/test/),
run against the Shepherd port.

The test_*.py files here are upstream's, unmodified (MIT, see LICENSE.RTX).
``upstream_alias`` makes their imports (``from ARAX_query import ARAXQuery``,
``import Expand.expand_utilities``, ``from openapi_server.models...``) resolve to
the port's modules, so they exercise the port's code.

Which tests run:
- by default (CI, offline): the tests in offline_passing.txt, the ones that pass
  on upstream ARAX itself in an offline environment (no KPs, NodeNorm, NameRes
  or ARAX's own databases); every other test is skipped, since it cannot pass
  offline on upstream either. EXPECTED_DEVIATIONS are the port's recorded
  differences from upstream, run as strict xfails;
- with ``--arax-live``: every test, with upstream's own options (``--runslow``,
  ``--runexternal``, ``--runbroken``) deciding as upstream does. Run it where the
  Translator services are reachable and the real ARAX data files are in
  ARAX_DBS_DIR / ARAX_PATHFINDER_DBS_DIR (see README.md here).

The options and markers below are upstream's conftest.py, minus its session
start (ARAX's database manager and KP-info cacher, which Shepherd replaces).
"""

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # upstream's tests import each other by bare name

import upstream_alias  # noqa: E402

upstream_alias.install()

from Filter_KG.remove_nodes import RemoveNodes  # noqa: E402

RemoveNodes.load_block_list_file()

with open(os.path.join(HERE, "offline_passing.txt")) as f:
    OFFLINE_PASSING = {line.strip() for line in f if line.strip()}

# The port's recorded differences from upstream that upstream's own tests see
EXPECTED_DEVIATIONS = {
    "test_ARAX_xcrg_connect.py::test_xcrg_retriever_url_uses_arax_maturity": (
        "DEC-4: xCRG queries Shepherd's Retriever (SYNC_KG_RETRIEVAL_URL), not a "
        "per-maturity URL table; see tests/unit/arax/test_ARAX_connect.py"
    ),
    "test_ARAX_xcrg_connect.py::test_connect_xcrg_calls_package_and_updates_response": (
        "DEC-4: the xCRG config's retriever_url is Shepherd's Retriever, not "
        "retriever.<maturity>.transltr.io; everything else it checks is upstream's"
    ),
}


def pytest_addoption(parser):
    parser.addoption(
        "--arax-live",
        action="store_true",
        default=False,
        help="run all of upstream ARAX's tests (needs the Translator services and ARAX's data files)",
    )
    parser.addoption(
        "--runslow", action="store_true", default=False, help="include slow tests"
    )
    parser.addoption(
        "--runonlyslow", action="store_true", default=False, help="run only slow tests"
    )
    parser.addoption(
        "--runexternal",
        action="store_true",
        default=False,
        help="include tests that rely on external KPs",
    )
    parser.addoption(
        "--runonlyexternal",
        action="store_true",
        default=False,
        help="run only external tests",
    )
    parser.addoption(
        "--runbroken",
        action="store_true",
        default=False,
        help="include known broken tests",
    )
    parser.addoption(
        "--runonlybroken",
        action="store_true",
        default=False,
        help="Run only the known broken tests",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line(
        "markers", "external: mark test as relying on an external KP"
    )
    config.addinivalue_line("markers", "broken: mark test as broken to run")


def _upstream_id(item):
    return f"{os.path.basename(str(item.fspath))}::{item.name}"


def pytest_collection_modifyitems(config, items):
    live = config.getoption("--arax-live")
    skip_offline = pytest.mark.skip(
        reason="does not pass offline on upstream ARAX either; run with --arax-live where the services are reachable"
    )
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    skip_fast = pytest.mark.skip(
        reason="--runonlyslow option was used; this test is fast"
    )
    skip_external = pytest.mark.skip(reason="need --runexternal option to run")
    skip_internal = pytest.mark.skip(
        reason="--runonlyexternal option was used; this test is internal"
    )
    skip_broken = pytest.mark.skip(reason="need --runbroken option to run")
    skip_working = pytest.mark.skip(
        reason="--runonlybroken option was used; this test is known working"
    )
    for item in items:
        if not str(item.fspath).startswith(HERE):
            continue
        upstream_id = _upstream_id(item)
        if upstream_id in EXPECTED_DEVIATIONS:
            item.add_marker(
                pytest.mark.xfail(reason=EXPECTED_DEVIATIONS[upstream_id], strict=True)
            )
            continue
        if not live:
            if upstream_id not in OFFLINE_PASSING:
                item.add_marker(skip_offline)
            continue
        # --arax-live: upstream's own selection
        if "slow" in item.keywords:
            if not config.getoption("--runslow") and not config.getoption(
                "--runonlyslow"
            ):
                item.add_marker(skip_slow)
        elif config.getoption("--runonlyslow"):
            item.add_marker(skip_fast)
        if "external" in item.keywords:
            if not config.getoption("--runexternal") and not config.getoption(
                "--runonlyexternal"
            ):
                item.add_marker(skip_external)
        elif config.getoption("--runonlyexternal"):
            item.add_marker(skip_internal)
        if "broken" in item.keywords:
            if not config.getoption("--runbroken") and not config.getoption(
                "--runonlybroken"
            ):
                item.add_marker(skip_broken)
        elif config.getoption("--runonlybroken"):
            item.add_marker(skip_working)
