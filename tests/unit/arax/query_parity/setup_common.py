"""Import-path and fixture setup shared by the port and upstream runners."""

import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
EXPAND = os.path.join(HERE, "..", "expand_parity")
sys.path.insert(0, HERE)
sys.path.insert(0, EXPAND)

# expand_parity/runner_common.py under another name (this dir has its own)
import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "runner_common_expand", os.path.join(EXPAND, "runner_common.py")
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["runner_common_expand"] = _mod

import mock_retriever  # noqa: E402

PORT = mock_retriever.start()
os.environ["EXPAND_PARITY_MOCK_URL"] = f"http://127.0.0.1:{PORT}"
_spec.loader.exec_module(_mod)

import fixtures as F  # noqa: E402
import dbs  # noqa: E402

TIER0 = "tier0-info-for-overlay_v1.0_tier0-20260621.sqlite"
CURIE_TO_PMIDS = "curie_to_pmids_v1.0_tier0-20260621.sqlite"
XDTD = "ExplainableDTD_v1.0_tier0-20260621-all_with_paths.db"
COHD = "COHDdatabase_v1.0_KG2.8.0.db"
FDA = "fda_approved_drugs_v1.0.pickle"


def write_data(tier0_dir, fda_dir, ngd_dir, cohd_dir, xdtd_dir):
    F.write_tier0_sqlite(os.path.join(tier0_dir, TIER0))
    F.write_fda_pickle(os.path.join(fda_dir, FDA))
    dbs.write_all(
        os.path.join(ngd_dir, CURIE_TO_PMIDS),
        os.path.join(cohd_dir, COHD),
        os.path.join(xdtd_dir, XDTD),
    )


def patch_cohd_api():
    """COHD's biolink_to_omop web call, answered from dbs.OMOP in both runs."""
    n = 0
    for mod in list(sys.modules.values()):
        cls = getattr(mod, "COHDIndex", None)
        if isinstance(cls, type):
            cls._call_cohd_biolink_to_omop_api = staticmethod(dbs.fake_biolink_to_omop)
            n += 1
    return n
