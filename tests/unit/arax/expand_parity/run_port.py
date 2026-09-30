"""Run the Expand parity cases against the Shepherd port; writes JSON results.

Usage: PYTHONHASHSEED=0 python run_port.py OUT.json
(ARAX builds several lists from sets, so outputs are only reproducible with a
fixed hash seed -- the same one Shepherd's Dockerfiles pin.)
"""

import json, os, shutil, sys, tempfile, warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..", "..")))

import mock_retriever  # noqa: E402

port = mock_retriever.start()
DATA = tempfile.mkdtemp(prefix="expand_parity_")
os.environ["EXPAND_PARITY_MOCK_URL"] = f"http://127.0.0.1:{port}"
os.environ["SYNC_KG_RETRIEVAL_URL"] = f"http://127.0.0.1:{port}/query"
os.environ["ARAX_PATHFINDER_DBS_DIR"] = os.path.join(DATA, "pathfinder")
os.environ["ARAX_DBS_DIR"] = os.path.join(DATA, "arax")
# as upstream's goldens were recorded: the KP cache always misses, stores nothing
os.environ["ARAX_KP_CACHE_ENABLED"] = "false"
os.environ["ARAX_BIOLINK_CACHE_DIR"] = os.path.join(DATA, "biolink")
os.environ["SERVER_MATURITY"] = "development"
os.makedirs(os.environ["ARAX_BIOLINK_CACHE_DIR"])
# Biolink 4.2.5 lookup map built by biolink-helper-pkg 1.0.1, so no GitHub fetch
shutil.copy(
    os.path.join(HERE, "biolink_lookup_map_4.2.5_v5.pickle"),
    os.environ["ARAX_BIOLINK_CACHE_DIR"],
)

import fixtures as F  # noqa: E402

F.write_tier0_sqlite(
    os.path.join(
        DATA, "pathfinder", "tier0-info-for-overlay_v1.0_tier0-20260621.sqlite"
    )
)
F.write_fda_pickle(os.path.join(DATA, "arax", "fda_approved_drugs_v1.0.pickle"))

from shepherd_utils.arax.ARAX_response import ARAXResponse  # noqa: E402
from shepherd_utils.arax.ARAX_messenger import ARAXMessenger  # noqa: E402
from shepherd_utils.arax.ARAX_expander import ARAXExpander  # noqa: E402
import shepherd_utils.arax.Expand.kp_selector as ks  # noqa: E402
from runner_common import run_all  # noqa: E402

ks.get_smartapi_kp_names = lambda log: set(F.OTHER_KPS)
F.patch_synonymizer_classes()

from cases import PORT_ONLY_CASES  # noqa: E402

out = run_all(ARAXResponse, ARAXMessenger, ARAXExpander, only=sys.argv[2:] or None)
out["_port_only"] = run_all(
    ARAXResponse, ARAXMessenger, ARAXExpander, cases=PORT_ONLY_CASES
)
with open(sys.argv[1], "w") as f:
    json.dump(out, f, sort_keys=True, default=str)
shutil.rmtree(DATA, ignore_errors=True)
