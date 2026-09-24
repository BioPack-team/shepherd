"""Run the end-to-end ARAXQuery parity cases against the Shepherd port.

Usage: PYTHONHASHSEED=0 python run_port.py OUT.json [case ...]
"""

import json, os, shutil, sys, tempfile, warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..", "..")))

import setup_common as S  # noqa: E402

DATA = tempfile.mkdtemp(prefix="query_parity_")
os.environ["SYNC_KG_RETRIEVAL_URL"] = f"http://127.0.0.1:{S.PORT}/query"
os.environ["ARAX_PATHFINDER_DBS_DIR"] = os.path.join(DATA, "pathfinder")
os.environ["ARAX_DBS_DIR"] = os.path.join(DATA, "arax")
os.environ["ARAX_BIOLINK_CACHE_DIR"] = os.path.join(DATA, "biolink")
os.environ["SERVER_MATURITY"] = "development"
os.environ["SERVER_URL"] = "http://shepherd.test"
os.makedirs(os.environ["ARAX_BIOLINK_CACHE_DIR"])
shutil.copy(
    os.path.join(S.EXPAND, "biolink_lookup_map_4.2.5_v5.pickle"),
    os.environ["ARAX_BIOLINK_CACHE_DIR"],
)
arax = os.path.join(DATA, "arax")
S.write_data(os.path.join(DATA, "pathfinder"), arax, arax, arax, arax)

from shepherd_utils.arax.ARAX_query import ARAXQuery  # noqa: E402
import shepherd_utils.arax.Expand.kp_selector as ks  # noqa: E402
import shepherd_utils.arax.KnowledgeSources.COHD_local.scripts.COHDIndex  # noqa: E402,F401
from query_runner import RESPONSE_ID, run_all  # noqa: E402
from query_cases import CASES  # noqa: E402

ks.get_smartapi_kp_names = lambda log: set(S.F.OTHER_KPS)
S.F.patch_synonymizer_classes()
assert S.patch_cohd_api()

only = sys.argv[2:]
out = run_all(
    lambda: ARAXQuery(response_id=RESPONSE_ID),
    [c for c in CASES if not only or c[0] in only],
)
with open(sys.argv[1], "w") as f:
    json.dump(out, f, sort_keys=True, default=str)
shutil.rmtree(DATA, ignore_errors=True)
