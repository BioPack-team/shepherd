"""Regenerate goldens.json.gz by running the cases through upstream ARAXQuery.

Usage: PYTHONHASHSEED=0 RTX_CODE=/path/to/RTX/code python run_upstream.py
Needs the same RTX checkout and offline config as expand_parity/run_upstream.py.
ARAX's query tracker and response store (MySQL/S3) are replaced by stubs, as
is the legacy creativeCRG module (dead code, not exercised by any case), and
ARAX's KP cache always misses.
"""

import gzip, json, os, sys, types, warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
RTX = os.environ["RTX_CODE"]
sys.path.insert(0, HERE)
for p in ("", "ARAX/ARAXQuery", "ARAX/ARAXQuery/Expand"):
    sys.path.insert(0, os.path.join(RTX, p))

import setup_common as S  # noqa: E402

KS = os.path.join(RTX, "ARAX", "KnowledgeSources")
S.write_data(
    os.path.join(KS, "KG2c"),
    KS,
    os.path.join(KS, "NormalizedGoogleDistance"),
    os.path.join(KS, "COHD_local", "data"),
    os.path.join(KS, "Prediction"),
    os.path.join(KS, "NormalizedGoogleDistance"),
)

from query_runner import RESPONSE_ID, run_all  # noqa: E402


class StubTracker:
    def __init__(self, *a, **k):
        pass

    def create_tracker_entry(self, attributes=None):
        return None

    def update_tracker_entry(self, *a, **k):
        return None

    def alter_tracker_entry(self, *a, **k):
        return None


class StubResponseCache:
    """ARAX's add_new_response minus MySQL/S3: same envelope.id, fixed id."""

    def __init__(self, *a, **k):
        pass

    def connect(self):
        pass

    def add_new_response(self, response):
        response.debug("Writing response record to MySQL")
        response.envelope.id = f"https://localhost/api/arax/v1.4/response/{RESPONSE_ID}"
        return RESPONSE_ID

    def get_response(self, response_id):
        return None


for name, attr, obj in (
    ("ARAX_query_tracker", "ARAXQueryTracker", StubTracker),
    ("response_cache", "ResponseCache", StubResponseCache),
    ("creativeCRG", "creativeCRG", type("creativeCRG", (), {})),
):
    m = types.ModuleType(name)
    setattr(m, attr, obj)
    sys.modules[name] = m

from ARAX_response import ARAXResponse  # noqa: E402
from ARAX_query import ARAXQuery  # noqa: E402

ARAXResponse.output = None
sys.path.insert(0, os.path.join(KS, "COHD_local", "scripts"))
import COHDIndex  # noqa: E402,F401
from biolink_helper import get_biolink_helper  # noqa: E402
import universe as U  # noqa: E402
from query_runner import MOCK  # noqa: E402

# The same KP-info stand-in as expand_parity/run_upstream.py
bh = get_biolink_helper()
cats = set(bh.get_descendants("biolink:NamedThing", include_mixins=True)) | set(
    bh.get_descendants("biolink:NamedThing", include_mixins=False)
)
META = {
    "infores:retriever": {
        "predicates": {
            "biolink:NamedThing": {"biolink:NamedThing": {"biolink:related_to"}}
        },
        "prefixes": {c: set(U.PREFIXES) for c in cats},
    }
}
for kp in S.F.OTHER_KPS:
    META[kp] = {
        "predicates": {"biolink:Nothing": {"biolink:Nothing": {"biolink:nothing"}}},
        "prefixes": {},
    }
URLS = {"infores:retriever": MOCK, **{kp: MOCK + "/unused" for kp in S.F.OTHER_KPS}}
for mod in list(sys.modules.values()):
    cls = getattr(mod, "KPSelector", None)
    if isinstance(cls, type):
        cls._load_cached_kp_info = lambda self: (META, dict(URLS), set(), set())
S.F.patch_synonymizer_classes()
assert S.patch_cohd_api()
sys.path.insert(0, os.path.join(RTX, "reasoningtool", "kg-construction"))
import NormGoogleDistance  # noqa: E402,F401

assert S.patch_eutils()

# The KP cache always misses and stores nothing (the port runs with
# ARAX_KP_CACHE_ENABLED=false), so the goldens don't depend on what an earlier
# run left in ARAX's cache database.
# ARAX imports the cacher under two module names (trapi_query_cacher from
# Expand's querier, Expand.trapi_query_cacher from the expander and Connect), so
# every loaded copy of the class is patched.
import trapi_query_cacher as tqc  # noqa: E402
import Expand.trapi_query_cacher  # noqa: E402,F401

n_cachers = 0
for mod in list(sys.modules.values()):
    cls = getattr(mod, "KPQueryCacher", None)
    if isinstance(cls, type):
        cls.get_cached_result = lambda self, *a, **k: (
            None,
            tqc.NO_CACHED_RESPONSE,
            0.0,
            None,
        )
        cls.store_response = lambda self, *a, **k: None
        n_cachers += 1
assert n_cachers >= 2, n_cachers

from query_cases import CASES  # noqa: E402

out = run_all(lambda: ARAXQuery(), CASES)
with gzip.open(os.path.join(HERE, "goldens.json.gz"), "wt") as f:
    json.dump(out, f, sort_keys=True, default=str)
print("wrote goldens for", len(out), "cases")
