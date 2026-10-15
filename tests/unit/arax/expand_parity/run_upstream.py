"""Regenerate goldens.json.gz by running the same cases against upstream ARAX.

Usage: PYTHONHASHSEED=0 RTX_CODE=/path/to/RTX/code python run_upstream.py
Needs an RTX checkout at the commit pinned in shepherd_utils/arax/README.md,
ARAX's Python requirements, a dummy code/config_secrets_local.json and
code/maturity_override.txt ("development") so RTXConfiguration starts offline.
"""

import gzip, json, os, sys, warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
RTX = os.environ["RTX_CODE"]
sys.path.insert(0, HERE)
for p in ("", "ARAX/ARAXQuery", "ARAX/ARAXQuery/Expand"):
    sys.path.insert(0, os.path.join(RTX, p))

import mock_retriever  # noqa: E402

os.environ["EXPAND_PARITY_MOCK_URL"] = f"http://127.0.0.1:{mock_retriever.start()}"
import fixtures as F  # noqa: E402

F.write_tier0_sqlite(
    os.path.join(
        RTX,
        "ARAX",
        "KnowledgeSources",
        "KG2c",
        "tier0-info-for-overlay_v1.0_tier0-20260621.sqlite",
    )
)
F.write_fda_pickle(
    os.path.join(RTX, "ARAX", "KnowledgeSources", "fda_approved_drugs_v1.0.pickle")
)
from ARAX_response import ARAXResponse  # noqa: E402

ARAXResponse.output = None
from ARAX_messenger import ARAXMessenger  # noqa: E402
from ARAX_expander import ARAXExpander  # noqa: E402
from biolink_helper import get_biolink_helper  # noqa: E402
import universe as U  # noqa: E402
from runner_common import MOCK, run_all  # noqa: E402

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
for kp in F.OTHER_KPS:
    META[kp] = {
        "predicates": {"biolink:Nothing": {"biolink:Nothing": {"biolink:nothing"}}},
        "prefixes": {},
    }
URLS = {"infores:retriever": MOCK, **{kp: MOCK + "/unused" for kp in F.OTHER_KPS}}
for mod in list(sys.modules.values()):
    cls = getattr(mod, "KPSelector", None)
    if isinstance(cls, type):
        cls._load_cached_kp_info = lambda self: (META, dict(URLS), set(), set())
F.patch_synonymizer_classes()

out = run_all(ARAXResponse, ARAXMessenger, ARAXExpander)
with gzip.open(os.path.join(HERE, "goldens.json.gz"), "wt") as f:
    json.dump(out, f, sort_keys=True, default=str)
print("wrote goldens for", len(out), "cases")
