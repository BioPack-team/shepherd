"""Regenerate goldens.json.gz from upstream's knowledge_source_metadata,
meta_kg_background_refresh and autocomplete/rtxcomplete.

Usage: RTX_CODE=/path/to/RTX/code python run_upstream.py
PloverDB is replaced by the case's base meta-KG and the KP info cache is
absent (so upstream's merge step leaves it unchanged), which is what the port's
Retriever fetch returns (DEC-11).
"""

import glob, gzip, json, os, sys, warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
RTX = os.environ["RTX_CODE"]
sys.path.insert(0, HERE)
for p in (
    "",
    "ARAX/ARAXQuery",
    "ARAX/ARAXQuery/Expand",
    "ARAX/KnowledgeSources",
    "ARAX/NodeSynonymizer",
    "autocomplete",
):
    sys.path.insert(0, os.path.join(RTX, p))

import knowledge_source_metadata as ksm_mod  # noqa: E402
import meta_kg_background_refresh as refresh_mod  # noqa: E402
import kp_info_cacher  # noqa: E402
from aux_cases import write_terms_db  # noqa: E402
from aux_runner import run_autocomplete, run_meta_kg  # noqa: E402

kp_info_cacher.KPInfoCacher.cache_file_present = lambda self: False
KS_DIR = os.path.join(RTX, "ARAX", "KnowledgeSources")
FETCH = {}
ksm_mod.KnowledgeSourceMetadata._fetch_ploverdb_meta_kg = lambda self: FETCH["kg"]


def set_fetch(kg):
    FETCH["kg"] = kg


def clear_backups():
    for f in glob.glob(os.path.join(KS_DIR, "meta_kg_*.json")):
        os.remove(f)


out = {
    "meta_kg": run_meta_kg(
        ksm_mod.KnowledgeSourceMetadata,
        set_fetch,
        clear_backups,
        refresh_mod.refresh_meta_kg,
    )
}
clear_backups()

from RTXConfiguration import RTXConfiguration  # noqa: E402

AC_DIR = os.path.join(RTX, "autocomplete")
write_terms_db(
    os.path.join(AC_DIR, RTXConfiguration().autocomplete_path.split("/")[-1])
)
cache = os.path.join(AC_DIR, "rtxcomplete_cache.sqlite")
if os.path.exists(cache):
    os.remove(cache)
import rtxcomplete  # noqa: E402

out["autocomplete"] = run_autocomplete(rtxcomplete)
with gzip.open(os.path.join(HERE, "goldens.json.gz"), "wt") as f:
    json.dump(out, f, sort_keys=True)
print("wrote goldens")
