"""Run the meta-KG / autocomplete parity cases against the port. Usage: python run_port.py OUT.json"""

import json, os, shutil, sys, tempfile, warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..", "..")))
DATA = tempfile.mkdtemp(prefix="aux_parity_")
os.environ["ARAX_DBS_DIR"] = DATA

import shepherd_utils.arax.KnowledgeSources.knowledge_source_metadata as ksm_mod  # noqa: E402
import shepherd_utils.arax.KnowledgeSources.meta_kg_background_refresh as refresh_mod  # noqa: E402
from aux_cases import write_terms_db  # noqa: E402
from aux_runner import run_autocomplete, run_meta_kg  # noqa: E402

FETCH = {}
ksm_mod.KnowledgeSourceMetadata._fetch_retriever_meta_kg = lambda self: FETCH["kg"]


def set_fetch(kg):
    FETCH["kg"] = kg


def clear_backups():
    for f in os.listdir(DATA):
        if f.startswith("meta_kg_"):
            os.remove(os.path.join(DATA, f))


out = {
    "meta_kg": run_meta_kg(
        ksm_mod.KnowledgeSourceMetadata,
        set_fetch,
        clear_backups,
        refresh_mod.refresh_meta_kg,
    )
}

from shepherd_utils.arax.autocomplete import rtxcomplete  # noqa: E402

write_terms_db(rtxcomplete.RTXConfig.autocomplete_path)
out["autocomplete"] = run_autocomplete(rtxcomplete)
with open(sys.argv[1], "w") as f:
    json.dump(out, f, sort_keys=True)
shutil.rmtree(DATA, ignore_errors=True)
