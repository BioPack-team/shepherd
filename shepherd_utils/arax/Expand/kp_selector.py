# Ported from RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/Expand/kp_selector.py.
# Changes from upstream (the class keeps its interface; the internals follow
# docs/ARAX_PORT_BASELINE.md DEC-4, DEC-9, DEC-10 and DEC-12):
#   - no KPInfoCacher / meta-KG pickle: queries go only to infores:retriever
#     (DEC-4), so there is no per-KP meta map to load
#   - kp_urls holds only infores:retriever, at settings.sync_kg_retrieval_url
#   - valid_kps = the KPs SmartAPI lists for TRAPI 1.6.0 at this maturity (the
#     same filter ARAX's KPInfoCacher applies), plus infores:retriever. It is
#     used only to fill the query plan the ARAX UI shows (DEC-12), fetched with
#     ARAX's own SmartAPI client and cached in-process for an hour (ARAX's
#     background refresh interval)
#   - get_kps_for_single_hop_qg always returns {infores:retriever} and marks
#     every other valid KP "Skipped" in the query plan (DEC-12)
#   - kp_accepts_single_hop_qg accepts, as ARAX already does for
#     infores:retriever (KPS_SKIP_METAKG_CHECKS)
#   - make_qg_use_supported_prefixes returns the query graph unchanged: no
#     curie-prefix conversion or canonicalization (DEC-9)
#   - the supported-prefix / meta-map helpers are dropped, as nothing calls them
#   - main() is dropped
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
import threading
import time
from typing import Optional

from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.BiolinkHelper.biolink_helper import get_biolink_helper
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph
from shepherd_utils.arax.RTXConfiguration import RTXConfiguration
from shepherd_utils.config import settings


RTX_CONFIG = RTXConfiguration()
KPS_SKIP_METAKG_CHECKS = {'infores:retriever'}
RETRIEVER_INFORES = "infores:retriever"
# ARAX's KPInfoCacher.forced_kp_version: KPs are filtered on this TRAPI version
FORCED_KP_VERSION = '1.6.0'
# ARAX's background tasker refreshes the KP info caches this often
SMARTAPI_REFRESH_SEC = 3600
SKIPPED_NOT_RETRIEVER_MESSAGE = "Shepherd's ARAX sends every query to infores:retriever"

_smartapi_kps: Optional[set[str]] = None
_smartapi_kps_fetched_at = 0.0
_smartapi_lock = threading.Lock()


def _retriever_endpoint() -> str:
    """The Retriever base URL; TRAPIQuerier appends "/query" as it does for any KP."""
    url = settings.sync_kg_retrieval_url.rstrip("/")
    return url[: -len("/query")] if url.endswith("/query") else url


def get_smartapi_kp_names(log: ARAXResponse) -> set[str]:
    """KPs registered in SmartAPI for TRAPI 1.6.0 at this maturity (ARAX's filter).

    Cached for an hour. If SmartAPI can't be reached, the last good list is kept
    (as ARAX keeps its previous cache); with no previous list it is empty. Either
    way this only affects which KPs the query plan lists as Skipped.
    """
    global _smartapi_kps, _smartapi_kps_fetched_at
    with _smartapi_lock:
        fresh = time.time() - _smartapi_kps_fetched_at < SMARTAPI_REFRESH_SEC
        if _smartapi_kps is not None and fresh:
            return set(_smartapi_kps)
        try:
            from shepherd_utils.arax.Expand.smartapi import SmartAPI
            registrations = SmartAPI().get_all_trapi_kp_registrations(
                trapi_version=FORCED_KP_VERSION, req_maturity=RTX_CONFIG.maturity)
            names = {registration["infores_name"] for registration in registrations
                     if registration.get("infores_name")}
            if names or _smartapi_kps is None:
                _smartapi_kps = names
            _smartapi_kps_fetched_at = time.time()
        except Exception as e:
            log.warning(f"Could not get the KP list from SmartAPI ({type(e).__name__}: {e}); "
                        f"the query plan will list only {RETRIEVER_INFORES}")
            if _smartapi_kps is None:
                _smartapi_kps = set()
            _smartapi_kps_fetched_at = time.time()
        return set(_smartapi_kps)


class KPSelector:

    def __init__(self, kg2_mode: bool = False, log: ARAXResponse = ARAXResponse()):
        self.log = log
        self.kg2_mode = kg2_mode
        self.kp_urls = {RETRIEVER_INFORES: _retriever_endpoint()}
        self.kps_excluded_by_version: set[str] = set()
        self.kps_excluded_by_maturity: set[str] = set()
        self.valid_kps = get_smartapi_kp_names(log) | {RETRIEVER_INFORES}
        self.bh = get_biolink_helper()

    def get_kps_for_single_hop_qg(self, qg: QueryGraph) -> Optional[set[str]]:
        """
        Every one-hop query goes to infores:retriever (DEC-4). The other KPs ARAX
        would have considered are marked "Skipped" in the query plan (DEC-12).
        """
        qedge_key = next(qedge_key for qedge_key in qg.edges)
        self.log.debug(f"Selecting KPs to use for qedge {qedge_key}")
        # confirm that the qg is one hop
        if len(qg.edges) > 1:
            self.log.error(f"Query graph can only have one edge, but instead has {len(qg.edges)}.",
                           error_code="UnexpectedQG")
            return None
        for kp in self.valid_kps - {RETRIEVER_INFORES}:
            self.log.update_query_plan(qedge_key, kp, "Skipped", SKIPPED_NOT_RETRIEVER_MESSAGE)
        return {RETRIEVER_INFORES}

    def kp_accepts_single_hop_qg(self, qg: QueryGraph, kp: str) -> Optional[bool]:
        """
        This function determines whether a KP can answer a given one-hop query based on the categories/predicates
        used in the query graph. Retriever is the only KP queried, and ARAX skips meta-KG checks for it.
        """
        self.log.debug(f"Verifying that {kp} can answer this kind of one-hop query")
        # Confirm that the qg is one-hop
        if len(qg.edges) > 1:
            self.log.error(f"Query graph can only have one edge, but instead has {len(qg.edges)}.",
                           error_code="UnexpectedQG")
            return None
        return kp in KPS_SKIP_METAKG_CHECKS

    def make_qg_use_supported_prefixes(self, qg: QueryGraph, kp_name: str, log: ARAXResponse) -> Optional[QueryGraph]:
        # DEC-9: data and incoming queries are assumed normalized; no conversion.
        return qg
