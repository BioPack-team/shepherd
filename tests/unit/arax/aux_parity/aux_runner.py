"""Shared by run_port.py / run_upstream.py."""

import copy
import json

from aux_cases import AUTOCOMPLETE_STEPS, META_KG_CASES


def run_meta_kg(ksm_class, set_fetch, clear_backups, refresh):
    out = {}
    for name, steps in META_KG_CASES:
        clear_backups()
        ksm_class.cached_meta_knowledge_graph = None
        ksm_class.cached_simplified_meta_knowledge_graph = None
        ksm_class.cache_timestamp = None
        results = []
        for fetch, formats in steps:
            if fetch == "REFRESH":
                # the hourly refresh: drop the cache so the next request rebuilds
                ksm_class.cached_meta_knowledge_graph = None
                ksm_class.cache_timestamp = None
                continue
            set_fetch(copy.deepcopy(fetch))
            for format_ in formats:
                results.append(
                    json.loads(
                        json.dumps(
                            ksm_class().get_meta_knowledge_graph(format_=format_)
                        )
                    )
                )
        out[name] = results
    # the background refresh function (meta_kg_background_refresh.refresh_meta_kg)
    clear_backups()
    set_fetch(copy.deepcopy(META_KG_CASES[0][1][0][0]))
    out["_refresh"] = [
        refresh(),
        json.loads(json.dumps(ksm_class.cached_simplified_meta_knowledge_graph)),
    ]
    return out


def run_autocomplete(rtxcomplete):
    rtxcomplete.load()
    return [
        rtxcomplete.get_nodes_like(word, limit) for word, limit in AUTOCOMPLETE_STEPS
    ]
