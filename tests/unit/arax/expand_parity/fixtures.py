"""Shared stubs + data files so upstream ARAX and the port run identically offline."""

import json, os, pickle, sqlite3, sys, types
import universe as U

NODES, EDGES, AUX = U.build()
FDA_APPROVED = {"CHEBI:1", "CHEBI:2", "CHEBI:3", "CHEBI:5", "CHEBI:20"}
OTHER_KPS = {"infores:rtx-kg2", "infores:spoke"}


def fake_canonical(
    curies=None, names=None, return_all_categories=False, debug=False, **kw
):
    out = {}
    by_name = {n["name"]: k for k, n in NODES.items()}
    for c in (curies if isinstance(curies, list) else ([curies] if curies else [])):
        if c in by_name:  # a name (NameRes): its node
            k = by_name[c]
            info = {
                "preferred_curie": k,
                "preferred_name": c,
                "preferred_category": U.CATS[k.split(":")[0]],
            }
            if return_all_categories:
                info["all_categories"] = {info["preferred_category"]: 1}
            out[c] = info
            continue
        p = c.split(":")[0]
        if p in U.CATS:
            info = {
                "preferred_curie": c,
                "preferred_name": NODES.get(c, {}).get("name", c),
                "preferred_category": U.CATS[p],
            }
            if return_all_categories:
                info["all_categories"] = {U.CATS[p]: 1}
            out[c] = info
        else:
            out[c] = None
    return out


def fake_equivalents(curies=None, names=None, include_unrecognized_entities=True, **kw):
    return {c: [c] for c in (curies if isinstance(curies, list) else [curies])}


def fake_names(curies, **kw):
    curies = curies if isinstance(curies, list) else [curies]
    return {c: NODES.get(c, {}).get("name", c) for c in curies}


def patch_synonymizer_classes():
    """Patch every NodeSynonymizer class object loaded (upstream imports it under two module names)."""
    n = 0
    for mod in list(sys.modules.values()):
        cls = getattr(mod, "NodeSynonymizer", None)
        if isinstance(cls, type) and not getattr(cls, "_patched", False):
            cls.__init__ = lambda self, *a, **k: None
            cls.get_canonical_curies = lambda self, *a, **k: fake_canonical(*a, **k)
            cls.get_equivalent_nodes = lambda self, *a, **k: fake_equivalents(*a, **k)
            cls.get_curie_names = lambda self, *a, **k: fake_names(*a, **k)
            cls._patched = True
            n += 1
    return n


def write_tier0_sqlite(path):
    """neighbors(id, neighbor_counts) + category_counts(category, count) for FET."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)
    counts = {}
    for e in EDGES.values():
        for a, b in ((e["subject"], e["object"]), (e["object"], e["subject"])):
            cat = NODES[b]["categories"][0]
            counts.setdefault(a, {}).setdefault(cat, 0)
            counts[a][cat] += 1
            counts[a].setdefault("biolink:NamedThing", 0)
            counts[a]["biolink:NamedThing"] += 1
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE neighbors (id TEXT, neighbor_counts TEXT)")
    con.execute("CREATE TABLE category_counts (category TEXT, count INT)")
    con.executemany(
        "INSERT INTO neighbors VALUES (?,?)",
        [(k, json.dumps(v)) for k, v in counts.items()],
    )
    cat_counts = {}
    for n in NODES.values():
        cat_counts[n["categories"][0]] = cat_counts.get(n["categories"][0], 0) + 1
    cat_counts["biolink:NamedThing"] = len(NODES)
    con.executemany(
        "INSERT INTO category_counts VALUES (?,?)", list(cat_counts.items())
    )
    con.commit()
    con.close()


def write_fda_pickle(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(FDA_APPROVED, f)


def normalize_plan(plan, retriever="infores:retriever"):
    """Query plan: full detail for edge_properties and Retriever (timings stripped); status only for others."""
    import re

    out = {}
    for qedge_key, entries in plan.get("qedge_keys", {}).items():
        out[qedge_key] = {}
        for kp, v in entries.items():
            if kp == "edge_properties":
                out[qedge_key][kp] = v
            elif kp == retriever:
                out[qedge_key][kp] = {
                    "status": v["status"],
                    "description": re.sub(
                        r"127\.0\.0\.1:\d+",
                        "127.0.0.1:PORT",
                        re.sub(r"\d+\.\d+ seconds", "N seconds", v["description"]),
                    ),
                    "query": v.get("query"),
                }
            else:
                out[qedge_key][kp] = {"status": v["status"]}
    return out
