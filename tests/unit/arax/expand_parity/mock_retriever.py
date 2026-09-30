"""Mock Retriever: answers one-hop / single-node TRAPI queries from the universe."""

import json, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import universe as U

NODES, EDGES, AUX = U.build()
REQUESTS = []
LOCK = threading.Lock()
ALL_CATS = {
    "biolink:NamedThing",
    "biolink:BiologicalEntity",
    "biolink:ChemicalEntity",
    "biolink:DiseaseOrPhenotypicFeature",
    "biolink:Protein",
    "biolink:GeneOrGeneProduct",
}


def cat_ok(node_id, qnode):
    cats = qnode.get("categories")
    if not cats:
        return True
    node_cat = NODES[node_id]["categories"][0]
    if node_cat in cats or ALL_CATS.intersection(cats) and "biolink:NamedThing" in cats:
        return True
    if node_cat == "biolink:SmallMolecule" and "biolink:ChemicalEntity" in cats:
        return True
    if node_cat == "biolink:Gene" and (
        "biolink:Protein" in cats or "biolink:GeneOrGeneProduct" in cats
    ):
        return True
    if (
        node_cat in ("biolink:Disease", "biolink:PhenotypicFeature")
        and "biolink:DiseaseOrPhenotypicFeature" in cats
    ):
        return True
    return False


def match_node(node_id, qnode):
    """Return the query_id the node fulfils (or node_id), or None."""
    ids = qnode.get("ids")
    if ids:
        if node_id in ids:
            return node_id
        parent = U.SUBCLASS.get(node_id)
        if parent in ids:
            return parent
        return None
    return node_id if cat_ok(node_id, qnode) else None


def pred_ok(pred, qpreds):
    if not qpreds or "biolink:related_to" in qpreds:
        return True
    if pred in qpreds:
        return True
    return bool(U.PRED_PARENTS.get(pred, set()).intersection(qpreds))


def answer(body):
    qg = body["message"]["query_graph"]
    qnodes, qedges = qg["nodes"], qg.get("edges", {})
    all_ids = [i for q in qnodes.values() for i in (q.get("ids") or [])]
    if U.ERROR_CURIE in all_ids:
        return 500, {"detail": "boom"}
    if U.SLOW_CURIE in all_ids:
        time.sleep(3)
    kg_nodes, kg_edges, results, aux_used = {}, {}, [], {}
    if not qedges:
        ((qk, qn),) = qnodes.items()
        for nid in sorted(qn.get("ids") or []):
            if nid in NODES:
                kg_nodes[nid] = NODES[nid]
                results.append(
                    {
                        "node_bindings": {qk: [{"id": nid}]},
                        "analyses": [
                            {"resource_id": "infores:retriever", "edge_bindings": {}}
                        ],
                    }
                )
    elif len(qedges) > 1:
        return 200, {"message": answer_multi_hop(qg)}
    else:
        ((ek, qe),) = qedges.items()
        sq, oq = qnodes[qe["subject"]], qnodes[qe["object"]]
        for eid in sorted(EDGES):
            e = EDGES[eid]
            if eid.startswith("support") or not pred_ok(
                e["predicate"], qe.get("predicates")
            ):
                continue
            orientations = [(e["subject"], e["object"])]
            if e["predicate"] in U.SYMMETRIC:
                orientations.append((e["object"], e["subject"]))
            for s, o in orientations:
                sm, om = match_node(s, sq), match_node(o, oq)
                if sm is None or om is None:
                    continue
                kg_edges[eid] = e
                kg_nodes[s] = NODES[s]
                kg_nodes[o] = NODES[o]
                sb = {"id": s}
                ob = {"id": o}
                if sm != s:
                    sb["query_id"] = sm
                if om != o:
                    ob["query_id"] = om
                results.append(
                    {
                        "node_bindings": {qe["subject"]: [sb], qe["object"]: [ob]},
                        "analyses": [
                            {
                                "resource_id": "infores:retriever",
                                "edge_bindings": {ek: [{"id": eid}]},
                            }
                        ],
                    }
                )
                for a in e["attributes"]:
                    if a["attribute_type_id"] == "biolink:support_graphs":
                        for ag in a["value"]:
                            aux_used[ag] = AUX[ag]
                            for sek in AUX[ag]["edges"]:
                                if sek in EDGES:
                                    kg_edges[sek] = EDGES[sek]
                break
    msg = {
        "query_graph": qg,
        "knowledge_graph": {"nodes": kg_nodes, "edges": kg_edges},
        "results": results,
    }
    if aux_used:
        msg["auxiliary_graphs"] = aux_used
    return 200, {"message": msg}


def edge_matches(qe, sq, oq):
    """(edge id, subject id, object id) for every universe edge fulfilling qe."""
    out = []
    for eid in sorted(EDGES):
        e = EDGES[eid]
        if eid.startswith("support") or not pred_ok(
            e["predicate"], qe.get("predicates")
        ):
            continue
        orientations = [(e["subject"], e["object"])]
        if e["predicate"] in U.SYMMETRIC:
            orientations.append((e["object"], e["subject"]))
        for s, o in orientations:
            if match_node(s, sq) is not None and match_node(o, oq) is not None:
                out.append((eid, s, o))
                break
    return out


def answer_multi_hop(qg, max_results=200):
    """A multi-edge (e.g. xCRG's two-hop) query: each qedge's matches joined on
    the qnodes they share, in a fixed order."""
    qnodes, qedges = qg["nodes"], qg["edges"]
    rows = [{}]  # partial results: qnode -> node id, plus "_edges"
    for ek in sorted(qedges):
        qe = qedges[ek]
        matches = edge_matches(qe, qnodes[qe["subject"]], qnodes[qe["object"]])
        new_rows = []
        for row in rows:
            for eid, s, o in matches:
                if row.get(qe["subject"], s) != s or row.get(qe["object"], o) != o:
                    continue
                r = dict(row)
                r[qe["subject"]], r[qe["object"]] = s, o
                r["_edges"] = {**row.get("_edges", {}), ek: eid}
                new_rows.append(r)
        rows = new_rows[:max_results]
    kg_nodes, kg_edges, results = {}, {}, []
    for row in rows:
        for qk in qnodes:
            kg_nodes[row[qk]] = NODES[row[qk]]
        for eid in row["_edges"].values():
            kg_edges[eid] = EDGES[eid]
        results.append(
            {
                "node_bindings": {qk: [{"id": row[qk]}] for qk in sorted(qnodes)},
                "analyses": [
                    {
                        "resource_id": "infores:retriever",
                        "edge_bindings": {
                            ek: [{"id": eid}]
                            for ek, eid in sorted(row["_edges"].items())
                        },
                    }
                ],
            }
        )
    return {
        "query_graph": qg,
        "knowledge_graph": {"nodes": kg_nodes, "edges": kg_edges},
        "results": results,
    }


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with LOCK:
            REQUESTS.append({"path": self.path, "body": body})
        code, resp = answer(body)
        data = json.dumps(resp, allow_nan=True).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        with LOCK:
            data = json.dumps(REQUESTS).encode()
            REQUESTS.clear()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def start(port=0):
    """Serve in a daemon thread; returns the bound port (0 picks a free one)."""
    server = ThreadingHTTPServer(("127.0.0.1", port), H)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
