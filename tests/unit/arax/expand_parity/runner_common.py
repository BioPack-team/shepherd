import copy, json, re, urllib.request
import fixtures as F
from cases import CASES

import os

MOCK = os.environ.get("EXPAND_PARITY_MOCK_URL", "http://127.0.0.1:18777")


def fetch_requests():
    with urllib.request.urlopen(MOCK + "/_requests") as r:
        return json.loads(r.read())


def norm_text(s):
    s = re.sub(r"127\.0\.0\.1:\d+", "127.0.0.1:PORT", s)
    s = re.sub(r'File "[^"]*/(ARAX_[a-z_]+\.py|[a-z_]+\.py)"', r'File "\1"', s)
    s = re.sub(r", line \d+,", ", line N,", s)
    s = re.sub(r"\d+\.\d+ seconds", "N seconds", s)
    s = re.sub(r"after \d+(\.\d+)? seconds", "after N seconds", s)
    s = re.sub(r"in \d+(\.\d+)? seconds", "in N seconds", s)
    return s


def inject_query_options(params, qo):
    # What ARAXQuery.execute_processing_plan does before calling expand (ORC-04)
    params = dict(params)
    for name in ("kp_timeout", "prune_threshold"):
        if name in qo and name not in params:
            params[name] = int(qo[name])
    params["return_minimal_metadata"] = qo.get("return_minimal_metadata") is True
    return params


def run_all(ARAXResponse, ARAXMessenger, ARAXExpander, only=None, cases=CASES):
    out = {}
    fetch_requests()  # clear
    for name, qg, params, qo in cases:
        if only and name not in only:
            continue
        response = ARAXResponse()
        messenger = ARAXMessenger()
        messenger.create_envelope(response)
        response.envelope.message = messenger.from_dict(
            {"query_graph": copy.deepcopy(qg)}
        )
        response.envelope.query_options = dict(qo, bypass_cache=True)
        try:
            ARAXExpander().apply(response, inject_query_options(params, qo))
            exc = None
        except Exception as e:  # record, don't stop the run
            exc = f"{type(e).__name__}: {e}"
        m = response.envelope.message
        kg = m.knowledge_graph
        rec = {
            "exception": exc,
            "status": response.status,
            "error_code": response.error_code,
            "qg": m.query_graph.to_dict() if m.query_graph else None,
            "qg_filled": (
                {
                    k: getattr(e, "filled", None)
                    for k, e in (m.query_graph.edges or {}).items()
                }
                if m.query_graph and hasattr(m.query_graph, "edges")
                else None
            ),
            "kg": kg.to_dict() if kg else None,
            "node_qnode_keys": (
                {
                    k: sorted(getattr(n, "qnode_keys", None) or [])
                    for k, n in (kg.nodes or {}).items()
                }
                if kg
                else None
            ),
            "node_query_ids": (
                {
                    k: sorted(getattr(n, "query_ids", None) or [])
                    for k, n in (kg.nodes or {}).items()
                }
                if kg
                else None
            ),
            "edge_qedge_keys": (
                {
                    k: sorted(getattr(e, "qedge_keys", None) or [])
                    for k, e in (kg.edges or {}).items()
                }
                if kg
                else None
            ),
            "aux": {k: v.to_dict() for k, v in (m.auxiliary_graphs or {}).items()},
            "kryptonite": {
                k: {q: sorted(v) for q, v in d.items()}
                for k, d in getattr(m, "encountered_kryptonite_edges_info", {}).items()
            },
            "plan": F.normalize_plan(response.query_plan),
            "logs": [
                (x["level"], norm_text(x["message"]))
                for x in response.messages
                if x["level"] != "DEBUG"
            ],
            "requests": fetch_requests(),
        }
        out[name] = rec
    return out
