import copy, json, re, urllib.request, os

import fixtures as F
from runner_common_expand import (
    norm_text,
)  # noqa: F401  (expand_parity's runner_common)

MOCK = os.environ["EXPAND_PARITY_MOCK_URL"]
RESPONSE_ID = "R1"


def fetch_requests():
    with urllib.request.urlopen(MOCK + "/_requests") as r:
        return json.loads(r.read())


def norm(s):
    s = norm_text(s)
    # stored-response URLs differ by design (DEC-3)
    s = re.sub(r"It can be viewed at \S+", "It can be viewed at URL", s)
    s = re.sub(r"after \d+\.\d+(e-?\d+)?\b", "after N", s)
    return s


def record(response, exc):
    env = response.envelope.to_dict()
    envelope_id = env.pop("id", None)
    env.pop("logs", None)
    env.pop("datetime", None)
    if env.get("description"):
        env["description"] = norm(env["description"])
    qo = env.get("query_options") or {}
    if "query_plan" in qo:
        qo["query_plan"] = F.normalize_plan(qo["query_plan"])
    # attribute values stamped with the current time (e.g. FET's)
    env = json.loads(
        re.sub(
            r"\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d(\.\d+)?(\+00:00|Z)?",
            "DATETIME",
            json.dumps(env, sort_keys=True),
        )
    )
    return {
        "exception": exc,
        "status": response.status,
        "error_code": response.error_code,
        "http_status": getattr(response, "http_status", None),
        "message": norm(response.message or ""),
        "envelope": env,
        "envelope_id": envelope_id,
        "logs": [
            (x["level"], x.get("code"), norm(x["message"]))
            for x in response.messages
            if x["level"] != "DEBUG"
        ],
        "requests": fetch_requests(),
    }


def run_all(new_query, cases):
    """new_query() -> a fresh ARAXQuery set up to store under RESPONSE_ID."""
    out = {}
    fetch_requests()  # clear
    for name, query in cases:
        araxq = new_query()
        try:
            araxq.query(copy.deepcopy(query))
            exc = None
        except Exception as e:  # record, don't stop the run
            exc = f"{type(e).__name__}: {norm_text(str(e))}"
        out[name] = record(araxq.response, exc)
    return out
