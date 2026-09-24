"""Regenerate goldens.json.gz from upstream ARAX's ResponseCache.get_response.

Usage: RTX_CODE=/path/to/RTX/code python run_upstream.py
flask and boto3 are stubbed (not reached); MySQL is replaced by a fake session
and a local response is served from data/responses_1_0/{id}.json, upstream's
first place to look; requests.get answers from the case data (ARS messages at
ars-prod, the first ARS host upstream tries).
"""

import gzip, json, os, sys, types, warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
RTX = os.environ["RTX_CODE"]
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(RTX, "ARAX", "ResponseCache"))
flask = types.ModuleType("flask")
flask.Flask, flask.redirect = object, None
sys.modules["flask"] = flask
sys.modules["boto3"] = types.ModuleType("boto3")

import response_cache as rc  # noqa: E402
from response_runner import ARS_HOST, FakeValidator, run_all  # noqa: E402

rc.TRAPIResponseValidator = FakeValidator

RESPONSES_DIR = os.path.join(RTX, "..", "data", "responses_1_0")
os.makedirs(RESPONSES_DIR, exist_ok=True)
STATE = {}


class FakeHTTP:
    def __init__(self, status_code, content):
        self.status_code, self.content = status_code, content

    def json(self):
        return json.loads(self.content)


def fake_get(url, headers=None, timeout=None):
    prefix = f"https://{ARS_HOST}/ars/api/messages/"
    if url.startswith(prefix):
        pk, _, query = url[len(prefix) :].partition("?")
        return FakeHTTP(
            *STATE.get("ars", {}).get((pk, query == "trace=y"), (404, b"Not found"))
        )
    if "/ars/api/messages/" in url:
        return FakeHTTP(404, b"Not found")
    return FakeHTTP(*STATE["urls"][url])


rc.requests.get = fake_get


class FakeQuery:
    def __init__(self, ids):
        self.ids = ids

    def filter(self, cond):
        self.wanted = cond.right.value
        return self

    def first(self):
        return (
            types.SimpleNamespace(response_id=self.wanted)
            if self.wanted in self.ids
            else None
        )


def fake_init(self):
    self.rtxConfig = None
    ids = {i for i, _ in STATE.get("local", {}).values()}
    self.session = types.SimpleNamespace(query=lambda model: FakeQuery(ids))


rc.ResponseCache.__init__ = fake_init
rc.ResponseCache.__del__ = lambda self: None
LOCAL_IDS = {}


def setup(data):
    # upstream caches every child ARS response it finishes (json_cache/);
    # each case starts with it empty
    for name in os.listdir(rc.component_cache_dir):
        os.remove(os.path.join(rc.component_cache_dir, name))
    STATE.clear()
    STATE.update(data)
    for port_id, (arax_id, envelope) in data.get("local", {}).items():
        LOCAL_IDS[port_id] = arax_id
        with open(os.path.join(RESPONSES_DIR, f"{arax_id}.json"), "w") as f:
            json.dump(envelope, f)


def lookup(response_id):
    # upstream's local ids are integers; the port's are Shepherd response ids
    if not response_id.startswith("LOCAL"):
        return rc.ResponseCache().get_response(response_id)
    arax_id = str(LOCAL_IDS.get(response_id, 99999))
    result = rc.ResponseCache().get_response(arax_id)
    # ...so a not-found message names the id the port was asked for
    return json.loads(
        json.dumps(result).replace(
            f"response_id={arax_id}", f"response_id={response_id}"
        )
    )


out = run_all(setup, lookup)
with gzip.open(os.path.join(HERE, "goldens.json.gz"), "wt") as f:
    json.dump(out, f, sort_keys=True)
print("wrote goldens for", len(out), "cases")
