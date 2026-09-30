"""Run the /response parity cases against the port. Usage: python run_port.py OUT.json"""

import asyncio, json, os, sys, warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..", "..")))

from shepherd_utils.arax.ResponseCache import response_lookup as rl  # noqa: E402
from response_runner import ARS_HOST, FakeValidator, run_all  # noqa: E402

rl._TRAPIResponseValidator = FakeValidator

STATE = {}
CACHE = {}
rl.component_cache_put = lambda key, obj: CACHE.__setitem__(
    key, json.loads(json.dumps(obj))
)
rl.component_cache_get = lambda key: CACHE.get(key)


def fake_get_message_sync(response_id):
    return json.loads(json.dumps(STATE["local"][response_id][1]))


import shepherd_utils.db as db  # noqa: E402

db.get_message_sync = lambda i: (
    fake_get_message_sync(i) if i in STATE.get("local", {}) else {}[i]
)


async def fetch_url(url):
    return STATE["urls"][url]


async def fetch_ars(pk, trace):
    return STATE.get("ars", {}).get((pk, trace), (404, b"Not found"))


async def run(fn, *args):
    return fn(*args)


def setup(data):
    CACHE.clear()
    STATE.clear()
    STATE.update(data)


def lookup(response_id):
    return asyncio.run(
        rl.get_response(
            response_id,
            fetch_url=fetch_url,
            fetch_ars=fetch_ars,
            ars_host=ARS_HOST,
            run=run,
        )
    )


out = run_all(setup, lookup)
with open(sys.argv[1], "w") as f:
    json.dump(out, f, sort_keys=True)
