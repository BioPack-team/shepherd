"""Shared by run_port.py / run_upstream.py: run every case, normalize, record."""

import json
import re

from response_cases import CASES

ARS_HOST = "ars-prod.transltr.io"


class FakeValidator:
    """Stands in for reasoner-validator's TRAPIResponseValidator on both sides.

    The package is the same version for the port and upstream, and it fetches
    schemas and the Biolink model over the network, so what is compared is
    ARAX's handling of its output. The messages depend on the envelope, so the
    PASS / ERROR / FAIL / crash paths are all reached.
    """

    def __init__(self, trapi_version=None, biolink_version=None):
        self.args = (trapi_version, biolink_version)
        self.messages = {
            "info": {},
            "skipped": {},
            "warning": {},
            "error": {},
            "critical": {},
        }

    def check_compliance_of_trapi_response(self, envelope):
        message = envelope.get("message") or {}
        if "boom" in json.dumps(envelope):
            raise ValueError("validator exploded")
        if not message.get("query_graph"):
            self.messages["critical"][
                "critical.trapi.response.message.query_graph.missing"
            ] = {}
        if not message.get("results"):
            self.messages["error"]["error.trapi.response.message.results.empty"] = {}
        if envelope.get("schema_version") is None:
            self.messages["warning"][
                "warning.trapi.response.schema_version.missing"
            ] = {}
        self.messages["info"][f"info.checked.{self.args[0]}.{self.args[1]}"] = {}

    def get_all_messages(self):
        return {"Validate TRAPI Response": {"Standards Test": self.messages}}

    def dumps(self):
        return json.dumps(self.messages, sort_keys=True) * 3


def normalize(obj):
    """Z-component ids are uuid4s: number them in order of appearance."""
    text = json.dumps(obj, sort_keys=True)
    seen = {}
    for z in re.findall(
        r"Z[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", text
    ):
        seen.setdefault(z, f"Z-{len(seen)}")
    for z, token in seen.items():
        text = text.replace(z, token)
    text = re.sub(r'"timestamp": "\d{4}-\d\d-\d\dT[\d:.]+"', '"timestamp": "T"', text)
    return json.loads(text)


def first_node_detail(result):
    body = (
        result[0]
        if isinstance(result, list) and len(result) == 2 and isinstance(result[1], int)
        else result
    )
    nodes = body["message"]["knowledge_graph"]["nodes"]
    return nodes[sorted(nodes)[0]]["detail_lookup"]


def run_all(setup, lookup):
    """setup(data) prepares a case's reachable data; lookup(id) -> result (tuples as lists)."""
    out = {}
    for name, steps, data in CASES:
        setup(data)
        results = []
        for step in steps:
            if step == "$Z":
                step = first_node_detail(results[-1])
            result = lookup(step)
            if isinstance(result, tuple):
                result = list(result)
            results.append(json.loads(json.dumps(result, default=str)))
        out[name] = [normalize(r) for r in results]
    return out
