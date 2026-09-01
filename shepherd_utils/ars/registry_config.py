"""ARS actor registry configuration.

Carries the upstream config verbatim (ars_config.yaml is Relay's
config/config.yaml; url-config-legacy.yaml likewise) plus the declarative
equivalent of the ten tr_ara_* Django apps' AppConfig registrations and the
tr_kp_* apps, ported from NCATSTranslator/Relay @ dd1e71b.

Every app registered: agent name = app_path, agent uri = /<app_path>/api/,
one actor at path 'runquery' on the listed channels with the listed
inforesid. The endpoint word (query vs asyncquery) and extra params come
from ars_config.yaml's httpclients map, exactly like SmartApiDiscover.
"""

import functools
import pathlib
from typing import Any, Dict, List, Optional

import yaml

_HERE = pathlib.Path(__file__).resolve().parent


@functools.lru_cache(maxsize=1)
def _config() -> Dict[str, Any]:
    with open(_HERE / "ars_config.yaml") as f:
        return yaml.safe_load(f) or {}


@functools.lru_cache(maxsize=1)
def _legacy_urls() -> Dict[str, str]:
    with open(_HERE / "url-config-legacy.yaml") as f:
        return yaml.safe_load(f) or {}


def inactive_clients() -> List[str]:
    return _config().get("inactive_clients") or []


def httpclient(inforesid: str) -> Dict[str, Any]:
    return (_config().get("httpclients") or {}).get(inforesid) or {}


def endpoint_override(inforesid: str) -> Optional[str]:
    return httpclient(inforesid).get("endpoint")


def params_override(inforesid: str) -> Optional[str]:
    return httpclient(inforesid).get("params")


def legacy_url(inforesid: str) -> Optional[str]:
    return _legacy_urls().get(inforesid)


# (app_path, inforesid, channels) -- from the tr_ara_*/tr_kp_* AppConfigs.
# Actor path is 'runquery' for every app.
ARA_APPS = [
    ("ara-aragorn", "infores:aragorn", ["general", "workflow"]),
    ("ara-arax", "infores:arax", ["general", "workflow"]),
    ("ara-bte", "infores:biothings-explorer", ["general"]),
    ("ara-cqs", "infores:cqs", ["general", "workflow"]),
    ("ara-improving", "infores:improving-agent", ["general"]),
    ("ara-shepherd-aragorn", "infores:shepherd-aragorn", ["general", "workflow"]),
    ("ara-shepherd-arax", "infores:shepherd-arax", ["general", "workflow"]),
    ("ara-shepherd-bte", "infores:shepherd-bte", ["general", "workflow"]),
    ("ara-unsecret", "infores:unsecret-agent", ["general"]),
    ("ara-wfr", "infores:workflow-runner", ["workflow"]),
]

KP_APPS = [
    ("kp-cam", "infores:automat-cam-kp", ["general"]),
    ("kp-chp", "infores:connections-hypothesis", ["general"]),
    ("kp-clinical", "infores:multiomics-clinicaltrials", ["general"]),
    ("kp-cohd", "infores:cohd", ["general"]),
    ("kp-drug", "infores:multiomics-drugapprovals", ["general"]),
    ("kp-genetics", "infores:genetics-data-provider", ["general"]),
    ("kp-molecular", "infores:molepro", ["general"]),
    ("kp-openpredict", "infores:openpredict", ["general"]),
    ("kp-textmining", "infores:text-mining-provider-targeted", ["general"]),
]


def seed_actor_specs() -> List[Dict[str, Any]]:
    """The get_or_create_actor payloads for every registered app."""
    specs = []
    for app_path, inforesid, channels in ARA_APPS + KP_APPS:
        specs.append(
            {
                "agent": {"name": app_path, "uri": f"/{app_path}/api/"},
                "channel": channels,
                "path": "runquery",
                "inforesid": inforesid,
            }
        )
    return specs
