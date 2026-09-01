"""SmartAPI service discovery.

Port of NCATSTranslator/Relay @ dd1e71b tr_sys/tr_smartapi_client/
smart_api_discover.py + tr_sys/utils2.py urlRemoteFromInforesid. Resolution
precedence is upstream's exactly: the smart-api.info registry (filtered by
maturity settings.tr_env and TRAPI version settings.tr_ver, newest
registration per infores, hourly refresh / 30s retry after failure), falling
back to the bundled url-config-legacy.yaml; the endpoint word (query vs
asyncquery) and extra query params always come from the bundled config.yaml
httpclients map.
"""

import logging
import threading
import time
from typing import Any, Dict, Optional

import httpx

from shepherd_utils.ars.registry_config import (
    endpoint_override,
    legacy_url,
    params_override,
)
from shepherd_utils.config import settings

logger = logging.getLogger(__name__)

SECS_TIMEOUT = 5


def _getpath(j, fields):
    for field in fields:
        if j is None:
            return None
        j = j[field] if field in j else None
    return j


def _irhits_from_res(j):
    for hit in j.get("hits", []):
        _id = _getpath(hit, ["_id"])
        date_updated = _getpath(hit, ["_meta", "last_updated"])
        x_trapi_version = _getpath(hit, ["info", "x-trapi", "version"])
        infores = _getpath(hit, ["info", "x-translator", "infores"])
        servers = _getpath(hit, ["servers"])
        if servers is not None:
            for server in servers:
                maturity = _getpath(server, ["x-maturity"])
                url_server = _getpath(server, ["url"])
                if x_trapi_version is not None:
                    yield {
                        "infores": infores,
                        "urlServer": url_server,
                        "maturity": maturity,
                        "_id": _id,
                        "date_updated": date_updated,
                        "version": x_trapi_version,
                    }


def _newer(irhit1, irhit2):
    if irhit1.get("date_updated") is None and irhit2.get("date_updated") is None:
        return None
    return irhit1["date_updated"] > irhit2["date_updated"]


def _by_infores_latest(j, maturity, version):
    by_irid: Dict[str, Dict[str, Any]] = {}
    for irhit in _irhits_from_res(j):
        if maturity != irhit["maturity"]:
            continue
        key = irhit.get("infores")
        if key is None:
            continue
        extant = by_irid.get(key)
        if extant is None:
            by_irid[key] = irhit
            continue
        if version is not None:
            current_ok = irhit.get("version") == version
            extant_ok = extant.get("version") == version
            if current_ok and extant_ok and _newer(irhit, extant):
                by_irid[key] = irhit
            elif current_ok and not extant_ok:
                by_irid[key] = irhit
            elif not current_ok and extant_ok:
                continue
            elif not current_ok and not extant_ok and _newer(irhit, extant):
                by_irid[key] = irhit
        else:
            if _newer(irhit, extant):
                by_irid[key] = irhit
    logger.info(f"found {len(by_irid)} registrations with maturity={maturity}")
    return by_irid


def _fetch_registry(maturity: str, version: Optional[str]):
    try:
        url = (
            f"{settings.smartapi_url}?q=servers.x-maturity:{maturity}"
            "&size=150&fields=_meta,info,servers&meta=1"
        )
        transport = httpx.HTTPTransport(retries=5)
        with httpx.Client(transport=transport, timeout=SECS_TIMEOUT) as client:
            res = client.get(url)
        if res.status_code != 200:
            logger.warning(f"HTTP status {res.status_code} for {url}")
            return None
        return _by_infores_latest(res.json(), maturity, version)
    except httpx.HTTPError as e:
        logger.warning(f"Exception fetching from smart-api: {e}")
        return None


class SmartApiDiscoverer:
    """Per-process cached discoverer (upstream caches per process too)."""

    def __init__(self) -> None:
        self._maturity = settings.tr_env or "production"
        self._version = settings.tr_ver or None
        self._t_next_refresh = time.time()
        self._map_dynamic: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def ensure(self):
        with self._lock:
            if time.time() >= self._t_next_refresh:
                registry = _fetch_registry(self._maturity, self._version)
                if registry is not None:
                    self._map_dynamic = registry
                    self._t_next_refresh = time.time() + settings.smartapi_refresh_sec
                else:
                    self._t_next_refresh = time.time() + settings.smartapi_retry_sec

    def url_server(self, inforesid: str) -> Optional[str]:
        self.ensure()
        if inforesid in self._map_dynamic:
            return self._map_dynamic[inforesid].get("urlServer")
        return legacy_url(inforesid)

    def endpoint(self, inforesid: str) -> Optional[str]:
        return endpoint_override(inforesid)

    def params(self, inforesid: str) -> Optional[str]:
        return params_override(inforesid)


_discoverer: Optional[SmartApiDiscoverer] = None


def _get_discoverer() -> SmartApiDiscoverer:
    global _discoverer
    if _discoverer is None:
        _discoverer = SmartApiDiscoverer()
    return _discoverer


def url_server(inforesid: Optional[str]) -> Optional[str]:
    if not inforesid:
        return None
    return _get_discoverer().url_server(inforesid)


def endpoint(inforesid: Optional[str]) -> Optional[str]:
    if not inforesid:
        return None
    return _get_discoverer().endpoint(inforesid)


def params(inforesid: Optional[str]) -> Optional[str]:
    if not inforesid:
        return None
    return _get_discoverer().params(inforesid)


def url_remote_from_inforesid(inforesid: Optional[str]) -> Optional[str]:
    """utils2.urlRemoteFromInforesid: server + /endpoint + ?params."""
    if not inforesid:
        return None
    server = url_server(inforesid)
    if server is None:
        return None
    ep = endpoint(inforesid)
    prms = params(inforesid)
    if server[-1] == "/":
        server = server[:-1]
    return (
        server
        + (("/" + ep) if ep is not None else "")
        + (("?" + prms) if prms is not None else "")
    )
