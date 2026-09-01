"""SmartAPI service discovery (port of tr_smartapi_client).

Phase-5 module: the full registry client with the shared Redis cache lands
here. Until then, url_remote_from_inforesid resolves purely from the legacy
URL config, mirroring utils2.urlRemoteFromInforesid's fallback behavior.
"""

from typing import Optional


def url_remote_from_inforesid(inforesid: Optional[str]) -> Optional[str]:
    """Best-effort remote URL for an infores id; None when unknown."""
    if not inforesid:
        return None
    from shepherd_utils.ars.registry_config import legacy_url

    return legacy_url(inforesid)
