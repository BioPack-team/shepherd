"""Validation of caller-supplied callback URLs (the /asyncquery SSRF guard).

``/asyncquery`` takes a ``callback`` URL from an unauthenticated caller and
``finish_query`` later POSTs the finished response to it. Accepted unchecked,
that lets anyone make Shepherd send requests to whatever it can reach: the
cloud metadata endpoint (credential theft), internal services such as Redis or
Postgres, or a third party it is turned into an amplifier against.

So a callback must:

- use ``http`` or ``https`` (``file://``, ``gopher://`` and the ARS handoff
  sentinel ``shepherd-ars://`` are refused -- the sentinel is only ever written
  internally by ``ars_fanout``, never accepted from a caller);
- name a host that resolves only to public addresses, unless the host or
  address is on the operator's allowlist (``callback_allowed_hosts``) -- the
  local/Test flow calls back to ``host.docker.internal`` or the docker bridge,
  which are private, so those deployments allowlist them;
- never resolve to a cloud metadata address, allowlisted or not.

Hostnames are resolved and *every* address checked, so a public-looking name
that points at 169.254.169.254 is refused like the literal. The check runs at
intake (a clear 4xx for the caller) and again just before delivery, which
covers queries accepted before the check existed and narrows the window for a
name whose DNS changes in between (rebinding). It cannot close that window
entirely; the delivery client does not follow redirects, so a public receiver
cannot bounce the POST somewhere internal either.
"""

import asyncio
import ipaddress
import socket
from typing import Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlsplit

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
IPNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]

ALLOWED_SCHEMES = {"http", "https"}
# Longer than any real callback; bounds what we parse and echo back.
MAX_CALLBACK_URL_LENGTH = 2048
# How long intake waits on DNS for the callback host before refusing it.
RESOLVE_TIMEOUT_SEC = 5.0
# Instance metadata services. Refused even when an allowlisted network covers
# them (169.254.0.0/16 is an easy thing to allowlist by accident).
METADATA_ADDRESSES = {
    ipaddress.ip_address("169.254.169.254"),  # AWS, GCP, Azure, OpenStack
    ipaddress.ip_address("169.254.170.2"),  # AWS ECS task metadata
    ipaddress.ip_address("fd00:ec2::254"),  # AWS IMDS over IPv6
    ipaddress.ip_address("100.100.100.200"),  # Alibaba Cloud
}


class CallbackURLError(ValueError):
    """A callback URL Shepherd refuses to deliver to.

    The message is client-safe: it is returned verbatim to the caller, so it
    names the rule that was broken and never the addresses a name resolved to.
    """


def parse_allowlist(value: str) -> Tuple[Set[str], List[IPNetwork]]:
    """Split ``callback_allowed_hosts`` into hostnames and networks.

    Comma-separated; each entry is a hostname (matched exactly, case-insensitive),
    an IP address, or a CIDR network.

    >>> names, nets = parse_allowlist("host.docker.internal, 172.16.0.0/12,::1")
    >>> sorted(names), [str(n) for n in nets]
    (['host.docker.internal'], ['172.16.0.0/12', '::1/128'])
    """
    names: Set[str] = set()
    networks: List[IPNetwork] = []
    for entry in (value or "").split(","):
        entry = entry.strip().lower().rstrip(".")
        if not entry:
            continue
        try:
            networks.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            names.add(entry)
    return names, networks


def _normalize(address: IPAddress) -> IPAddress:
    """Unwrap an IPv4-mapped IPv6 address (``::ffff:127.0.0.1``) to IPv4."""
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        return address.ipv4_mapped
    return address


def check_addresses(
    host: str,
    addresses: Iterable[IPAddress],
    allowed_hosts: str = "",
) -> None:
    """Raise ``CallbackURLError`` unless every address is an allowed target."""
    names, networks = parse_allowlist(allowed_hosts)
    host_allowlisted = host in names
    for address in addresses:
        address = _normalize(address)
        if address in METADATA_ADDRESSES:
            raise CallbackURLError(
                "Invalid callback URL: it targets a cloud metadata address."
            )
        if host_allowlisted or any(address in net for net in networks):
            continue
        if not address.is_global or address.is_multicast:
            raise CallbackURLError(
                "Invalid callback URL: its host resolves to a private, loopback, "
                "link-local or otherwise non-public address."
            )


def split_callback_url(url: object) -> Tuple[str, int]:
    """Check a callback URL's shape; its (lowercased host, port)."""
    if not isinstance(url, str) or not url:
        raise CallbackURLError("Invalid callback URL: expected a non-empty string.")
    if len(url) > MAX_CALLBACK_URL_LENGTH:
        raise CallbackURLError(
            f"Invalid callback URL: longer than {MAX_CALLBACK_URL_LENGTH} characters."
        )
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as e:
        raise CallbackURLError("Invalid callback URL: could not be parsed.") from e
    scheme = parts.scheme.lower()
    if scheme not in ALLOWED_SCHEMES:
        raise CallbackURLError(
            "Invalid callback URL: the scheme must be http or https."
        )
    host = (parts.hostname or "").rstrip(".")
    if not host:
        raise CallbackURLError("Invalid callback URL: no host.")
    if port is None:
        port = 443 if scheme == "https" else 80
    return host, port


async def resolve_host(host: str, port: int) -> List[IPAddress]:
    """Every address ``host`` resolves to (just itself for an IP literal)."""
    try:
        return [ipaddress.ip_address(host)]
    except ValueError:
        pass
    loop = asyncio.get_running_loop()
    try:
        infos = await asyncio.wait_for(
            loop.getaddrinfo(host, port, type=socket.SOCK_STREAM),
            RESOLVE_TIMEOUT_SEC,
        )
    except (OSError, asyncio.TimeoutError, UnicodeError) as e:
        raise CallbackURLError(
            "Invalid callback URL: its host could not be resolved."
        ) from e
    # sockaddr[0] can carry an IPv6 zone ("fe80::1%eth0"); the zone is not part
    # of the address.
    addresses = {ipaddress.ip_address(info[4][0].split("%")[0]) for info in infos}
    if not addresses:
        raise CallbackURLError("Invalid callback URL: its host could not be resolved.")
    return list(addresses)


async def validate_callback_url(
    url: object, allowed_hosts: Optional[str] = None
) -> str:
    """Raise ``CallbackURLError`` unless ``url`` is safe to POST a response to.

    ``allowed_hosts`` defaults to ``settings.callback_allowed_hosts``. Returns
    the URL unchanged so callers can validate and assign in one step.
    """
    if allowed_hosts is None:
        from shepherd_utils.config import settings

        allowed_hosts = settings.callback_allowed_hosts
    host, port = split_callback_url(url)
    addresses = await resolve_host(host, port)
    check_addresses(host, addresses, allowed_hosts)
    return url  # type: ignore[return-value]
