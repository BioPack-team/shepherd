"""Normalization + diffing for differential ARS parity runs.

Both stacks produce trees full of volatile identifiers (message pks,
timestamps, hostnames) that legitimately differ; everything else must match.
The normalizer masks exactly the volatile fields and canonicalizes
order-insensitive collections, so `diff` output is a real behavioral
difference, never noise.
"""

import json
import re
from typing import Any, Dict, List, Tuple

UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)
TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")
HMS_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")

VOLATILE_KEYS = {"timestamp", "updated_at", "created_at", "time_elapsed"}


def mask_scalars(value: Any) -> Any:
    if isinstance(value, str):
        value = UUID_RE.sub("<uuid>", value)
        value = TS_RE.sub("<ts>", value)
        if HMS_RE.match(value):
            value = "<hms>"
        # hostnames differ between the two stacks' callback/notify URLs
        value = re.sub(r"https?://[^/\s]+", "<host>", value)
    return value


def normalize(obj: Any) -> Any:
    """Mask volatile values recursively; keys are preserved."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            key = mask_scalars(k) if isinstance(k, str) else k
            if key in VOLATILE_KEYS and isinstance(v, str):
                out[key] = "<ts>"
            else:
                out[key] = normalize(v)
        return out
    if isinstance(obj, list):
        return [normalize(v) for v in obj]
    return mask_scalars(obj)


def canonical(obj: Any) -> str:
    return json.dumps(normalize(obj), sort_keys=True, default=str)


def _multiset(items: List[Any]) -> List[str]:
    return sorted(json.dumps(i, sort_keys=True, default=str) for i in items)


def diff(a: Any, b: Any, path: str = "$") -> List[str]:
    """Field-level differences between two normalized structures.

    Lists compare ordered first, falling back to multiset equality (the few
    upstream set-union spots have hash-order-dependent ordering).
    """
    problems: List[str] = []
    a, b = normalize(a), normalize(b)
    _diff_normalized(a, b, path, problems)
    return problems


def _diff_normalized(a: Any, b: Any, path: str, problems: List[str]) -> None:
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                problems.append(f"{path}.{k}: missing on left")
            elif k not in b:
                problems.append(f"{path}.{k}: missing on right")
            else:
                _diff_normalized(a[k], b[k], f"{path}.{k}", problems)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            problems.append(f"{path}: list length {len(a)} != {len(b)}")
            return
        ordered: List[str] = []
        for i, (x, y) in enumerate(zip(a, b)):
            _diff_normalized(x, y, f"{path}[{i}]", ordered)
        if ordered and _multiset(a) != _multiset(b):
            problems.extend(ordered)
    elif isinstance(a, float) or isinstance(b, float):
        try:
            if abs(float(a) - float(b)) > 1e-9 * max(1.0, abs(float(a))):
                problems.append(f"{path}: {a} != {b}")
        except (TypeError, ValueError):
            problems.append(f"{path}: {a!r} != {b!r}")
    elif a != b:
        problems.append(f"{path}: {a!r} != {b!r}")


def summarize_tree(trace: Dict[str, Any]) -> Dict[str, Any]:
    """The state-tree comparison surface: per-child terminal facts as a
    multiset, parent facts, merged-version count."""

    def child_fact(node):
        return (
            node.get("actor", {}).get("agent"),
            node.get("status"),
            node.get("code"),
            node.get("result_count"),
        )

    children = trace.get("children", [])
    return {
        "parent_status": trace.get("status"),
        "parent_code": trace.get("code"),
        "children": sorted(
            (
                json.dumps(child_fact(c))
                for c in children
                if c.get("actor", {}).get("agent") != "ars-ars-agent"
            ),
        ),
        "merged_present": trace.get("merged_version") not in (None, "None"),
        "merged_versions_count": _merged_count(trace.get("merged_versions_list")),
    }


def _merged_count(mvl: Any) -> int:
    if mvl in (None, "None"):
        return 0
    if isinstance(mvl, str):
        try:
            import ast

            mvl = ast.literal_eval(mvl)
        except (ValueError, SyntaxError):
            return -1
    return len(mvl) if isinstance(mvl, list) else -1
