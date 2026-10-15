"""Differential parity driver: original ARS vs Shepherd-ARS, same mockworld.

For each scenario: arm the mockworld, submit the identical query to both
stacks' /ars/api/submit, poll both parents to a terminal state, then diff:

  1. the submit envelope (normalized)
  2. the terminal trace tree summary (per-child terminal facts, parent
     state, merged-version bookkeeping)
  3. the final merged message content (normalized TRAPI)
  4. the mockworld journal's outbound side effects per stack

Usage:
    python tests/parity_e2e/run_parity.py \
        --relay-url http://localhost:8000 \
        --shepherd-url http://localhost:5439 \
        --mockworld-url http://localhost:8099 \
        [--scenario two_aras_merge] [--include-slow] \
        [--report parity_report.json]

Exit code 0 = every scenario matched; 1 = at least one diff (the report
lists every field-level difference).
"""

import argparse
import asyncio
import json
import sys
import time

import httpx

from normalize import canonical, diff, summarize_tree  # noqa: E402
from scenarios import SCENARIOS  # noqa: E402

TERMINAL = {"Done", "Stopped", "Error", "Unknown"}


async def submit(client, base, query):
    r = await client.post(f"{base}/ars/api/submit", json=query)
    r.raise_for_status()
    return r.json()


async def poll_terminal(client, base, pk, timeout_sec):
    deadline = time.time() + timeout_sec
    trace = None
    while time.time() < deadline:
        r = await client.get(f"{base}/ars/api/messages/{pk}?trace=y")
        if r.status_code == 200:
            trace = r.json()
            if trace.get("status") in TERMINAL:
                return trace
        await asyncio.sleep(2)
    return trace


async def merged_payload(client, base, trace):
    merged_pk = trace.get("merged_version")
    if merged_pk in (None, "None"):
        return None
    r = await client.get(f"{base}/ars/api/messages/{merged_pk}")
    if r.status_code != 200:
        return {"_fetch_status": r.status_code}
    return (r.json().get("fields") or {}).get("data")


def journal_summary(journal):
    """Outbound side effects, order-insensitive."""
    return sorted(
        json.dumps(
            {k: v for k, v in entry.items() if k not in ("ts", "callback")},
            sort_keys=True,
        )
        for entry in journal
        if entry.get("kind")
        in ("ara_asyncquery", "ara_query", "appraise", "annotate", "notification")
    )


async def run_scenario(name, scenario, args, client):
    report = {"scenario": name, "diffs": []}
    per_stack = {}
    for stack, base in (("relay", args.relay_url), ("shepherd", args.shepherd_url)):
        # arm the mockworld freshly per stack so journals are separable
        arm = dict(scenario)
        arm.pop("query", None)
        arm.pop("slow", None)
        await client.put(f"{args.mockworld_url}/scenario", json=arm)
        envelope = await submit(client, base, scenario["query"])
        pk = envelope["pk"]
        timeout = 720 if scenario.get("slow") else args.timeout
        trace = await poll_terminal(client, base, pk, timeout)
        journal = (await client.get(f"{args.mockworld_url}/journal")).json()
        per_stack[stack] = {
            "envelope": envelope,
            "trace": trace,
            "merged": await merged_payload(client, base, trace or {}),
            "journal": journal_summary(journal),
        }

    relay, shepherd = per_stack["relay"], per_stack["shepherd"]
    report["diffs"] += diff(
        relay["envelope"].get("fields", {}).get("status"),
        shepherd["envelope"].get("fields", {}).get("status"),
        "$.submit.status",
    )
    if relay["trace"] is None or shepherd["trace"] is None:
        report["diffs"].append(
            f"terminal state not reached: relay={relay['trace'] is not None} "
            f"shepherd={shepherd['trace'] is not None}"
        )
        return report
    report["diffs"] += diff(
        summarize_tree(relay["trace"]),
        summarize_tree(shepherd["trace"]),
        "$.tree",
    )
    report["diffs"] += diff(relay["merged"], shepherd["merged"], "$.merged")
    if relay["journal"] != shepherd["journal"]:
        report["diffs"].append(
            "$.journal: outbound side effects differ\n"
            f"  relay:    {relay['journal']}\n"
            f"  shepherd: {shepherd['journal']}"
        )
    report["canonical_merged"] = canonical(shepherd["merged"])
    return report


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--relay-url", required=True)
    parser.add_argument("--shepherd-url", required=True)
    parser.add_argument("--mockworld-url", required=True)
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--include-slow", action="store_true")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--report", default="parity_report.json")
    args = parser.parse_args()

    names = args.scenario or list(SCENARIOS)
    reports = []
    failed = False
    async with httpx.AsyncClient(timeout=60) as client:
        for name in names:
            scenario = SCENARIOS[name]
            if scenario.get("slow") and not args.include_slow:
                print(f"SKIP  {name} (slow; use --include-slow)")
                continue
            print(f"RUN   {name} ...", flush=True)
            report = await run_scenario(name, scenario, args, client)
            reports.append(report)
            if report["diffs"]:
                failed = True
                print(f"DIFF  {name}: {len(report['diffs'])} difference(s)")
                for d in report["diffs"][:10]:
                    print(f"      {d}")
            else:
                print(f"OK    {name}")

    with open(args.report, "w") as f:
        json.dump(reports, f, indent=2)
    print(f"report written to {args.report}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
