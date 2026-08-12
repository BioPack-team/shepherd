import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

import httpx

target_urls = {
    "aragorn-ci": "https://shepherd.ci.transltr.io/aragorn",
    "arax-ci": "https://shepherd.ci.transltr.io/arax",
    "aragorn-dev": "https://shepherd.renci.org/aragorn",
    "arax-dev": "https://shepherd.renci.org/arax",
    "aragorn-local": "http://localhost:5439/aragorn",
    "arax-local": "http://localhost:5439/arax",
    "bte-local": "http://localhost:5439/bte",
}

RESPONSES_DIR = "responses"


def generate_query(curie1: str, curie2: str) -> dict:
    """Given a curie, return a TRAPI message."""
    parameters = {
        # "timeout": 300,
        # "tiers": [0],
    }
    return {
      "message": {
          "query_graph": {
              "nodes": {
                  "on": {
                      "constraints": [],
                      "ids": [
                          curie1
                      ],
                  },
                  "sn": {
                      "constraints": [],
                      "ids": [
                          curie2
                      ],
                  }
              },
              "paths": {
                  "p0": {
                      "object": "on",
                      "subject": "sn",
                      "predicates": [
                          "biolink:related_to",
                      ]
                  }
              }
          }
      },
      "parameters": parameters,
      "log_level": "DEBUG",
  }


async def single_lookup(curies: tuple[str, str], target: str):
    """Run a single query lookup synchronously."""
    query = generate_query(curies[0], curies[1])
    start_time = datetime.now()
    try:
        async with httpx.AsyncClient(timeout=600000) as client:
            response = await client.post(
                f"{target_urls[target]}/query",
                json=query,
            )
            response.raise_for_status()
            response_json = response.json()
            results = (response_json.get("message") or {}).get("results") or []
            num_results = len(results)
            assert num_results == 1
            num_analyses = len(results[0]["analyses"])
    except Exception as e:
        num_results = 0
        num_analyses = 0
        response_json = {
            "Error": str(e),
        }

    stop_time = datetime.now()
    print(f"{curies[0]}->{curies[1]} took {stop_time - start_time} seconds and gave {num_analyses} results")
    out_dir = Path(RESPONSES_DIR) / "pathfinder" / target
    out_dir.mkdir(parents=True, exist_ok=True)
    response_path = out_dir / f"{('_').join(curies[0].split(':'))}_{('_').join(curies[1].split(':'))}_response.json"
    with response_path.open("w", encoding="utf-8") as f:
        json.dump(response_json, f, indent=2)


query_list = [
    ('MONDO:0021095', 'MONDO:0005105'),
    ('CHEBI:9139', 'MONDO:0004975'),
    ('CHEBI:5118', 'MONDO:0100233'),
    ('MONDO:0005180', 'MONDO:0005105'),
    ('MONDO:0019632', 'MONDO:0005340'),
    ('CHEBI:27881', 'NCBIGene:2739'),
    ('CHEBI:45783', 'MONDO:0004979'),  # Imatinib -> Asthma
    ('GO:0006914', 'MONDO:0005265'),
    ('NCBIGene:3458', 'CHEBI:16828'),
    ('MONDO:0005532', 'MONDO:0005180'),
    ('CHEBI:15647', 'UNII:31YO63LBSN'),
    ('CHEBI:28364', 'MONDO:0005311'),
    ('NCBIGene:3458', 'MONDO:0100096'),
    ('NCBIGene:27240', 'MONDO:0100096'),
    ('CHEBI:3750', 'MONDO:0013209'),
    ('CHEBI:83766', 'MONDO:0008170'),
    ('CHEBI:45783', 'MONDO:0004784'),
    ('UNII:7SE5582Q2P', 'MONDO:0007037'),
    ('MONDO:0005011', 'MONDO:0005180'),
    ('CHEBI:15365', 'MONDO:0005575'),
    ('CHEBI:50924', 'MONDO:0007256'),
    ('CHEBI:45713', 'NCBIGene:2739'),
    ('NCBIGene:54716', 'MONDO:0100096'),
    ('CHEBI:7465', 'MONDO:0008218'),
    # ('CHEBI:10033', 'MONDO:0004992'),  # Warfarin -> Cancer, DON'T RUN
]


async def main():
    """Run the given query and time it."""
    targets = ["arax-local"]
    runs_per_target = 1

    start = time.time()
    queries = []
    for curies in query_list:
        queries.extend([
            single_lookup(curies, target)
            for target in targets
            for _ in range(runs_per_target)
        ])
    await asyncio.gather(*queries)
    print(f"\nAll queries took {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
