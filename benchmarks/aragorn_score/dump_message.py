"""Save a real message from Shepherd's Redis to a file for the benchmark.

    python dump_message.py RESPONSE_ID out/real.json.zst

Run it where the worker's Redis settings are in the environment (the same
settings ``shepherd_utils.config`` reads). The best one to grab is the
response the ``aragorn.score`` stream was handed: its ``response_id``, before
scoring ran -- or any ARAGORN response, since scoring overwrites the scores.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import orjson  # noqa: E402

from shepherd_utils.db import encode_message, get_message_sync  # noqa: E402


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    response_id, out = sys.argv[1], Path(sys.argv[2])
    message = get_message_sync(response_id)
    data = encode_message(message)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(
        data
        if out.suffix == ".zst"
        else encode_message.__globals__["zstandard"].decompress(data)
    )
    print(f"wrote {out} ({len(data) / 1e6:.1f} MB compressed)", file=sys.stderr)


if __name__ == "__main__":
    main()
