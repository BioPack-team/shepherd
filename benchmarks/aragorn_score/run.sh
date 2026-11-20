#!/usr/bin/env bash
# Run the Python and Rust aragorn.score scorers on the same message and
# compare them. Debugging aid for deciding on a Rust scorer; not used by the
# workers.
#
#   ./run.sh                         # synthetic message (see gen_message.py)
#   ./run.sh path/to/message.json.zst
#   REPEAT=10 GEN_ARGS="--results 5000 --nested" ./run.sh
#   GEN_ARGS="--results 200 --quirks" ./run.sh    # parity on odd inputs
#
# Env: REPEAT (default 5), OUT (default ./out), GEN_ARGS, TOL (default 1e-9),
# PYTHON (default python3).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-$HERE/out}"
REPEAT="${REPEAT:-5}"
TOL="${TOL:-1e-9}"
PYTHON="${PYTHON:-python3}"
mkdir -p "$OUT"

MESSAGE="${1:-}"
if [[ -z "$MESSAGE" ]]; then
  MESSAGE="$OUT/synthetic.json.zst"
  # shellcheck disable=SC2086
  "$PYTHON" "$HERE/gen_message.py" --out "$MESSAGE" ${GEN_ARGS:-}
fi

echo "== building the Rust scorer (release)" >&2
cargo build --release --quiet --manifest-path "$HERE/rust/Cargo.toml"

echo "== scoring $MESSAGE ($REPEAT runs each)" >&2
"$PYTHON" "$HERE/python_scorer.py" "$MESSAGE" --repeat "$REPEAT" --out "$OUT/python.json"
"$HERE/rust/target/release/aragorn_score_rs" "$MESSAGE" --repeat "$REPEAT" --out "$OUT/rust.json"

echo >&2
"$PYTHON" "$HERE/compare.py" "$OUT/python.json" "$OUT/rust.json" --tol "$TOL"
