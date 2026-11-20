#!/usr/bin/env bash
# Run the Python aragorn.score scorer and Rust variants on the same message
# and compare them. Debugging aid for deciding on a Rust scorer; not used by
# the workers.
#
#   ./run.sh                         # synthetic message (see gen_message.py)
#   ./run.sh path/to/message.json.zst
#   REPEAT=10 GEN_ARGS="--results 5000 --nested" ./run.sh
#   GEN_ARGS="--results 300 --quirks" ./run.sh     # parity on odd inputs
#   VARIANTS="typed:cholesky" ./run.sh             # just the fastest Rust one
#
# Env:
#   REPEAT    runs per scorer (default 5)
#   VARIANTS  Rust variants, PARSE:SOLVER, space-separated
#             (default "value:svd typed:svd typed:cholesky")
#   CHECK_MESSAGES  1 (default) saves each scored message and checks they
#             agree with Python's; 0 skips that (it reloads every message)
#   OUT, GEN_ARGS, TOL (default 1e-9), PYTHON (default python3)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-$HERE/out}"
REPEAT="${REPEAT:-5}"
TOL="${TOL:-1e-9}"
PYTHON="${PYTHON:-python3}"
VARIANTS="${VARIANTS:-value:svd typed:svd typed:cholesky}"
CHECK_MESSAGES="${CHECK_MESSAGES:-1}"
mkdir -p "$OUT"

MESSAGE="${1:-}"
if [[ -z "$MESSAGE" ]]; then
  MESSAGE="$OUT/synthetic.json.zst"
  # shellcheck disable=SC2086
  "$PYTHON" "$HERE/gen_message.py" --out "$MESSAGE" ${GEN_ARGS:-}
fi

save() { [[ "$CHECK_MESSAGES" == 1 ]] && echo "--save-message $OUT/$1.scored.json.zst" || true; }

echo "== building the Rust scorer (release)" >&2
cargo build --release --quiet --manifest-path "$HERE/rust/Cargo.toml"

echo "== scoring $MESSAGE ($REPEAT runs each)" >&2
# shellcheck disable=SC2046
"$PYTHON" "$HERE/python_scorer.py" "$MESSAGE" --repeat "$REPEAT" --out "$OUT/python.json" $(save python)
results=()
for v in $VARIANTS; do
  parse="${v%%:*}"; solver="${v##*:}"
  # shellcheck disable=SC2046
  "$HERE/rust/target/release/aragorn_score_rs" "$MESSAGE" --repeat "$REPEAT" \
    --parse "$parse" --solver "$solver" --out "$OUT/rust-$parse-$solver.json" $(save "rust-$parse-$solver")
  results+=("$OUT/rust-$parse-$solver.json")
done

echo >&2
"$PYTHON" "$HERE/compare.py" "$OUT/python.json" "${results[@]}" --tol "$TOL"
