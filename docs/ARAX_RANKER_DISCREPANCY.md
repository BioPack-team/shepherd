# ARAX ranker: why Shepherd Dev and ARAX CI scores differ

Query: inferred `biolink:treats`, `ChemicalEntity` → `MONDO:0007186` (GERD).
Responses: `scripts/responses/arax-dev/` (Shepherd, `arax_internal=true`) and
`scripts/responses/arax-ci/` (remote ARAX, reached through Shepherd's legacy path).
Reproduce everything below with `python scripts/arax_ranker_eval.py` (about 1 minute).

## Summary

The ranker code is the same in both places. Shepherd's `shepherd_utils/arax/ARAX_ranker.py`
is a faithful port of RTX @ 9485431, and upstream `ARAX_ranker.py`, `ARAX_resultify.py`
and `result_transformer.py` haven't changed between 9485431 and RTX `master` or
`production`. The difference has two parts:

1. **The ranker gives different scores to results whose evidence is tied.**
   `_break_ties_and_preserve_order` makes every score unique. After a stable sort by raw
   score, each tied result gets 0.001 less than the one before it. In this query, 178
   results (ranks 244–421) have exactly the same raw score, so they're spread from
   0.406 down to 0.229 based only on where they are in the list.
2. **The order of tied results comes from Python's string hash seed.** Resultify creates
   one result per SN node by iterating over a `set` of CURIEs
   (`_get_kg_node_keys_by_qg_key` → `_create_result_graphs`, "Starting with a result
   graph for each SN node"). Set iteration order depends on `PYTHONHASHSEED`.
   Shepherd's ARAX image pins `PYTHONHASHSEED=0` (`workers/arax/Dockerfile`), so Dev
   always gives the same order. The remote ARAX process has a different seed, so CI's
   order is different.

Neither environment is wrong. Each one assigns tied results an arbitrary but repeatable
order. CI's order is tied to its process's hash seed, so CI's own scores for these
results can change after a restart. No change on the Shepherd side can reproduce CI's
current numbers. The only fix that works is making the scores independent of order.

## Evidence

### All 185 score differences are tie reshuffles

| Ranks | Results changed | What it is |
|---|---|---|
| 244–421 | 177 of 178 | One tie block in the raw scores. Published scores drop by exactly 0.001 per rank in both files, and the block has the same members in both. Only the order inside it differs. |
| 99–100 | 2 | A 2-way tie (CJ-12420, Enteryx): the same two scores, swapped. |
| 24–29 | 6 | Real raw-score differences of 0.007 or less. See "Open item" below. |

The examples in the task are all inside the 178-result block: Everolimus (rank 251),
Oxaliplatin (272), Chlorthalidone (387), Ruxolitinib (245) and sodium alginate (264).
So are the FAERS-only and clinicaltrials-only evidence groups, and the "near-tie" cases.
The evidence-profile groups are subsets of a single, larger raw-score tie. The ranker
only scores bound edges. In these results the bound `treats` edge is an ARAX-prediction
edge, whose key has no infores, so its base weight is 0. NGD `inf` adds nothing either.
Results whose supporting evidence differs can therefore still end up with exactly the
same raw score.

### The hash seed reproduces Dev's order exactly

Build a `set` from the SN CURIEs in Dev's KG node order and iterate it:

| `PYTHONHASHSEED` | Matches Dev's order in the 178 block | Matches CI's |
|---|---|---|
| 0 (Shepherd's pinned value) | **yes, exactly** | no |
| 1, 2, 3 | no | no |

Seed 0 also puts CJ-12420 before Enteryx, which matches Dev at ranks 99–100.

### Re-ranking in a different order changes only tied results

`scripts/arax_ranker_eval.py` rebuilds an approximate ranker input from each response.
The published bindings were rewritten by the ResultTransformer after ranking. The script
re-binds the N1 NGD edges and the Retriever edges behind each ARAX-prediction edge.
Absolute values don't match the published ones, because the 17 results the NGD-inf filter
removed are gone, but the order dependence shows clearly:

| | Current ranker | Tie-preserving fix |
|---|---|---|
| Dev data vs CI data, same result order | 0 differ | 0 differ |
| Dev data vs CI data, each in its own order | **177 differ** (the published number is also 177) | 0 differ |
| Dev data, 5 random result orders | 185–195 differ | 0 differ |
| Changed results with a unique raw score | 0 | 0 |

The inputs are therefore equivalent. Only the result order differs, and it only matters
for results with tied raw scores.

## Open item: ranks 24–29

Nemalite, cisapride, Aflurax, Citalopram HCl, Mylanta and Indobufen have slightly
different **values** in the two files (for example cisapride is 0.931 in Dev and 0.933
in CI), not just a different order. The published KG has no differences that the ranker
reads. So the ranking-time input must have differed in something the ResultTransformer
or KG pruning removed afterwards: the 17 results the NGD-inf filter dropped (rank 438 →
421), or KG edges pruned after ranking (3036 → 3000). A difference of a few quantile-rank
steps (1/1314 each) is enough. To pin it down, both sides need a dump of the ranker's
input (the message right after resultify).

## Proposed fix

Make the scores independent of order. Equal raw scores should get equal output scores,
and display order should use a deterministic secondary key.

```python
def _break_ties_and_preserve_order(scores):   # scores already sorted descending
    n = min(len(scores), 1000)
    return [round(max(min(s, 1), 0), 3) if i < n else 0 for i, s in enumerate(scores)]

# in aggregate_scores_dmk, replace the re-sort with:
message.results.sort(key=lambda r: (-r.analyses[0].score, r.essence or "", _sn_id(r)))
```

This also fixes a ranking-quality problem. Right now, 178 results with the same evidence
are spread over 0.177 of the score range, so a drug's score depends on its CURIE's hash
rather than its evidence.

How to roll it out:

- **Upstream first (RTXteam/RTX).** CI runs upstream ARAX. Dev and CI only match when
  both have the fix. If ARAX needs scores to be unique (for the ARS or the UI), a
  fallback keeps the 0.001 cascade but sorts by `(-raw, essence, curie)` before applying
  it. That makes the output deterministic, but tied evidence still gets different scores.
- **Then in Shepherd.** Under DEC-1 this is a recorded deviation in
  `docs/ARAX_PORT_BASELINE.md` (RNK-05) until upstream has it. It goes in both
  `shepherd_utils/arax/ARAX_ranker.py` and `workers/arax_rank/ranker.py`, and the parity
  goldens need regenerating.
- **Optional hardening:** iterate `sorted(...)` in resultify's
  `_create_result_graphs`, so result order stops depending on the hash seed everywhere
  else too.

Until then, compare Dev and CI on per-result scores only for results outside tie blocks.
A run of exact 0.001 steps marks a tie block.
