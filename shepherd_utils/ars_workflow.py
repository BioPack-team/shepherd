"""Shared ARS post-merge tail workflow.

Launched once every ARA has reported back (by ``ars_accumulate``, or forced by
the watchdog on timeout). ``finish_query`` is appended automatically by
``wrap_up_task`` when the list empties, so it is not listed here.

Order matches the ARS ``post_process`` sequence (remove_blocked -> annotate ->
appraise -> scoring), with a top-N trim last. Node normalization is NOT in this
tail: each ARA response is canonicalized per-response before the cross-ARA merge
(``ars_accumulate``), so the accumulated message is already normalized. Running
the blocklist before annotation/appraisal also prunes the message before those
expensive, memory-heavy steps. ``answer_appraise`` performs the final Sugeno
ranking, so ``filter_results_top_n`` trims the already-ranked results.
"""

ARS_TAIL_WORKFLOW = [
    {"id": "ars_blocklist"},
    {"id": "node_annotate"},
    {"id": "answer_appraise"},
    {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
]
