"""Shared ARS post-merge tail workflow.

Launched once every ARA has reported back (by ``ars_accumulate``, or forced by
the watchdog on timeout). ``finish_query`` is appended automatically by
``wrap_up_task`` when the list empties, so it is not listed here.

Order matches the ARS ``post_process`` sequence
(remove_blocked -> annotate -> appraise -> normalizeScores), with node
normalization first (Shepherd runs it post-merge) and a top-N trim last. Running
the blocklist before annotation/appraisal also prunes the message before those
expensive, memory-heavy steps.
"""

ARS_TAIL_WORKFLOW = [
    {"id": "node_norm"},
    {"id": "ars_blocklist"},
    {"id": "node_annotate"},
    {"id": "answer_appraise"},
    {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
]
