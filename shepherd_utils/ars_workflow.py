"""Shared ARS post-merge tail workflow.

Launched once every ARA has reported back (by ``ars_accumulate``, or forced by
the watchdog on timeout). ``finish_query`` is appended automatically by
``wrap_up_task`` when the list empties, so it is not listed here.
"""

ARS_TAIL_WORKFLOW = [
    {"id": "node_norm"},
    {"id": "node_annotate"},
    {"id": "answer_appraise"},
    {"id": "ars_blocklist"},
    {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
]
