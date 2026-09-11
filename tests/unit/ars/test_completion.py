"""Parity tests for the parent-completion arithmetic.

Upstream reference: NCATSTranslator/Relay @ 3e65975
  - tr_sys/tr_ars/signals.py  message_post_save lines 47-124

The upstream rules, verbatim from the signal:

    finished = no child has a status outside {'D','S','E','U'}
    for each child:
        if status=='D' and agent.name.startswith('ar')
                and result_count is not None and result_count > 0:
            merge_count += 1  if agent.name == 'ars-ars-agent'
            orig_count  += 1  otherwise
        if status=='E' and agent.name == 'ars-ars-agent':
            merge_count += 1  if code == 444
            orig_count  -= 1  otherwise
    complete = finished and merge_count == orig_count
    empty    = complete and merge_count == 0 and orig_count == 0

Behavior register rows: P-LC-1 .. P-LC-6 (arithmetic portion).
"""

from shepherd_utils.ars.completion import evaluate_completion


def child(status, agent, code=200, result_count=None):
    return {
        "status": status,
        "agent_name": agent,
        "code": code,
        "result_count": result_count,
    }


def ara(status="D", results=10, code=200, agent="ara-aragorn"):
    return child(status, agent, code=code, result_count=results)


def merge(status="D", results=10, code=200):
    return child(status, "ars-ars-agent", code=code, result_count=results)


def test_happy_path_all_merged():
    """P-LC-1: N result-bearing ARA children + N done merges -> complete."""
    decision = evaluate_completion([ara(), ara(agent="ara-arax"), merge(), merge()])
    assert decision.finished
    assert decision.orig_count == 2
    assert decision.merge_count == 2
    assert decision.complete
    assert not decision.empty


def test_not_finished_while_any_child_running():
    decision = evaluate_completion([ara(), merge(), ara(status="R", agent="ara-bte")])
    assert not decision.finished
    assert not decision.complete


def test_waiting_child_blocks_completion():
    decision = evaluate_completion([child("W", "ara-bte")])
    assert not decision.finished


def test_stopped_and_unknown_are_terminal():
    decision = evaluate_completion([child("S", "ara-bte"), child("U", "ara-arax")])
    assert decision.finished
    assert decision.complete  # 0 == 0
    assert decision.empty


def test_all_empty_results_is_empty_complete():
    """P-LC-2: ARA children done with 0 results count nothing -> empty merge."""
    decision = evaluate_completion([ara(results=0), ara(results=0, agent="ara-arax")])
    assert decision.finished
    assert decision.orig_count == 0
    assert decision.merge_count == 0
    assert decision.complete
    assert decision.empty


def test_none_result_count_counts_nothing():
    decision = evaluate_completion([ara(results=None)])
    assert decision.orig_count == 0
    assert decision.complete
    assert decision.empty


def test_errored_ara_child_excluded():
    """P-LC-3: a timed-out (598/'E') ARA child never joins orig_count."""
    decision = evaluate_completion(
        [ara(), merge(), child("E", "ara-bte", code=598, result_count=None)]
    )
    assert decision.finished
    assert decision.orig_count == 1
    assert decision.merge_count == 1
    assert decision.complete


def test_merge_444_counts_as_satisfied():
    """P-LC-4a: an 'E'/444 merge child adds to merge_count."""
    decision = evaluate_completion([ara(), merge(status="E", code=444)])
    assert decision.merge_count == 1
    assert decision.orig_count == 1
    assert decision.complete


def test_merge_error_other_code_decrements_orig():
    """P-LC-4b: an 'E' merge child with any other code decrements orig_count."""
    decision = evaluate_completion([ara(), merge(status="E", code=422)])
    assert decision.merge_count == 0
    assert decision.orig_count == 0
    assert decision.complete
    # both zero, but this is NOT the empty case only when counts started >0?
    # Upstream: empty is checked purely on merge_count==0 and orig_count==0,
    # so a 422-failed merge that cancels its origin DOES take the empty path.
    assert decision.empty


def test_done_merge_with_zero_results_does_not_count():
    """A 'D' merge child needs result_count > 0 to satisfy its origin --
    faithful to upstream, where such a parent never completes."""
    decision = evaluate_completion([ara(), merge(results=0)])
    assert decision.finished
    assert decision.orig_count == 1
    assert decision.merge_count == 0
    assert not decision.complete


def test_non_ar_agent_results_do_not_count():
    """Only agents whose name starts with 'ar' feed orig_count (KP agents
    like kp-genetics never do)."""
    decision = evaluate_completion([child("D", "kp-genetics", result_count=5)])
    assert decision.orig_count == 0
    assert decision.complete
    assert decision.empty


def test_merge_pending_blocks_count_parity():
    """A result-bearing ARA child whose merge hasn't finished yet leaves
    merge_count < orig_count -> not complete (even though all ARA children
    are terminal) while the merge child is still running."""
    decision = evaluate_completion([ara(), merge(status="R")])
    assert not decision.finished  # merge child 'R' also blocks finished
    assert not decision.complete


def test_no_children_completes_empty():
    """A parent with zero children (nothing matched any actor) is finished
    with 0==0 counts -> empty completion."""
    decision = evaluate_completion([])
    assert decision.finished
    assert decision.complete
    assert decision.empty
