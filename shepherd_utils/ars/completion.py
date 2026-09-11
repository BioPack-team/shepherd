"""Parent-completion arithmetic.

Ported verbatim from NCATSTranslator/Relay @ 3e65975,
tr_sys/tr_ars/signals.py ``message_post_save`` (lines 47-124). This module
holds the pure decision; the DB/notification orchestration around it lives in
``shepherd_utils.ars.db`` and the workers so this part stays exhaustively
testable.
"""

from dataclasses import dataclass
from typing import Any, Dict, List

from .statuses import TERMINAL_STATUSES

MERGE_AGENT_NAME = "ars-ars-agent"


@dataclass(frozen=True)
class CompletionDecision:
    finished: bool
    merge_count: int
    orig_count: int

    @property
    def complete(self) -> bool:
        return self.finished and self.merge_count == self.orig_count

    @property
    def empty(self) -> bool:
        """The zero-results case: an empty merged message is synthesized."""
        return self.complete and self.merge_count == 0 and self.orig_count == 0


def evaluate_completion(children: List[Dict[str, Any]]) -> CompletionDecision:
    """Evaluate the upstream completion rules over a parent's children.

    Each child record needs: ``status`` (letter), ``code`` (int),
    ``agent_name`` (str), ``result_count`` (int or None).
    """
    finished = True
    merge_count = 0
    orig_count = 0
    for child in children:
        status = child["status"]
        agent_name = child["agent_name"]
        result_count = child.get("result_count")
        if status not in TERMINAL_STATUSES:
            finished = False
        if (
            status == "D"
            and agent_name.startswith("ar")
            and (result_count is not None and result_count > 0)
        ):
            if agent_name == MERGE_AGENT_NAME:
                merge_count += 1
            else:
                orig_count += 1
        if status == "E" and agent_name == MERGE_AGENT_NAME:
            if child["code"] == 444:
                merge_count += 1
            else:
                orig_count -= 1
    return CompletionDecision(
        finished=finished, merge_count=merge_count, orig_count=orig_count
    )
