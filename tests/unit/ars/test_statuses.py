"""Parity tests for ARS status-letter semantics.

Upstream reference: NCATSTranslator/Relay @ 3e65975
  - tr_sys/tr_ars/models.py  Message.STATUS, Message.create, Message.to_dict
  - tr_sys/tr_ars/signals.py message_post_save (code coercion)
Behavior register rows: P-ST-1 .. P-ST-4.
"""

from shepherd_utils.ars.statuses import (
    STATUS,
    TERMINAL_STATUSES,
    coerce_code,
    to_letter,
    to_name,
)


def test_status_choices_match_upstream():
    """P-ST-1: the exact six (letter, long-name) pairs, in model order."""
    assert STATUS == (
        ("D", "Done"),
        ("S", "Stopped"),
        ("R", "Running"),
        ("E", "Error"),
        ("W", "Waiting"),
        ("U", "Unknown"),
    )


def test_terminal_statuses():
    """P-ST-2: the terminal set used by the parent-completion signal."""
    assert TERMINAL_STATUSES == {"D", "S", "E", "U"}


def test_to_letter_maps_long_names():
    """P-ST-3a: Message.create maps long names to letters..."""
    assert to_letter("Done") == "D"
    assert to_letter("Stopped") == "S"
    assert to_letter("Running") == "R"
    assert to_letter("Error") == "E"
    assert to_letter("Waiting") == "W"
    assert to_letter("Unknown") == "U"


def test_to_letter_passes_through_unknown_values():
    """...and leaves anything else (including letters) untouched."""
    assert to_letter("D") == "D"
    assert to_letter("bogus") == "bogus"


def test_to_name_maps_letters():
    """P-ST-3b: Message.to_dict maps letters to long names for display."""
    assert to_name("D") == "Done"
    assert to_name("S") == "Stopped"
    assert to_name("R") == "Running"
    assert to_name("E") == "Error"
    assert to_name("W") == "Waiting"
    assert to_name("U") == "Unknown"
    # pass-through for anything unrecognized
    assert to_name("Done") == "Done"


def test_code_coercion():
    """P-ST-4: post_save forces code 202 for 'R' and 200 for 'D'.

    Other statuses keep whatever code they were given (598 timeouts, 444/422
    post-process failures, 500 internal errors).
    """
    assert coerce_code("R", 200) == 202
    assert coerce_code("R", 598) == 202
    assert coerce_code("D", 202) == 200
    assert coerce_code("D", 444) == 200
    assert coerce_code("E", 598) == 598
    assert coerce_code("E", 444) == 444
    assert coerce_code("S", 200) == 200
    assert coerce_code("W", 202) == 202
    assert coerce_code("U", 0) == 0
