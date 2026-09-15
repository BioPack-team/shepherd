"""ARS message status semantics.

Ported from NCATSTranslator/Relay @ 3e65975:
  - tr_sys/tr_ars/models.py   (Message.STATUS, Message.create, Message.to_dict)
  - tr_sys/tr_ars/signals.py  (message_post_save code coercion)

Deviation from upstream: an unrecognized status is rejected rather than
written through (``coerce_status`` / ``validate_letter``). Upstream stored
whatever it was handed, which let a bad ``tr_ars.message.status`` header
strand a message -- see docs/ARS_PARITY_REGISTER.md.
"""

from typing import Any

# The exact (letter, long name) pairs, in Django model declaration order.
STATUS = (
    ("D", "Done"),
    ("S", "Stopped"),
    ("R", "Running"),
    ("E", "Error"),
    ("W", "Waiting"),
    ("U", "Unknown"),
)

STATUS_TO_NAME = {letter: name for letter, name in STATUS}
NAME_TO_STATUS = {name: letter for letter, name in STATUS}

# The terminal set the parent-completion signal checks children against.
TERMINAL_STATUSES = {"D", "S", "E", "U"}


# Every status the column may hold. ``ars_message.status`` is CHAR(1), so a
# value outside this set is either a write error or -- worse, when it happens
# to be one character -- a row that no code path can ever move again: it is
# not terminal, so the parent never completes, and the watchdog only scans
# 'R', so nothing reaps it.
VALID_STATUSES = frozenset(letter for letter, _ in STATUS)


def to_letter(status: str) -> str:
    """Long name -> letter; anything else passes through unchanged.

    Mirrors ``Message.create``, which only rewrites values that exactly match
    a long name. Use ``coerce_status`` at any boundary where the value came
    from outside -- this function does not validate.
    """
    return NAME_TO_STATUS.get(status, status)


class InvalidStatus(ValueError):
    """A status that is neither a known letter nor a known long name."""


def coerce_status(status: Any, default: str = "U") -> str:
    """Normalize an untrusted status to a known letter, or fall back.

    The ARA result callback takes its status from the ``tr_ars.message.status``
    request header and the workers carry that value through the task payload,
    so it is caller-controlled all the way to the column. Anything
    unrecognized becomes ``default`` rather than being written verbatim.
    """
    if isinstance(status, str):
        letter = NAME_TO_STATUS.get(status, status)
        if letter in VALID_STATUSES:
            return letter
    return default


def validate_letter(status: str) -> str:
    """Return ``status`` if it is a storable letter, else raise.

    The last line of defense, called by the db layer on every write: a status
    that got past the boundary checks fails loudly here instead of landing in
    the column.
    """
    if status not in VALID_STATUSES:
        raise InvalidStatus(
            f"{status!r} is not a valid ARS message status "
            f"(expected one of {sorted(VALID_STATUSES)})"
        )
    return status


def to_name(letter: str) -> str:
    """Letter -> long name; anything unrecognized passes through unchanged.

    Mirrors ``Message.to_dict``, which only rewrites values that exactly match
    a letter code.
    """
    return STATUS_TO_NAME.get(letter, letter)


def coerce_code(status: str, code: int) -> int:
    """Apply the post_save code coercion: 'R' -> 202, 'D' -> 200.

    Every ARS message write funnels through this (the upstream signal ran on
    every save), except writes that upstream performed with
    ``_skip_post_save`` -- callers pass the un-coerced value explicitly there.
    """
    if status == "R":
        return 202
    if status == "D":
        return 200
    return code
