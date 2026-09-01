"""ARS message status semantics.

Ported from NCATSTranslator/Relay @ dd1e71b:
  - tr_sys/tr_ars/models.py   (Message.STATUS, Message.create, Message.to_dict)
  - tr_sys/tr_ars/signals.py  (message_post_save code coercion)
"""

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


def to_letter(status: str) -> str:
    """Long name -> letter; anything unrecognized passes through unchanged.

    Mirrors ``Message.create``, which only rewrites values that exactly match
    a long name.
    """
    return NAME_TO_STATUS.get(status, status)


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
