"""Parity tests for the Django-serializer wire envelope.

Upstream reference: NCATSTranslator/Relay @ 3e65975
  - tr_sys/tr_ars/models.py  ARSModel.to_dict (django serializers.serialize),
    Message.to_dict (status long name + inline-decompressed data),
    Actor.to_dict (adds fields.url)
  - django.core.serializers.json.DjangoJSONEncoder datetime formatting
Behavior register rows: P-ENV-1 .. P-ENV-6.
"""

import datetime
import uuid

from shepherd_utils.ars.envelope import django_datetime, message_envelope

UTC = datetime.timezone.utc


def _message_row(**overrides):
    row = {
        "id": uuid.UUID("11111111-2222-3333-4444-555555555555"),
        "name": "ars-default-agent",
        "code": 202,
        "status": "R",
        "agent": "ara-shepherd-aragorn",
        "ts": datetime.datetime(2026, 9, 1, 12, 34, 56, 789012, tzinfo=UTC),
        "updated_at": datetime.datetime(2026, 9, 1, 12, 35, 0, 0, tzinfo=UTC),
        "url": None,
        "ref": None,
        "result_count": None,
        "result_stat": None,
        "retain": False,
        "merge_semaphore": False,
        "merged_version": None,
        "merged_versions_list": None,
        "params": {"query_type": "standard"},
        "clients": [],
    }
    row.update(overrides)
    return row


def test_django_datetime_truncates_microseconds_to_milliseconds():
    """P-ENV-1: DjangoJSONEncoder keeps 3 fractional digits and uses Z."""
    dt = datetime.datetime(2026, 9, 1, 12, 34, 56, 789012, tzinfo=UTC)
    assert django_datetime(dt) == "2026-09-01T12:34:56.789Z"


def test_django_datetime_without_microseconds():
    dt = datetime.datetime(2026, 9, 1, 12, 34, 56, 0, tzinfo=UTC)
    assert django_datetime(dt) == "2026-09-01T12:34:56Z"


def test_django_datetime_naive_passthrough():
    """A naive datetime keeps isoformat with truncation, no Z appended."""
    dt = datetime.datetime(2026, 9, 1, 12, 34, 56, 789012)
    assert django_datetime(dt) == "2026-09-01T12:34:56.789"


def test_message_envelope_shape():
    """P-ENV-2: {"model": "tr_ars.message", "pk": str(uuid), "fields": {...}}"""
    env = message_envelope(_message_row())
    assert env["model"] == "tr_ars.message"
    assert env["pk"] == "11111111-2222-3333-4444-555555555555"
    fields = env["fields"]
    # Exact Django model field order (pk excluded).
    assert list(fields.keys()) == [
        "name",
        "code",
        "status",
        "agent",
        "timestamp",
        "updated_at",
        "data",
        "url",
        "ref",
        "result_count",
        "result_stat",
        "retain",
        "merge_semaphore",
        "merged_version",
        "merged_versions_list",
        "params",
        "clients",
    ]


def test_message_envelope_status_long_name():
    """P-ENV-3: fields.status carries the long name, not the letter."""
    assert message_envelope(_message_row())["fields"]["status"] == "Running"
    assert message_envelope(_message_row(status="D"))["fields"]["status"] == "Done"
    assert message_envelope(_message_row(status="E"))["fields"]["status"] == "Error"


def test_message_envelope_data_none_and_inline():
    """P-ENV-4: data is None when absent; the decompressed dict when present.

    Upstream to_dict only replaces fields.data when it is not None.
    """
    env = message_envelope(_message_row())
    assert env["fields"]["data"] is None

    payload = {"message": {"query_graph": {"nodes": {}, "edges": {}}}}
    env = message_envelope(_message_row(), data=payload)
    assert env["fields"]["data"] == payload


def test_message_envelope_fk_and_misc_fields():
    """P-ENV-5: agent is the row's agent name (where upstream carried the
    actor's int pk); ref/merged_version are str(uuid) or None; timestamps
    are Django-encoded strings; clients is a list of int pks."""
    ref = uuid.uuid4()
    mv = uuid.uuid4()
    env = message_envelope(
        _message_row(
            ref=ref,
            merged_version=mv,
            merged_versions_list=[[str(mv), "ara-aragorn"]],
            clients=[3, 9],
            result_count=12,
            result_stat={"mean": 0.5},
            retain=True,
            merge_semaphore=True,
        )
    )
    fields = env["fields"]
    assert fields["agent"] == "ara-shepherd-aragorn"
    assert fields["ref"] == str(ref)
    assert fields["merged_version"] == str(mv)
    assert fields["merged_versions_list"] == [[str(mv), "ara-aragorn"]]
    assert fields["timestamp"] == "2026-09-01T12:34:56.789Z"
    assert fields["updated_at"] == "2026-09-01T12:35:00Z"
    assert fields["clients"] == [3, 9]
    assert fields["result_count"] == 12
    assert fields["result_stat"] == {"mean": 0.5}
    assert fields["retain"] is True
    assert fields["merge_semaphore"] is True
