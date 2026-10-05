# Shepherd stand-in for RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/ARAX_query_tracker.py.
# Not a port: ARAX's tracker keeps its arax_query / arax_ongoing_query tables in
# MySQL/SQLite and enforces per-remote-address limits (OPS-01, ORC-14). Shepherd
# tracks queries itself (Postgres query table, span, logs), so this keeps the
# class and the methods ARAX_query.py calls, as no-ops: create_tracker_entry()
# returns None (never -999, so a query is never denied as OverLimit here).
# See docs/ARAX_PORT_BASELINE.md (DEC-3, DEC-14).


class ARAXQueryTracker:

    def create_tracker_entry(self, attributes=None):
        return None

    def update_tracker_entry(self, tracker_id, attributes=None):
        return None

    def alter_tracker_entry(self, tracker_id, attributes=None):
        return None
