# Shepherd stand-in for RTXteam/RTX @ 9485431, code/ARAX/ResponseCache/response_cache.py.
# Not a port of the storage: ARAX writes each response to MySQL + S3 (OPS-05);
# in Shepherd the response lives where every Shepherd response lives, and is
# written there by whoever ran the query (the ARAX worker saves it under the
# query's response id). This keeps the interface ARAX_query.py uses:
#   - add_new_response() assigns the id (the caller's, else a new UUID), sets
#     envelope.id to {settings.server_url}/arax/response/{id} (DEC-3) and
#     returns the id; it does not write anything itself
#   - get_response() reads a stored response back from Shepherd's data store,
#     as a dict, or returns None if there is none
# The full /response/{id} behavior (validation, ARS lookups, ...) is in the
# server's ARAX API, not here. See docs/ARAX_PORT_BASELINE.md (DEC-3, DEC-14).
import uuid
from typing import Optional

from shepherd_utils.config import settings


def response_url(response_id) -> str:
    """The URL Shepherd serves a stored ARAX response at (DEC-3)."""
    return f"{settings.server_url.rstrip('/')}/arax/response/{response_id}"


class ResponseCache:

    def __init__(self, response_id: Optional[str] = None):
        self.response_id = response_id
        self.connect()

    def connect(self):
        pass

    def disconnect(self):
        pass

    def add_new_response(self, response):
        response_id = self.response_id or str(uuid.uuid4())
        response.envelope.id = response_url(response_id)
        return response_id

    def get_response(self, response_id):
        from shepherd_utils.db import get_message_sync

        if response_id is None:
            return None
        try:
            return get_message_sync(str(response_id))
        except KeyError:
            return None
