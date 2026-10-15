# Ported from RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/Expand/trapi_query_cacher.py.
# Changes from upstream:
#   - storage is Shepherd's Redis data store (DEC-18) instead of a SQLite index next to
#     this file plus gzip pickles: each entry is a JSON record (upstream's KPQuery
#     columns) and a zstd-compressed JSON response (stdlib json, which also takes the
#     numpy floats in ARAX envelopes, as pickle did), keyed by the same query hash;
#     kp_query_id comes from a counter and an id -> hash index. Every arax worker
#     replica and the server share it, and it survives restarts
#   - entries expire settings.arax_kp_cache_ttl_sec after their last request (the TTL is
#     renewed on each hit, not by a refresh), since the store outlives any one process;
#     upstream's cache lives until the next BackgroundTasker start clears it. The
#     BackgroundTasker-mode clear at startup is dropped for the same reason
#   - settings.arax_kp_cache_enabled = False makes every lookup miss and every store a
#     no-op (as the parity tests stub upstream's cacher)
#   - async_post_query_to_web_service verifies TLS (D-5 does not apply) and, like
#     upstream, lets aiohttp errors propagate (its requests.* except clauses never match)
#   - delete_input_query and purge_cache also delete the stored response (upstream leaves
#     the file, after which store_response for that query gives up)
#   - a data-store error in a lookup is a miss, and in a store skips it (upstream's
#     SQLite is local; a transient Redis error should not fail the KP query)
#   - total_cache_size_MiB is the stored (compressed) response size
#   - main(): the --query_number test query (hard-wired KP URLs) is dropped
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.

"""
KPQueryCacher: A Python class to cache KP TRAPI queries in Shepherd's data store.
"""

import sys
import os
import json
import hashlib
import time
from datetime import datetime
# External dependencies
import aiohttp
import requests
from typing import cast, Any, Collection

import redis
import zstandard

from shepherd_utils.config import settings
from shepherd_utils.db import _get_sync_data_db

def eprint(*args, **kwargs): print(*args, file=sys.stderr, **kwargs)

# Constants
DEBUG = False
REFRESH_TIME_LIMIT_SECONDS = 60.0
AGE_BEFORE_REFRESH_HOURS = 6.0
AGE_BEFORE_TIMEOUT_RETRY_HOURS = 0.02
NO_CACHED_RESPONSE = -2
CONNECTION_ERROR = -1
TEST_QUERY_FAILURE = False

# Keys in the data store
KEY_PREFIX = "arax_kp_cache"
IDS_KEY = f"{KEY_PREFIX}:ids"          # hash: kp_query_id -> query_hash
NEXT_ID_KEY = f"{KEY_PREFIX}:next_id"  # counter for kp_query_id

# upstream's KPQuery columns, in order
COLUMNS = [
    'kp_query_id', 'status', 'kp_curie', 'query_url', 'query_hash', 'query_object',
    'first_request_datetime', 'last_request_datetime', 'first_query_elapsed',
    'first_query_http_code', 'first_query_n_results', 'n_requests',
    'last_attempted_refresh_datetime', 'last_successful_refresh_datetime',
    'n_successful_refreshes', 'n_failed_refreshes', 'last_refresh_elapsed',
    'last_attempted_refresh_http_code', 'last_refresh_http_code', 'last_refresh_n_results',
    'n_refresh_same_results', 'n_refresh_different_results',
]


def record_key(query_hash: str) -> str:
    return f"{KEY_PREFIX}:record:{query_hash}"


def response_key(query_hash: str) -> str:
    return f"{KEY_PREFIX}:response:{query_hash}"


def _encode(obj: Any) -> bytes:
    return zstandard.compress(json.dumps(obj).encode())


def _decode(blob: bytes) -> Any:
    return json.loads(zstandard.decompress(blob))


def record_to_dict(record: dict) -> dict:
    """upstream's KPQuery.to_dict: every column, the query object redacted."""
    d = {}
    for col_name in COLUMNS:
        if col_name == 'query_object':
            d[col_name] = "<Pickled Object (Use hash to retrieve)>"
        else:
            d[col_name] = record.get(col_name)
    return d


# --- KPQueryCacher Class ---

class KPQueryCacher:
    """
    Manages caching of web queries in Shepherd's data store.
    """

    def __init__(self, mode=None):
        """
        Initializes the cacher.
        """
        self.enabled = settings.arax_kp_cache_enabled
        self.ttl = settings.arax_kp_cache_ttl_sec
        self._db = None

    @property
    def db(self):
        if self._db is None:
            self._db = _get_sync_data_db()
        return self._db

    # ------------------------------------------------------------------
    # Storage helpers (upstream: the SQLAlchemy session and cache files)
    # ------------------------------------------------------------------

    def _get_record(self, query_hash: str) -> dict | None:
        blob = self.db.get(record_key(query_hash))
        return None if blob is None else json.loads(blob)

    def _put_record(self, record: dict, keep_ttl: bool = False) -> None:
        blob = json.dumps(record)
        if keep_ttl:
            self.db.set(record_key(record['query_hash']), blob, keepttl=True)
        else:
            self.db.set(record_key(record['query_hash']), blob, ex=self.ttl)

    def _delete_record(self, record: dict) -> None:
        self.db.delete(record_key(record['query_hash']), response_key(record['query_hash']))
        self.db.hdel(IDS_KEY, str(record['kp_query_id']))

    def _get_record_by_id(self, kp_query_id) -> dict | None:
        query_hash = self.db.hget(IDS_KEY, str(kp_query_id))
        if query_hash is None:
            return None
        return self._get_record(query_hash.decode())

    def _all_records(self) -> list[dict]:
        """Every live record; index entries whose record has expired are dropped."""
        records = []
        for kp_query_id, query_hash in self.db.hgetall(IDS_KEY).items():
            record = self._get_record(query_hash.decode())
            if record is None or str(record['kp_query_id']) != kp_query_id.decode():
                # expired, or two processes stored the same query at once and
                # this id lost (the record keeps the other one)
                self.db.hdel(IDS_KEY, kp_query_id)
            else:
                records.append(record)
        records.sort(key=lambda record: record['kp_query_id'])
        return records

    def initialize_cache(self):
        """
        Wipes and re-initializes the cache.
        All records and all cached responses will be deleted.
        """
        print("Initializing cache: Wiping DB and cache directory...")
        keys = list(self.db.scan_iter(match=f"{KEY_PREFIX}:*"))
        if keys:
            self.db.delete(*keys)
        print("Cache initialization complete.")



    def _hash_query(self, query_object: dict) -> str:
        """
        Creates a stable SHA-256 hash of a query object.

        :param query_object: The dictionary to hash.
        :return: A hex digest string.
        """

        query_json = json.dumps(query_object, sort_keys=True, ensure_ascii=True).encode('utf-8')

        new_query_object = self._regularize_query_object(query_object)
        new_query_json = json.dumps(new_query_object, sort_keys=True, ensure_ascii=True).encode('utf-8')
        hash = hashlib.sha256(new_query_json).hexdigest()
        if DEBUG:
            eprint(f"%%%%%%%%%%%%%%%%%%% query_json to hash %%%%%%%%%%")
            eprint(query_json)
            eprint(f"%% to %%%%%")
            eprint(new_query_json)
            eprint(f"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
        return hash



    def _read_cache_file(self, query_hash: str) -> Any:
        """Reads a stored response (upstream: de-pickles a compressed cache file)."""
        blob = self.db.get(response_key(query_hash))
        if blob is None:
            raise FileNotFoundError(response_key(query_hash))
        return _decode(blob)



    def _write_cache_file(self, query_hash: str, data: Any, keep_ttl: bool = False):
        """Stores a response (upstream: pickles it to a compressed cache file)."""
        if keep_ttl:
            self.db.set(response_key(query_hash), _encode(data), keepttl=True)
        else:
            self.db.set(response_key(query_hash), _encode(data), ex=self.ttl)



    def _get_n_results(self, response_object: Any) -> int | None:
        """
        Heuristically determines the number of results from a response object.

        :param response_object: The JSON response from the web service.
        :return: An integer count of results, or None.
        """
        try:
            if isinstance(response_object, dict):
                if 'results' in response_object and isinstance(response_object['results'], list):
                    return len(response_object['results'])
                if 'message' in response_object and isinstance(response_object['message'], dict):
                    msg = response_object['message']
                    if 'results' in msg and isinstance(msg['results'], list):
                        return len(msg['results'])
            elif isinstance(response_object, list):
                return len(response_object)
        except Exception:
            # Fail silently and return None if structure is unexpected
            pass
        return None



    def _regularize_query_object(self, query_object: dict) -> dict | None:
        """
        Attempts to uniformly sort lists that might be in different orders.

        :param query_object: The query object to regularize.
        :return: regularized query object, or None.
        """

        if query_object is None:
            return None

        query_graph = {}
        if 'query_object' in query_object:
            query_graph = query_object['query_object']
        if 'message' in query_graph:
            query_graph = query_graph['message']
        if 'query_graph' in query_graph:
            query_graph = query_graph['query_graph']
        if 'nodes' in query_graph:
            for node_id, node in query_graph['nodes'].items():
                if 'categories' in node:
                    if node['categories'] is not None:
                        node['categories'] = sorted(node['categories'])

        return query_object



    async def get_result(self, query_url: str, query_object: dict, kp_curie: str, timeout=30, bypass_cache=False, async_session=None) -> tuple:
        """
        Looks for a cached result based on the query object.
        If found, updates access stats and returns the decompressed response.
        if not found, then perform the remote query and store the result in the cache

        :param query_object: The query object to hash and look up.
        :return: A tuple of (response_data, http_status_code, elapsed_time, error_message)
            http_status_code -1 means the cached result is a timeout
            http_status_code -2 means there is not cached result available
        """

        #### Try to get the response from the cache
        #eprint(f"*** Checking cache for query with {kp_curie} to {query_url}")
        if bypass_cache:
            eprint(f"*** Bypassing cache by user request")
        else:
            response_data, http_code, elapsed_time, error = self.get_cached_result(query_url, query_object)
            if http_code != NO_CACHED_RESPONSE:
                #eprint("*** Found cached result")
                return response_data, http_code, elapsed_time, 'from cache'

        #### Else send it to the service
        #eprint(f"*** Fetch data directly from KP {query_url} using payload {query_object}, timeout={timeout}")
        try:
            response_data, http_code, elapsed_time, error = await self.async_post_query_to_web_service(query_url, query_object, timeout=timeout, async_session=async_session)
            n_results = self._get_n_results(response_data)
        except TimeoutError:
            response_data = None
            http_code = -1
            elapsed_time = timeout
            error = 'Timeout'
        #eprint(f"*** Fetched a response with http_code={http_code}, n_results={n_results} from the cache in {elapsed_time:.3f} seconds with error={error}")

        #### And store that result in the cache
        #eprint("*** Store the response in the cache")
        self.store_response(
            kp_curie=kp_curie,
            query_url=query_url,
            query_object=query_object,
            response_object=response_data,
            http_code=http_code,
            elapsed_time=elapsed_time,
            status="OK"
        )

        return response_data, http_code, elapsed_time, error



    def get_cached_result(self, query_url: str, query_object: dict) -> tuple:
        """
        Looks for a cached result based on the query object.
        If found, updates access stats and returns the decompressed response.

        :param query_object: The query object to hash and look up.
        :return: A tuple of (response_data, http_status_code, elapsed_time, error_message)
            http_status_code -1 means the cached result is a timeout
            http_status_code -2 means there is not cached result available
        """

        start_time = time.time()
        if not self.enabled:
            return None, NO_CACHED_RESPONSE, time.time() - start_time, None
        query_hash = self._hash_query( { 'query_url': query_url, 'query_object': query_object } )

        eprint(f"*** Looking for pre-existing query_url={query_url}, query_object={query_object} which yields query_hash={query_hash}")

        try:
            record = self._get_record(query_hash)
        except redis.RedisError as e:
            eprint(f"KP cache lookup failed ({type(e).__name__}: {e}); treating it as a miss")
            return None, NO_CACHED_RESPONSE, time.time() - start_time, e
        if not record or record['query_url'] != query_url:
            return None, NO_CACHED_RESPONSE, time.time() - start_time, None

        # Now try to read the file
        try:
            # Found a record, update stats (and keep it for another TTL)
            record['n_requests'] += 1
            record['last_request_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._put_record(record)

            response_data = self._read_cache_file(query_hash)
            self.db.expire(response_key(query_hash), self.ttl)
            return response_data, record['last_refresh_http_code'] or record['first_query_http_code'], time.time() - start_time, None

        except FileNotFoundError as e:
            # Cache inconsistency: DB record exists, but file is missing.
            # Delete the bad record and return None.
            eprint(f"Cache inconsistency detected. Deleting record for hash: {query_hash}")
            self._delete_record(record)
            return None, NO_CACHED_RESPONSE, time.time() - start_time, e

        except Exception as e:
            # Other read error
            eprint(f"Error reading cache file {response_key(query_hash)}: {e}")
            return None, NO_CACHED_RESPONSE, time.time() - start_time, e



    def post_query_to_web_service(self, query_url: str, query_object: dict, timeout: int = 30, async_session=None) -> tuple:
        """
        Posts the query to the remote KP.

        :param query_object: The query (request body) to send.
        :param query_url: The URL of the web service.
        :param timeout: Request timeout in seconds.
        :return: A tuple of (response_data, http_status_code, elapsed_time, error_message)
        """
        start_time = time.time()

        requests_query_url = query_url
        if TEST_QUERY_FAILURE:
            requests_query_url = requests_query_url.replace('https://dev.retriever.biothings.io', 'https://gateway.systemsbiology.net')
            eprint(f"XXXXXX POSTing to {requests_query_url} instead of {query_url}")

        try:
            response = requests.post(requests_query_url, json=query_object, timeout=timeout, headers={'accept': 'application/json'})
            elapsed = time.time() - start_time
            # Raise an exception for bad status codes (4xx, 5xx)
            response.raise_for_status()
            return response.json(), response.status_code, elapsed, None

        except requests.exceptions.HTTPError as e:
            # Got a 4xx or 5xx response
            elapsed = time.time() - start_time
            return None, e.response.status_code, elapsed, str(e)

        except requests.exceptions.RequestException as e:
            # Connection error, timeout, DNS error, etc.
            elapsed = time.time() - start_time
            return None, CONNECTION_ERROR, elapsed, str(e) # -1 for non-HTTP errors



    async def async_post_query_to_web_service(self, query_url: str, query_object: dict, timeout: int = 30, async_session=None) -> tuple:
        """
        Posts the query to the remote KP.

        :param query_object: The query (request body) to send.
        :param query_url: The URL of the web service.
        :param timeout: Request timeout in seconds.
        :return: A tuple of (response_data, http_status_code, elapsed_time, error_message)
        """
        start_time = time.time()
        requests_query_url = query_url
        if TEST_QUERY_FAILURE:
            requests_query_url = requests_query_url.replace('https://dev.retriever.biothings.io', 'https://gateway.systemsbiology.net')
            eprint(f"XXXXXX POSTing async to {requests_query_url} instead of {query_url}")

        async with aiohttp.ClientSession() as session:
            async with session.post(requests_query_url, json=query_object, timeout=timeout, headers={'accept': 'application/json'}) as response:
                elapsed = time.time() - start_time
                # Raise an exception for bad status codes (4xx, 5xx)
                response.raise_for_status()
                json_response = await response.json()
                return json_response, response.status, elapsed, None



    def store_response(self,
                       kp_curie: str,
                       query_url: str,
                       query_object: dict,
                       response_object: Any,
                       http_code: int,
                       elapsed_time: float,
                       status: str = "OK"):
        """
        Stores a new web service response in the cache.

        :param kp_curie: CURIE of the Knowledge Provider.
        :param query_url: The URL that was queried.
        :param query_object: The query object that was sent.
        :param response_object: The response object that was received.
        :param http_code: The HTTP status code from the query.
        :param elapsed_time: The time the query took.
        :param status: The initial status string (e.g., "OK" or "FAILED").
        """

        if not self.enabled:
            return

        query_hash = self._hash_query( { 'query_url': query_url, 'query_object': query_object } )

        eprint(f"*** Planning to write a cache entry for query_url={query_url}, query_object={query_object} which yields query_hash={query_hash}")

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        n_results = self._get_n_results(response_object)

        fields = dict(
            status=status,
            kp_curie=kp_curie,
            query_url=query_url,
            query_hash=query_hash,
            query_object=query_object,
            first_request_datetime=now_str,
            last_request_datetime=now_str,
            first_query_elapsed=elapsed_time,
            first_query_http_code=http_code,
            first_query_n_results=n_results,
            n_requests=1,
            n_successful_refreshes=0,
            n_failed_refreshes=0,
            n_refresh_same_results=0,
            n_refresh_different_results=0
        )

        try:
            self._store_record(query_hash, fields, response_object)
        except redis.RedisError as e:
            eprint(f"Failed to write the KP cache entry for {query_hash} ({type(e).__name__}: {e})")

    def _store_record(self, query_hash: str, fields: dict, response_object: Any) -> None:
        record = self._get_record(query_hash)
        if record is not None:
            eprint(f"There is already a cache entry for {query_hash}. Maybe some parallel job beat us to it or maybe the previous try resulted in an error")
            eprint(f"The existing record number {record['kp_query_id']} has a first_query_http_code={record['first_query_http_code']}. Update the record with new information")
            record.update(fields)
        else:
            record = {column: None for column in COLUMNS}
            record.update(fields)
            record['kp_query_id'] = int(self.db.incr(NEXT_ID_KEY))

        try:
            self._write_cache_file(query_hash, response_object)
        except Exception as e:
            eprint(f"Failed to write cache file {response_key(query_hash)}: {e}")
            return # Don't create a DB record if file write fails

        self._put_record(record)
        self.db.hset(IDS_KEY, str(record['kp_query_id']), query_hash)



    def purge_cache(self, query_url_start):
        """
        Iterates through all cached queries and purges the ones where the query_url
        begins with the provide string.
        """

        timestamp = str(datetime.now().isoformat())
        eprint(f"{timestamp}: INFO: KPQueryCacher.purge_cache: Starting cache purge with query_url partial match '{query_url_start}'")

        records = self._all_records()
        eprint(f"{timestamp}: INFO: KPQueryCacher.purge_cache: Processing {len(records)} records")

        deleted_count = 0
        for record in records:

            if record['query_url'] and record['query_url'].startswith(query_url_start):
                timestamp = str(datetime.now().isoformat())
                eprint(f"{timestamp}: INFO: KPQueryCacher.purge_cache: Deleting record {record['kp_query_id']} matching {record['query_url']}")
                self._delete_record(record)
                deleted_count += 1

        timestamp = str(datetime.now().isoformat())
        eprint(f"{timestamp}: INFO: KPQueryCacher.purge_cache: Successfully deleted {deleted_count} records.")
        return deleted_count



    def refresh_cache(self):
        """
        Iterates through all cached queries and re-queries the web service
        to refresh the data. Updates refresh statistics for each record.
        """

        #timestamp = str(datetime.now().isoformat())
        #eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Starting KP response cache refresh process")
        start_time = time.time()

        if not self.enabled:
            return

        try:
            cached_queries = [record_to_dict(record) for record in self._all_records()]
        except Exception as e:
            timestamp = str(datetime.now().isoformat())
            eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Error fetching records")
            return

        #### Sort our list to prioritize the least-recently refreshed
        for item in cached_queries:
            if item['last_attempted_refresh_datetime'] is None:
                item['last_attempted_refresh_datetime'] = ''
        cached_queries.sort(key=lambda x: x['last_attempted_refresh_datetime'])

        cached_queries_to_refresh = []
        cache_stats = { 'min_query_age': 9999999, 'max_query_age': 0.0 }
        for cached_query in cached_queries:

            #### Skip non http entries
            if not cached_query['query_url'].startswith('http'):
                continue

            time_now = datetime.now()
            time_at_last_refresh_str = cached_query['last_attempted_refresh_datetime'] or cached_query['last_successful_refresh_datetime'] or cached_query['first_request_datetime']
            time_at_last_refresh = datetime.strptime(time_at_last_refresh_str, "%Y-%m-%d %H:%M:%S")
            time_difference = time_now - time_at_last_refresh
            seconds_difference = time_difference.total_seconds()
            hours_difference = seconds_difference / 3600

            if hours_difference < cache_stats['min_query_age']:
                cache_stats['min_query_age'] = hours_difference
            if hours_difference > cache_stats['max_query_age']:
                cache_stats['max_query_age'] = hours_difference

            #eprint(f"      kp_query_id={cached_query['kp_query_id']}  Stale by {hours_difference:.3f} hours")
            add_to_list = False
            if cached_query['last_refresh_http_code'] is None and cached_query['first_query_http_code'] == -1 and hours_difference > AGE_BEFORE_TIMEOUT_RETRY_HOURS:
                add_to_list = True
            elif cached_query['last_refresh_http_code'] is not None and cached_query['last_refresh_http_code'] == -1 and hours_difference > AGE_BEFORE_TIMEOUT_RETRY_HOURS:
                add_to_list = True
            elif hours_difference > AGE_BEFORE_REFRESH_HOURS:
                add_to_list = True

            if add_to_list:
                cached_queries_to_refresh.append(cached_query)

        timestamp = str(datetime.now().isoformat())
        eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Assessed {len(cached_queries)} cached queries: min_query_age={cache_stats['min_query_age']:.3f} hr, max_query_age={cache_stats['max_query_age']:.3f} hr")
        eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: {len(cached_queries_to_refresh)} cache records stale enough to refresh")

        iquery = 0
        for cached_query in cached_queries_to_refresh:

            try:
                record = self._get_record(cached_query['query_hash'])
                if not record:
                    print(f"Skipping kp_query_id {cached_query['kp_query_id']}, record not found (deleted?).")
                    continue

                eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Refreshing query {record['kp_query_id']} ({iquery+1} of {len(cached_queries_to_refresh)}) to {record['query_url']}")

                # 1. Log the attempt
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                record['last_attempted_refresh_datetime'] = now_str

                # 2. Post the query
            except Exception as e:
                timestamp = str(datetime.now().isoformat())
                eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Error getting record to update")
                return
            response_data, status_code, elapsed, error = self.post_query_to_web_service(
                record['query_url'],
                record['query_object'],
                timeout=30
            )

            # 3. Update stats based on outcome
            try:
                record['last_refresh_elapsed'] = elapsed
                record['last_attempted_refresh_http_code'] = status_code

                if error:
                    # Refresh failed
                    status_code_message = status_code
                    if status_code_message == -1:
                        status_code_message = '30s Timeout'
                    record['status'] = f"REFRESH_FAILED: {status_code_message}"
                    record['n_failed_refreshes'] = (record['n_failed_refreshes'] or 0) + 1
                    timestamp = str(datetime.now().isoformat())
                    eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Attempt to refresh query to {record['query_url']} failed with code {status_code} after {time.time() - start_time} seconds")

                else:
                    # Refresh succeeded
                    record['status'] = "OK"
                    record['last_refresh_http_code'] = status_code
                    record['n_successful_refreshes'] = (record['n_successful_refreshes'] or 0) + 1
                    record['last_successful_refresh_datetime'] = now_str
                    record['last_refresh_n_results'] = self._get_n_results(response_data)

                    # 4. Compare results
                    try:
                        old_response_data = self._read_cache_file(record['query_hash'])
                        if old_response_data is None:
                            old_response_n_results = -1
                        elif isinstance(old_response_data, tuple):
                            old_response_n_results = old_response_data[0]['message']['results']
                        else:
                            old_response_n_results = old_response_data['message']['results']
                        if old_response_n_results == response_data['message']['results']:
                            #eprint(f"The 'result' portion of the new response is the same as the old")
                            record['n_refresh_same_results'] = (record['n_refresh_same_results'] or 0) + 1
                        else:
                            #eprint(f"The 'result' portion of the new response is the different than the old. Storing new response")
                            record['n_refresh_different_results'] = (record['n_refresh_different_results'] or 0) + 1
                            # Overwrite file with new data
                            self._write_cache_file(record['query_hash'], response_data, keep_ttl=True)
                    #except FileNotFoundError:
                    #    # File was missing, so this counts as "different"
                    #    record.n_refresh_different_results = (record.n_refresh_different_results or 0) + 1
                    #    self._write_cache_file(filepath, response_data)
                    except Exception as e:
                        print(f"Error comparing/writing cache file {response_key(record['query_hash'])}: {e}")

                # Commit changes for this single record (a refresh does not extend its life)
                self._put_record(record, keep_ttl=True)
                iquery += 1

            except Exception as e:
                timestamp = str(datetime.now().isoformat())
                eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Error updating records {e}")
                return

            #### Compute how long we've been working on refreshing, and if more than the limit, yield control again for now
            working_time = time.time() - start_time
            if working_time > REFRESH_TIME_LIMIT_SECONDS:
                timestamp = str(datetime.now().isoformat())
                eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Working time {working_time:.1f} seconds. This is greater than {REFRESH_TIME_LIMIT_SECONDS} seconds, pausing refresh process")
                break

        #timestamp = str(datetime.now().isoformat())
        #eprint(f"{timestamp}: INFO: KPQueryCacher.refresh_cache: Refresh process complete for now")



    def get_cached_input_query(self, kp_query_id: int) -> str | None:
        """
        Fetches the input query for a given kp_query_id

        :return: A dict-list representation of the KP query payload.
        """
        record = self._get_record_by_id(kp_query_id)
        if not record:
            print(f"kp_query_id {kp_query_id} not found")
            return None

        return record['query_object']



    def delete_input_query(self, kp_query_id: int) -> None:
        """
        Deletes given kp_query_id

        :return: nothing.
        """
        record = self._get_record_by_id(kp_query_id)
        if record:
            eprint(f"INFO: Deleting record for kp_query_id={kp_query_id}")
            self._delete_record(record)
            eprint(f"INFO: Done")
            return None

        eprint(f"ERROR: Unable to find record for kp_query_id={kp_query_id}")

        return None



    def dump_response(self, kp_query_id: int) -> str | None:
        """
        Dumps the response for a given kp_query_id

        :return: dict response.
        """
        record = self._get_record_by_id(kp_query_id)
        if record:
            try:
                response_data = self._read_cache_file(record['query_hash'])
                return response_data

            except FileNotFoundError as e:
                eprint(f"ERROR: Unable to read hash file {response_key(record['query_hash'])}")
                return None

        eprint(f"ERROR: Unable to find record for kp_query_id={kp_query_id}")

        return None



    def list_cached_queries(self) -> dict[str, Collection[object]]:
        """
        Generates a JSON-encoded list of all query records in the cache.
        The 'query_object' field is redacted to avoid serializing large data.

        :return: A JSON string representing the list of cached query records.
        """
        records = self._all_records() if self.enabled else []
        cached_queries = [record_to_dict(record) for record in records]

        columns_to_round = { 'query_age_hr': 3, 'first_query_elapsed': 2, 'last_refresh_elapsed': 2 }

        cache_stats = { 'n_cached_queries': len(cached_queries),
                        'age_before_refresh_hr': AGE_BEFORE_REFRESH_HOURS,
                        'min_query_age_hr': 9999999,
                        'max_query_age_hr': 0.0,
                        'http_status_codes': {} }
        time_now = datetime.now()
        for cached_query in cached_queries:

            time_at_last_refresh_str = cached_query['last_successful_refresh_datetime'] or cached_query['last_attempted_refresh_datetime'] or cached_query['first_request_datetime']
            time_at_last_refresh = datetime.strptime(time_at_last_refresh_str, "%Y-%m-%d %H:%M:%S")
            time_difference = time_now - time_at_last_refresh
            seconds_difference = time_difference.total_seconds()
            hours_difference = seconds_difference / 3600
            cached_query['query_age_hr'] = hours_difference

            for column,digits in columns_to_round.items():
                if cached_query[column] is not None:
                    cached_query[column] = round(cached_query[column], digits)

            #### Update the stats except for PathFinder which does not get updated
            if cached_query['kp_curie'] not in [ 'PathFinder', 'xDTD' ]:
                if hours_difference < cast(float, cache_stats['min_query_age_hr']):
                    cache_stats['min_query_age_hr'] = hours_difference
                if hours_difference > cast(float, cache_stats['max_query_age_hr']):
                    cache_stats['max_query_age_hr'] = hours_difference

            http_status_code = cached_query['last_refresh_http_code'] or cached_query['first_query_http_code']
            http_status_codes = cast(dict, cache_stats['http_status_codes'])
            if http_status_code not in http_status_codes:
                http_status_codes[http_status_code] = 0
            http_status_codes[http_status_code] += 1

        column_data = [
            { "key": "kp_query_id", "title": "id", "title_hover": "Integer identifier of the cached KP query" },
            { "key": "status", "title": "status", "title_hover": "Status of the cached KP query" },
            { "key": "query_age_hr", "title": "age hr", "title_hover": "Age of the cache entry in hours", "red_if_greater_than_stat": "age_before_refresh_hr" },
            { "key": "kp_curie", "title": "KP curie", "title_hover": "CURIE of the target KP", "cell_hover_key": "query_url" },
            { "key": "first_request_datetime", "title": "first datetime", "title_hover": "Datetime of the first attempt at this query" },
            { "key": "last_request_datetime", "title": "last datetime", "title_hover": "Datetime of the most recent request of this query" },
            { "key": "first_query_elapsed", "title": "first elapsed", "title_hover": "Elapsed of the first query attempt in seconds", "red_if_greater_than_value": 5 },
            { "key": "first_query_http_code", "title": "first code", "title_hover": "HTTP code of the first query attempt (-1 is a timeout)", "red_if_not_equal_to_value": 200 },
            { "key": "first_query_n_results", "title": "first n results", "title_hover": "Number of TRAPI results in first query attempt" },
            { "key": "n_requests", "title": "n requests", "title_hover": "Number of total ARAX requests for this query" },
            { "key": "last_attempted_refresh_datetime", "title": "last attempted datetime", "title_hover": "Datetime of the last attempt to refresh this query" },
            { "key": "last_successful_refresh_datetime", "title": "last success datetime", "title_hover": "Datetime of the last successful attempt to refresh this query" },
            { "key": "n_successful_refreshes", "title": "n success", "title_hover": "Number of successful refreshes" },
            { "key": "n_failed_refreshes", "title": "n failed", "title_hover": "Number of failed refreshes", "red_if_greater_than_value": 0 },
            { "key": "last_refresh_elapsed", "title": "last elapsed", "title_hover": "Elapsed time of the last successfulrefresh in seconds", "red_if_greater_than_value": 5 },
            { "key": "last_attempted_refresh_http_code", "title": "last attemptcode", "title_hover": "HTTP code of the last refresh attempt (-1 is a timeout)", "red_if_not_equal_to_value": 200 },
            { "key": "last_refresh_http_code", "title": "last code", "title_hover": "HTTP code of the last successful refresh (-1 is a timeout)", "red_if_not_equal_to_value": 200 },
            { "key": "last_refresh_n_results", "title": "last n results", "title_hover": "Number of TRAPI results in the most recent successful refresh attempt" },
            { "key": "n_refresh_same_results", "title": "n same", "title_hover": "Number of refreshes that yielded the same results as the most recent successful refresh" },
            { "key": "n_refresh_different_results", "title": "n diff", "title_hover": "Number of refreshes that yielded different results as the most recent successful refresh", "red_if_greater_than_value": 0 },
            #{ "key": "query_hash", "title": "query hash", "title_hover": "query hash" },
        ]

        cache_stats['total_cache_size_MiB'] = sum(self.db.strlen(response_key(record['query_hash'])) for record in records) / 1024 / 1024

        response = { 'cache_stats': cache_stats, 'column_data': column_data, 'cache_data': cached_queries }
        return response



############################################ Main ############################################################
#### If this class is run from the command line, allow some basic testing of the class functionality
def main():
    import argparse
    argparser = argparse.ArgumentParser(description='CLI testing of the KPQueryCacher class')
    argparser.add_argument('--verbose', action='count', help='If set, print more information about ongoing processing' )
    argparser.add_argument('--initialize_cache', action='count', help='Invoke this parameter to (re)initialize the cache')
    argparser.add_argument('--summarize', action='count', help='Summarize the queries in the cache')
    argparser.add_argument('--list', action='count', help='List all queries in the cache')
    argparser.add_argument('--show_input_query', action='store', help='Print the input query for a given kp_query_id')
    argparser.add_argument('--dump_response', action='store', help='Dump the response for a given kp_query_id')
    argparser.add_argument('--delete_query', action='store', help='Delete the given kp_query_id')
    argparser.add_argument('--delete_query_url_match', action='store', help='Delete cached queries where the query_url matches the provided string')
    argparser.add_argument('--refresh', action='count', help='Refresh all queries in the cache')
    params = argparser.parse_args()

    verbose = False
    if params.verbose:
        verbose = True

    eprint(f"Create a cacher instance")
    cacher = KPQueryCacher()

    if params.initialize_cache:
        eprint(f"(Re)initializing the query cache")
        cacher.initialize_cache()
        eprint(f"Complete")
        return

    if params.show_input_query:
        result = cacher.get_cached_input_query(params.show_input_query)
        if isinstance(result, dict):
            print(json.dumps(result, indent=2))
        else:
            print(result)
        return

    if params.delete_query:
        result = cacher.delete_input_query(params.delete_query)
        return

    if params.delete_query_url_match:
        result = cacher.purge_cache(params.delete_query_url_match)
        return

    if params.dump_response:
        response = cacher.dump_response(params.dump_response)
        print(json.dumps(response, indent=2, sort_keys=False))
        return

    if params.summarize:
        eprint("Summarize queries in the cache")
        response = cacher.list_cached_queries()
        eprint(json.dumps(response['cache_stats'], indent=2, sort_keys=True))
        for entry in response['cache_data']:
            print(f"{entry['kp_query_id']}\t{entry['kp_curie']:50s}\t{entry['first_request_datetime']}\t{entry['first_query_n_results']}\t{entry['n_requests']}\t{entry['last_refresh_n_results']}")
        return


    if params.list:
        eprint("List all queries in the cache")
        response = cacher.list_cached_queries()
        eprint(json.dumps(response['cache_stats'], indent=2, sort_keys=True))
        eprint(json.dumps(response['cache_data'], indent=2, sort_keys=False))
        return


    if params.refresh:
        eprint("Refresh all queries in the cache")
        cacher.refresh_cache()
        return


if __name__ == "__main__": main()
