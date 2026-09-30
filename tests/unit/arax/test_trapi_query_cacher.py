"""ARAX's KP response cache (KPQueryCacher), kept in Shepherd's data store (DEC-18).

The logic is upstream's (trapi_query_cacher.py @ 9485431): the hash, what is
cached (timeouts included), hits and misses, bypass_cache, the refresh rules
and the /status?mode=kp_cache listing. The storage is a fakeredis per test (the
autouse ``arax_kp_cache_store`` fixture in tests/conftest.py).
"""

import asyncio
import json
from datetime import datetime, timedelta

import aiohttp
import pytest

import shepherd_utils.arax.Expand.trapi_query_cacher as tqc
from shepherd_utils.arax.ARAX_messenger import ARAXMessenger
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.Expand.trapi_querier import TRAPIQuerier
from shepherd_utils.arax.Expand.trapi_query_cacher import KPQueryCacher
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph
from shepherd_utils.config import settings

URL = "http://retriever.test/query"
QUERY = {
    "message": {
        "query_graph": {
            "nodes": {
                "n0": {"ids": ["MONDO:1"]},
                "n1": {"categories": ["biolink:SmallMolecule", "biolink:Drug"]},
            },
            "edges": {"e0": {"subject": "n1", "object": "n0"}},
        }
    }
}


# As upstream, hashing sorts each qnode's categories in place, so the query
# stored (and sent to the KP) has them sorted
SORTED_QUERY = json.loads(json.dumps(QUERY))
SORTED_QUERY["message"]["query_graph"]["nodes"]["n1"]["categories"].sort()


def _response(n_results=2):
    return {
        "message": {
            "results": [{"id": i} for i in range(n_results)],
            "knowledge_graph": {"nodes": {}, "edges": {}},
        }
    }


@pytest.fixture
def kp(monkeypatch):
    """The KP behind async_post_query_to_web_service / post_query_to_web_service:
    set ``kp.reply`` to what it returns (or raises)."""

    class KP:
        reply = (_response(), 200, 0.5, None)
        calls = []

        def _answer(self, url, query):
            self.calls.append((url, json.loads(json.dumps(query))))
            if isinstance(self.reply, BaseException):
                raise self.reply
            return self.reply

    k = KP()

    async def fake_async_post(self, url, query, timeout=30, async_session=None):
        return k._answer(url, query)

    def fake_post(self, url, query, timeout=30, async_session=None):
        return k._answer(url, query)

    monkeypatch.setattr(
        KPQueryCacher, "async_post_query_to_web_service", fake_async_post
    )
    monkeypatch.setattr(KPQueryCacher, "post_query_to_web_service", fake_post)
    return k


def get_result(query=QUERY, **kw):
    return asyncio.run(
        KPQueryCacher().get_result(
            URL, json.loads(json.dumps(query)), "infores:retriever", **kw
        )
    )


def _record(store, query=QUERY, url=URL):
    query_hash = KPQueryCacher()._hash_query(
        {"query_url": url, "query_object": json.loads(json.dumps(query))}
    )
    return json.loads(store.get(tqc.record_key(query_hash))), query_hash


def _age(store, query_hash, hours, field="first_request_datetime"):
    """Pretend a record's timestamp is ``hours`` old."""
    record = json.loads(store.get(tqc.record_key(query_hash)))
    record[field] = (datetime.now() - timedelta(hours=hours)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    store.set(tqc.record_key(query_hash), json.dumps(record), keepttl=True)


def test_miss_queries_the_kp_and_a_repeat_is_served_from_the_cache(
    kp, arax_kp_cache_store
):
    data, code, _, error = get_result()
    assert (code, error) == (200, None)
    assert data == _response()

    data, code, _, error = get_result()
    assert (code, error) == (200, "from cache")
    assert data == _response()
    assert len(kp.calls) == 1

    record, _ = _record(arax_kp_cache_store)
    assert record["kp_curie"] == "infores:retriever"
    assert record["first_query_n_results"] == 2
    assert record["n_requests"] == 2
    assert record["kp_query_id"] == 1
    assert record["query_object"] == SORTED_QUERY
    assert kp.calls[0][1] == SORTED_QUERY


def test_category_order_does_not_matter(kp):
    get_result()
    reordered = json.loads(json.dumps(QUERY))
    reordered["message"]["query_graph"]["nodes"]["n1"]["categories"].reverse()
    assert get_result(reordered)[3] == "from cache"
    assert len(kp.calls) == 1


def test_a_timeout_is_cached_as_minus_one(kp):
    kp.reply = TimeoutError()
    data, code, elapsed, error = get_result(timeout=7)
    assert (data, code, elapsed, error) == (None, -1, 7, "Timeout")
    kp.reply = (_response(), 200, 0.5, None)
    data, code, _, error = get_result()
    assert (data, code, error) == (None, -1, "from cache")


def test_an_http_error_propagates_and_is_not_cached(kp, arax_kp_cache_store):
    kp.reply = aiohttp.ClientResponseError(None, (), status=503)
    with pytest.raises(aiohttp.ClientResponseError):
        get_result()
    assert list(arax_kp_cache_store.scan_iter(match="arax_kp_cache:record:*")) == []


def test_bypass_cache_queries_the_kp_and_restarts_the_record(kp, arax_kp_cache_store):
    get_result()
    get_result()
    kp.reply = (_response(5), 200, 0.5, None)
    data, code, _, error = get_result(bypass_cache=True)
    assert (code, error) == (200, None)
    assert len(kp.calls) == 2
    record, _ = _record(arax_kp_cache_store)
    assert record["n_requests"] == 1  # as upstream: the record is rewritten
    assert record["kp_query_id"] == 1
    assert record["first_query_n_results"] == 5
    assert get_result()[0] == _response(5)


def test_entries_expire_after_their_last_request(kp, arax_kp_cache_store, monkeypatch):
    monkeypatch.setattr(settings, "arax_kp_cache_ttl_sec", 1000)
    get_result()
    _, query_hash = _record(arax_kp_cache_store)
    for key in (tqc.record_key(query_hash), tqc.response_key(query_hash)):
        assert 990 < arax_kp_cache_store.ttl(key) <= 1000
        arax_kp_cache_store.expire(key, 10)
    get_result()  # a hit renews both
    for key in (tqc.record_key(query_hash), tqc.response_key(query_hash)):
        assert arax_kp_cache_store.ttl(key) > 990


def test_a_record_without_its_response_is_dropped(kp, arax_kp_cache_store):
    get_result()
    _, query_hash = _record(arax_kp_cache_store)
    arax_kp_cache_store.delete(tqc.response_key(query_hash))
    assert get_result()[3] is None  # a miss: queried again
    assert len(kp.calls) == 2


def test_disabled_cache_never_hits_or_stores(kp, arax_kp_cache_store, monkeypatch):
    monkeypatch.setattr(settings, "arax_kp_cache_enabled", False)
    get_result()
    assert get_result()[3] is None
    assert len(kp.calls) == 2
    assert list(arax_kp_cache_store.scan_iter()) == []
    assert KPQueryCacher().list_cached_queries()["cache_data"] == []


def test_refresh_requeries_stale_entries(kp, arax_kp_cache_store):
    get_result()
    _, query_hash = _record(arax_kp_cache_store)

    KPQueryCacher().refresh_cache()  # fresh: nothing to do
    assert len(kp.calls) == 1

    _age(arax_kp_cache_store, query_hash, tqc.AGE_BEFORE_REFRESH_HOURS + 1)
    KPQueryCacher().refresh_cache()  # same results
    record, _ = _record(arax_kp_cache_store)
    assert len(kp.calls) == 2
    assert record["status"] == "OK"
    assert record["n_successful_refreshes"] == 1
    assert record["n_refresh_same_results"] == 1
    assert record["last_refresh_http_code"] == 200

    _age(arax_kp_cache_store, query_hash, 7, "last_attempted_refresh_datetime")
    kp.reply = (_response(3), 200, 0.5, None)
    KPQueryCacher().refresh_cache()  # different results replace the stored ones
    record, _ = _record(arax_kp_cache_store)
    assert record["n_refresh_different_results"] == 1
    assert record["last_refresh_n_results"] == 3
    assert get_result()[0] == _response(3)


def test_refresh_failure_is_recorded(kp, arax_kp_cache_store):
    get_result()
    _, query_hash = _record(arax_kp_cache_store)
    _age(arax_kp_cache_store, query_hash, 7)
    kp.reply = (None, -1, 30.0, "timed out")
    KPQueryCacher().refresh_cache()
    record, _ = _record(arax_kp_cache_store)
    assert record["status"] == "REFRESH_FAILED: 30s Timeout"
    assert record["n_failed_refreshes"] == 1
    assert record["last_attempted_refresh_http_code"] == -1
    assert get_result()[0] == _response()  # the old response is kept


def test_refresh_retries_a_cached_timeout_after_a_minute(kp, arax_kp_cache_store):
    kp.reply = TimeoutError()
    get_result()
    _, query_hash = _record(arax_kp_cache_store)
    kp.reply = (_response(), 200, 0.5, None)
    KPQueryCacher().refresh_cache()
    assert len(kp.calls) == 1  # too recent
    _age(arax_kp_cache_store, query_hash, 2 * tqc.AGE_BEFORE_TIMEOUT_RETRY_HOURS)
    KPQueryCacher().refresh_cache()
    assert len(kp.calls) == 2


def test_refresh_does_not_extend_an_entrys_life(kp, arax_kp_cache_store, monkeypatch):
    monkeypatch.setattr(settings, "arax_kp_cache_ttl_sec", 1000)
    get_result()
    _, query_hash = _record(arax_kp_cache_store)
    for key in (tqc.record_key(query_hash), tqc.response_key(query_hash)):
        arax_kp_cache_store.expire(key, 50)
    _age(arax_kp_cache_store, query_hash, 7)
    kp.reply = (_response(3), 200, 0.5, None)
    KPQueryCacher().refresh_cache()
    for key in (tqc.record_key(query_hash), tqc.response_key(query_hash)):
        assert arax_kp_cache_store.ttl(key) <= 50


def test_refresh_skips_non_http_entries(kp):
    KPQueryCacher().store_response("xCRG", "xCRG", {"q": 1}, _response(), 200, 1.0)
    cacher = KPQueryCacher()
    record = cacher._get_record_by_id(1)
    record["first_request_datetime"] = "2000-01-01 00:00:00"
    cacher._put_record(record)
    cacher.refresh_cache()
    assert kp.calls == []


def test_list_cached_queries(kp, arax_kp_cache_store):
    get_result()
    kp.reply = TimeoutError()
    other = json.loads(json.dumps(QUERY))
    other["message"]["query_graph"]["nodes"]["n0"]["ids"] = ["MONDO:2"]
    get_result(other)
    KPQueryCacher().store_response(
        "PathFinder", "PathFinder", {"q": 1}, _response(1), 200, 1.0
    )

    listing = KPQueryCacher().list_cached_queries()
    stats = listing["cache_stats"]
    assert stats["n_cached_queries"] == 3
    assert stats["http_status_codes"] == {200: 2, -1: 1}
    assert stats["age_before_refresh_hr"] == 6.0
    assert stats["total_cache_size_MiB"] > 0
    assert [q["kp_query_id"] for q in listing["cache_data"]] == [1, 2, 3]
    first = listing["cache_data"][0]
    assert first["query_object"] == "<Pickled Object (Use hash to retrieve)>"
    assert first["query_age_hr"] < 0.01
    assert [c["key"] for c in listing["column_data"]][:3] == [
        "kp_query_id",
        "status",
        "query_age_hr",
    ]
    json.dumps(listing)  # served as JSON by /status


def test_expired_entries_leave_the_listing(kp, arax_kp_cache_store):
    get_result()
    _, query_hash = _record(arax_kp_cache_store)
    arax_kp_cache_store.delete(tqc.record_key(query_hash))
    assert KPQueryCacher().list_cached_queries()["cache_data"] == []
    assert arax_kp_cache_store.hlen(tqc.IDS_KEY) == 0


def test_a_duplicate_id_from_a_store_race_is_dropped(kp, arax_kp_cache_store):
    get_result()
    _, query_hash = _record(arax_kp_cache_store)
    arax_kp_cache_store.hset(tqc.IDS_KEY, "7", query_hash)  # the losing process's id
    assert [
        q["kp_query_id"] for q in KPQueryCacher().list_cached_queries()["cache_data"]
    ] == [1]
    assert arax_kp_cache_store.hkeys(tqc.IDS_KEY) == [b"1"]


def test_cli_helpers(kp, arax_kp_cache_store):
    get_result()
    cacher = KPQueryCacher()
    assert cacher.get_cached_input_query(1) == SORTED_QUERY
    assert cacher.dump_response(1) == _response()
    assert cacher.purge_cache("http://other") == 0
    assert cacher.purge_cache("http://retriever") == 1
    assert get_result()[3] is None
    cacher.delete_input_query(2)
    assert cacher.dump_response(2) is None
    cacher.initialize_cache()
    assert list(arax_kp_cache_store.scan_iter()) == []


def test_envelopes_with_numpy_numbers_are_stored(kp):
    import numpy as np

    KPQueryCacher().store_response(
        "xDTD", "xDTD", {"q": 1}, {"message": {"score": np.float64(0.5)}}, 200, 1.0
    )
    assert KPQueryCacher().get_cached_result("xDTD", {"q": 1})[0] == {
        "message": {"score": 0.5}
    }


class _FakeSelector:
    kp_urls = {"infores:retriever": "http://retriever.test"}


def _querier_query(bypass_cache=False):
    response = ARAXResponse()
    ARAXMessenger().create_envelope(response)
    querier = TRAPIQuerier(
        response_object=response,
        kp_name="infores:retriever",
        user_specified_kp=False,
        kp_timeout=None,
        bypass_cache=bypass_cache,
        kp_selector=_FakeSelector(),
    )
    qg = QueryGraph.from_dict(
        {
            "nodes": {
                "n0": {"ids": ["MONDO:1"]},
                "n1": {"categories": ["biolink:Drug"]},
            },
            "edges": {"e0": {"subject": "n1", "object": "n0"}},
        }
    )
    asyncio.run(querier._answer_query_using_kp_async(qg))
    return response.query_plan["qedge_keys"]["e0"]["infores:retriever"]


def test_expand_serves_a_repeat_kp_query_from_the_cache(kp):
    kp.reply = (_response(0), 200, 0.5, None)
    first = _querier_query()
    second = _querier_query()
    assert len(kp.calls) == 1
    assert kp.calls[0][0] == "http://retriever.test/query"
    assert first["status"] == second["status"] == "Done"
    assert " from cache in " not in first["description"]
    assert second["description"].startswith("Returned 0 edges from cache in ")

    third = _querier_query(bypass_cache=True)
    assert len(kp.calls) == 2
    assert " from cache in " not in third["description"]


def test_a_data_store_outage_does_not_fail_the_kp_query(kp, monkeypatch):
    import redis

    class DownStore:
        def __getattr__(self, name):
            def fail(*args, **kwargs):
                raise redis.ConnectionError("store down")

            return fail

    monkeypatch.setattr(tqc, "_get_sync_data_db", lambda: DownStore())
    data, code, _, error = get_result()
    assert (data, code, error) == (_response(), 200, None)
    assert len(kp.calls) == 1
