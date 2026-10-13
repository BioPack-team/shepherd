from datetime import datetime, timedelta, timezone
from pathlib import Path

from shepherd_utils.arax.xcrg.reporting import StubReporter
from shepherd_utils.arax.xcrg.retriever import Retriever_Cache

def test_basic_cache_behavior(tmp_path):
    cache = Retriever_Cache(StubReporter(), tmp_path, ttl = timedelta(weeks = 1))
    file = Path(tmp_path / "foo.json")
    content = "{'foo': 'bar'}"

    cache.write_file(file.name, content)
    assert file.exists()
    with open(file, "r", encoding = "utf-8") as f:
        assert f.read() == content

    assert cache.read_file("foo.json") == content

def test_remove_all_files(tmp_path):
    cache = Retriever_Cache(StubReporter(), tmp_path)

    cache.write_file("foo.json", "")
    assert (tmp_path / "foo.json").exists()

    cache.write_file("bar.json", "")
    assert (tmp_path / "bar.json").exists()

    cache.remove_all_files()

    assert not cache.read_file("foo.json")
    assert not (tmp_path / "foo.json").exists()

    assert not cache.read_file("bar.json")
    assert not (tmp_path / "bar.json").exists()

def test_remove_expired_files(tmp_path):
    cache = Retriever_Cache(
        StubReporter(),
        tmp_path,
        # Files are going to expire immediately
        ttl = timedelta(seconds = -1),
        cleanup_interval = timedelta(seconds = -1)
    )
    cache.write_file("foo.json", "{'foo': 'bar'}")
    assert (tmp_path / "foo.json").exists()

    assert not cache.read_file("foo.json")
    assert not (tmp_path / "foo.json").exists()

def test_write_collisions_extend_expiration(tmp_path):
    cache = Retriever_Cache(StubReporter(), tmp_path, ttl = timedelta(weeks = 1))
    cache.write_file("foo.json", "")

    # We will use a 1-minute buffer for checks
    now = datetime.now(timezone.utc)

    files = cache.get_entries()
    assert len(files) == 1
    assert now <= files[0][1] < now + timedelta(weeks = 1) + timedelta(minutes = 1)

    cache.connection.close()

    cache = Retriever_Cache(StubReporter(), tmp_path, ttl = timedelta(weeks = 2))
    cache.write_file("foo.json", "")

    files = cache.get_entries()
    assert len(files) == 1
    assert now <= files[0][1] < now + timedelta(weeks = 2) + timedelta(minutes = 1)
