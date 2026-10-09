from pathlib import Path

from shepherd_utils.arax.xcrg.ngd import get_ngd_neighbors
from shepherd_utils.arax.xcrg.reporting import StubReporter
from tests.unit.arax.xcrg_suite.utilities import make_ngd_db_file


def test_ngd(tmp_path: Path):
    ngd_db_file = make_ngd_db_file(tmp_path, [
        ("FOO:123", [["BAR:123", 0.123, [123, 456]], ["BAZ:123", 0.246, [789]]], 123),
        ("FOO:234", [["BAR:234", 0.234, [321, 654]], ["BAZ:234", 0.468, [987]]], 234)
    ])
    assert get_ngd_neighbors(ngd_db_file, StubReporter(), "FOO:123") == {
        "BAR:123": 0.123,
        "BAZ:123": 0.246
    }
    assert get_ngd_neighbors(ngd_db_file, StubReporter(), "FOO:234") == {
        "BAR:234": 0.234,
        "BAZ:234": 0.468
    }
