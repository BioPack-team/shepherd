from pathlib import Path

from shepherd_utils.arax.xcrg.config import XCRGConfig
from shepherd_utils.arax.xcrg.pmid import get_curie_pmids
from shepherd_utils.arax.xcrg.reporting import StubReporter
from tests.unit.arax.xcrg_suite.utilities import make_curie_to_pmids_db


def test_pmids(tmp_path: Path, config: XCRGConfig):
    db_file = make_curie_to_pmids_db(tmp_path, {
        "FOO:1234": [2018915346, 2271560481],
        "FOO:5678": [4023233424, 4275878409]
    })
    assert get_curie_pmids(db_file, StubReporter(), "FOO:1234") == {"2018915346", "2271560481"}
    assert get_curie_pmids(db_file, StubReporter(), "FOO:5678") == {"4023233424", "4275878409"}
