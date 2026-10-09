"""Helpers for xCRG's unit tests.

Port of upstream xCRG's ``tests/utilities.py`` (Translator-CATRAX/xCRG @ e67f2c0),
keeping only what the ported unit tests use. The live-query helpers
(``make_xcrg_query``, ``find_*``, ``assert_answer``, ``XCRG_Answer``) serve
upstream's tests/arax/ and tests/integration/, which are not vendored.
"""

import json
import sqlite3
from pathlib import Path

from translator_tom import CURIE


def make_ngd_db_file(tmp_dir: Path, curie_ngd: list[tuple[CURIE, list, int]]):
    db_file = tmp_dir / "curie_ngd.sqlite"

    data = [
        (curie, json.dumps(ngd), pmid_length) for (curie, ngd, pmid_length) in curie_ngd
    ]

    with sqlite3.connect(db_file) as db:
        db.execute("""
            CREATE TABLE curie_ngd (
                curie TEXT PRIMARY KEY,
                ngd TEXT NOT NULL,
                pmid_length INTEGER NOT NULL
            )
        """)
        db.executemany(
            "INSERT INTO curie_ngd (curie, ngd, pmid_length) VALUES (?, ?, ?)", data
        )
        db.commit()

    return db_file


def make_curie_to_pmids_db(tmp_dir: Path, curies_to_pmids: dict[CURIE, list[int]]):
    db_file = tmp_dir / "curie_to_pmids.sqlite"

    data = list[tuple[str, bytearray]]()
    for curie in curies_to_pmids:
        ba = bytearray()
        for pmid in curies_to_pmids[curie]:
            ba.extend(pmid.to_bytes(4, byteorder="little"))
        data.append((curie, ba))

    with sqlite3.connect(db_file) as db:
        db.execute("""
            CREATE TABLE curie_to_pmids (
                curie TEXT PRIMARY KEY,
                pmids BLOB NOT NULL
            )
        """)
        db.executemany("INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)", data)
        db.commit()

    return db_file
