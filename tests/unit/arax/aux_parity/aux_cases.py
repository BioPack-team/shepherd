"""Meta-KG (AUX-01, DEC-11) and autocomplete (AUX-02) parity cases."""

import copy
import sqlite3


def base_meta_kg():
    """A Retriever-style meta-KG: some edges lack knowledge_types / attributes,
    some already carry attributes (kept), nodes partly lack attributes."""
    return {
        "nodes": {
            "biolink:SmallMolecule": {"id_prefixes": ["CHEBI", "PUBCHEM.COMPOUND"]},
            "biolink:Disease": {
                "id_prefixes": ["MONDO"],
                "attributes": [{"attribute_type_id": "biolink:description"}],
            },
            "biolink:Gene": {"id_prefixes": ["NCBIGene"]},
        },
        "edges": [
            {
                "subject": "biolink:SmallMolecule",
                "predicate": "biolink:treats",
                "object": "biolink:Disease",
            },
            {
                "subject": "biolink:SmallMolecule",
                "predicate": "biolink:affects",
                "object": "biolink:Gene",
                "knowledge_types": ["lookup", "inferred"],
                "attributes": [
                    {
                        "attribute_type_id": "biolink:knowledge_level",
                        "constraint_use": False,
                    }
                ],
            },
            {
                "subject": "biolink:Gene",
                "predicate": "biolink:gene_associated_with_condition",
                "object": "biolink:Disease",
                "qualifiers": [
                    {"qualifier_type_id": "biolink:subject_aspect_qualifier"}
                ],
            },
            {
                "subject": "biolink:SmallMolecule",
                "predicate": "biolink:applied_to_treat",
                "object": "biolink:Disease",
            },
            {
                "subject": "biolink:Gene",
                "predicate": "biolink:interacts_with",
                "object": "biolink:Gene",
                "attributes": [],
            },
        ],
    }


# (name, steps): a step is ("fetch", base meta-KG or None) followed by the
# formats to request. Each case starts with no cache and no backups.
META_KG_CASES = [
    ("full_then_simple", [(base_meta_kg(), ["full", "simple", None])]),
    ("simple_first", [(base_meta_kg(), ["simple", "full"])]),
    ("fetch_fails_no_backup", [(None, ["full"])]),
    # a successful build leaves a backup; a later refresh whose fetch fails serves it
    (
        "fetch_fails_uses_backup",
        [(base_meta_kg(), ["full"]), ("REFRESH", None), (None, ["full", "simple"])],
    ),
]

TERMS = [
    "acetaminophen",
    "Acetaminophen",
    "acetylcysteine",
    "acetone",
    "acetic acid",
    "aspirin",
    "asparaginase",
    "atorvastatin",
    "abacavir",
    "abiraterone",
    "diabetes mellitus",
    "type 2 diabetes mellitus",
    "Diabetes insipidus",
    "base excision repair",
    "phosphatase",
    "kinase",
    "lipase",
    "protease",
    "caseinate",
    "ceasefire",
    "ab",
    "a",
    'say "hi"',
    "zinc",
    "zzz top",
] + [f"acet compound {i}" for i in range(20)]

# (word, limit), run in order: the substring fragment cache carries over
AUTOCOMPLETE_STEPS = [
    ("a", 15),
    ("ab", 15),
    ("acet", 15),
    ("acet", 3),
    ("acetyl", 15),
    ("ase", 15),
    ("asel", 15),
    ("ase", 5),
    ("diab", 15),
    ("abet", 15),
    ('say "h', 15),
    ("zz", 15),
    ("nomatch", 15),
    ("ACET", 15),
    ("tase", 2),
]


def write_terms_db(path):
    con = sqlite3.connect(path)
    con.execute("DROP TABLE IF EXISTS terms")
    con.execute("CREATE TABLE terms(term VARCHAR(255))")
    con.executemany("INSERT INTO terms VALUES (?)", [(t,) for t in TERMS])
    con.commit()
    con.close()
