# Shepherd stand-in for RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/Path_Finder/utility.py.
# Same functions and return format ("sqlite:<path>"); the paths come from
# RTXConfiguration (Shepherd's data files, DEC-6) instead of
# code/ARAX/KnowledgeSources/*. main() is dropped.
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
from shepherd_utils.arax.RTXConfiguration import RTXConfiguration


def get_kg2c_db_path():
    return f"sqlite:{RTXConfiguration().kg2c_sqlite_path}"


def get_curie_ngd_path():
    return f"sqlite:{RTXConfiguration().curie_ngd_path}"


def get_curie_to_pmids_path():
    return f"sqlite:{RTXConfiguration().curie_to_pmids_path}"
