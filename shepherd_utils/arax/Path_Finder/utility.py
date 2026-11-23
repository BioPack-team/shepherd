# Shepherd stand-in for RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/Path_Finder/utility.py.
# Same functions and return format ("sqlite:<path>"); the paths come from
# RTXConfiguration (Shepherd's data files, DEC-6) instead of
# code/ARAX/KnowledgeSources/*. main() is dropped.
# From upstream trapi2.0 (f8d7ee8d): get_kg2c_db_path is get_info_for_overlay_db_path.
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
from shepherd_utils.arax.RTXConfiguration import RTXConfiguration


def get_info_for_overlay_db_path():
    # upstream trapi2.0 renamed get_kg2c_db_path; here both name the same tier0
    # info-for-overlay file (RTXConfiguration.kg2c_sqlite_path)
    return f"sqlite:{RTXConfiguration().kg2c_sqlite_path}"


def get_curie_ngd_path():
    return f"sqlite:{RTXConfiguration().curie_ngd_path}"


def get_curie_to_pmids_path():
    return f"sqlite:{RTXConfiguration().curie_to_pmids_path}"
