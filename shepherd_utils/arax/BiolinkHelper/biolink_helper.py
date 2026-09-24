# Ported from RTXteam/RTX @ 9485431, code/ARAX/BiolinkHelper/biolink_helper.py.
# Changes from upstream:
#   - import paths / sys.path hacks only
#   - the Biolink version is ARAX's pinned 4.2.5 (RTXConfiguration.BIOLINK_VERSION) instead of being read from ARAX's OpenAPI YAML/JSON
#   - the lookup-map cache lives in settings.arax_biolink_cache_dir (writable) instead of beside this file
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
"""
Usage:  python biolink_helper.py [biolink version number, e.g. 3.0.3]
"""

import datetime
import json
import os
import pathlib
import sys
from typing import Optional

import yaml
from biolink_helper_pkg import BiolinkHelper

from shepherd_utils.arax.RTXConfiguration import BIOLINK_VERSION
from shepherd_utils.config import settings


def eprint(*args, **kwargs): print(*args, file=sys.stderr, **kwargs)


def get_biolink_helper(biolink_version: Optional[str] = None):
    timestamp = str(datetime.datetime.now().isoformat())
    eprint(f"{timestamp}: DEBUG: In BiolinkHelper init")

    biolink_version = biolink_version if biolink_version else get_current_arax_biolink_version()
    if biolink_version == "4.2.0":
        eprint(f"{timestamp}: DEBUG: Overriding Biolink version from 4.2.0 to 4.2.1 due to issues with "
               f"treats predicates in 4.2.0")
        biolink_version = "4.2.1"

    biolink_helper_dir = settings.arax_biolink_cache_dir
    os.makedirs(biolink_helper_dir, exist_ok=True)

    return BiolinkHelper(biolink_version, biolink_helper_dir)


def get_current_arax_biolink_version() -> str:
    """
    Returns the current Biolink version that the ARAX system is using, according to the OpenAPI YAML file.
    """
    return BIOLINK_VERSION



