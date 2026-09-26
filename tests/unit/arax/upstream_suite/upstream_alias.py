"""Import upstream ARAX's module names as the Shepherd port's modules.

Upstream's tests import ARAX by its in-tree names (``from ARAX_query import
ARAXQuery``, ``import Expand.expand_utilities as eu``, ``from openapi_server.models.message
import Message``, ``from node_synonymizer import NodeSynonymizer``). This finder
resolves each such name to the port's module under ``shepherd_utils.arax`` and
registers the *same module object* under the upstream name, so a test that
monkeypatches ``ARAX_resultify.x`` patches the port's code.

Resolution: ``shepherd_utils.arax.<name>`` if it exists, else a bare name that
matches exactly one module file anywhere in the port (``node_synonymizer`` ->
``NodeSynonymizer.node_synonymizer``). Names the port does not have
(``ARAX_database_manager``, ``kp_info_cacher``: ARAX infrastructure Shepherd
replaces) are left to fail as ImportError.
"""

import importlib
import importlib.abc
import importlib.util
import os
import sys

import shepherd_utils.arax as _port

PORT = _port.__name__
PORT_DIR = os.path.dirname(_port.__file__)


def _index_bare_names() -> dict:
    found: dict = {}
    for root, dirs, files in os.walk(PORT_DIR):
        dirs[:] = [d for d in dirs if not d.startswith("__")]
        rel = os.path.relpath(root, PORT_DIR)
        prefix = "" if rel == "." else rel.replace(os.sep, ".") + "."
        for f in files:
            if f.endswith(".py") and f != "__init__.py":
                found.setdefault(f[:-3], []).append(prefix + f[:-3])
    return {name: paths[0] for name, paths in found.items() if len(paths) == 1}


BARE = _index_bare_names()


def port_name(fullname: str):
    """The port module an upstream module name stands for, or None."""
    if fullname.startswith(PORT) or fullname.split(".")[0] in sys.stdlib_module_names:
        return None
    candidate = f"{PORT}.{fullname}"
    try:
        if importlib.util.find_spec(candidate) is not None:
            return candidate
    except (ImportError, ValueError):
        pass
    if "." not in fullname and fullname in BARE:
        return f"{PORT}.{BARE[fullname]}"
    return None


class _AliasLoader(importlib.abc.Loader):
    def __init__(self, target: str):
        self.target = target

    def create_module(self, spec):
        return importlib.import_module(self.target)

    def exec_module(self, module):
        pass


class UpstreamAliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        # a real, installed package of that name wins (e.g. pathfinder, xcrg)
        if fullname.split(".")[0] in ("pathfinder", "xcrg", "biolink_helper_pkg"):
            return None
        target_name = port_name(fullname)
        if target_name is None:
            return None
        target_spec = importlib.util.find_spec(target_name)
        spec = importlib.util.spec_from_loader(
            fullname,
            _AliasLoader(target_name),
            is_package=target_spec.submodule_search_locations is not None,
        )
        if target_spec.submodule_search_locations is not None:
            spec.submodule_search_locations = list(
                target_spec.submodule_search_locations
            )
        return spec


def install() -> None:
    if not any(isinstance(f, UpstreamAliasFinder) for f in sys.meta_path):
        # before the path finders, so an upstream checkout on sys.path can't win
        sys.meta_path.insert(0, UpstreamAliasFinder())
