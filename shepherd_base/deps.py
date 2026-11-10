"""Print Shepherd's dependencies from pyproject.toml, one per line.

    python deps.py [extra ...]

With no arguments, the base ``[project] dependencies``; with extras, those
optional-dependency groups instead. The base image installs these before any
Shepherd code is copied, so a code change never reinstalls them (#223 C11-5).
"""

import sys
import tomllib
from pathlib import Path

# Beside this script in the image (/opt/shepherd), the repo root in a checkout.
HERE = Path(__file__).resolve().parent
PYPROJECT = next(
    path
    for path in (HERE / "pyproject.toml", HERE.parent / "pyproject.toml")
    if path.exists()
)

project = tomllib.loads(PYPROJECT.read_text())["project"]
extras = sys.argv[1:]
if extras:
    groups = project.get("optional-dependencies", {})
    missing = [extra for extra in extras if extra not in groups]
    if missing:
        sys.exit(f"unknown extra(s): {', '.join(missing)}")
    requirements = [req for extra in extras for req in groups[extra]]
else:
    requirements = project["dependencies"]
print("\n".join(requirements))
