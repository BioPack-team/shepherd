"""Every Shepherd image builds on the shared base, in cache-friendly order
(#223 C11-5).

Each Dockerfile used to copy shepherd_utils before ``pip install .``, so any
code change reinstalled every dependency in every image; none used
``--no-cache-dir``; and ``chmod -R 777`` made /app (and, in some images, the
code) world-writable. Building the images needs Docker and the registry, so
the layout is pinned on the sources here.
"""

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
IMAGES = sorted(
    [REPO / "shepherd_server" / "Dockerfile", *REPO.glob("workers/*/Dockerfile")]
)


def _instructions(path):
    """The Dockerfile's instructions, continuation lines joined."""
    text = re.sub(r"\\\n\s*", " ", path.read_text())
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _index(instructions, pattern):
    for i, line in enumerate(instructions):
        if re.search(pattern, line):
            return i
    raise AssertionError(f"no instruction matches {pattern!r}")


@pytest.mark.parametrize("dockerfile", IMAGES, ids=lambda p: p.parent.name)
def test_images_build_on_the_base_in_cache_friendly_order(dockerfile):
    steps = _instructions(dockerfile)

    assert steps[0] == "FROM shepherd_base"
    code = _index(steps, r"^COPY \./shepherd_utils ")
    # Shepherd itself is installed without its dependencies (the base has them)
    install = _index(steps, r"^RUN pip install (--no-deps -e|--no-deps) \.$")
    assert code < install
    # the image's own requirements come before any Shepherd code ...
    if any(re.match(r"COPY \S+/requirements\.txt ", step) for step in steps):
        assert _index(steps, r"^RUN pip install .*-r requirements\.txt") < code
    # ... and the release version after it, so neither invalidates the layers
    # above them
    assert _index(steps, r"^ARG SHEPHERD_VERSION") > install
    assert steps[-1].startswith("CMD ")
    assert any(step == "USER nru" for step in steps)


@pytest.mark.parametrize(
    "dockerfile",
    [*IMAGES, REPO / "shepherd_base" / "Dockerfile"],
    ids=lambda p: p.parent.name,
)
def test_no_image_is_world_writable(dockerfile):
    assert not [s for s in _instructions(dockerfile) if "chmod -R 777" in s]


def test_the_base_installs_dependencies_without_code_or_cache():
    steps = _instructions(REPO / "shepherd_base" / "Dockerfile")
    joined = "\n".join(steps)
    assert "PIP_NO_CACHE_DIR=1" in joined
    assert "shepherd_utils" not in joined
    assert _index(steps, r"deps\.py > /tmp/requirements\.txt")


def test_every_compose_image_gets_the_base_context():
    compose = yaml.safe_load((REPO / "compose.yml").read_text())
    for name, service in compose["services"].items():
        build = service.get("build")
        if not isinstance(build, dict):
            continue
        if not (REPO / build["dockerfile"]).read_text().count("FROM shepherd_base"):
            continue
        assert build.get("additional_contexts", {}).get("shepherd_base") == (
            "service:shepherd_base"
        ), name
    assert compose["services"]["shepherd_base"]["profiles"] == ["base"]


def test_the_release_builds_every_image_on_this_releases_base():
    workflow = yaml.safe_load(
        (REPO / ".github" / "workflows" / "release.yml").read_text()
    )
    jobs = workflow["jobs"]
    assert "push_base_to_registry" in jobs
    for name in ("push_server_to_registry", "push_workers_to_registry"):
        assert jobs[name]["needs"] == "push_base_to_registry"
        (push,) = [
            step
            for step in jobs[name]["steps"]
            if step.get("uses", "").startswith("docker/build-push-action")
        ]
        assert "shepherd_base=docker-image://" in push["with"]["build-contexts"]
        assert ":${{ steps.get_version.outputs.VERSION }}" in (
            push["with"]["build-contexts"]
        )


def test_deps_lists_pyproject_dependencies():
    import subprocess
    import sys
    import tomllib

    project = tomllib.loads((REPO / "pyproject.toml").read_text())["project"]
    out = subprocess.run(
        [sys.executable, str(REPO / "shepherd_base" / "deps.py")],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    assert out == project["dependencies"]
    extra = subprocess.run(
        [sys.executable, str(REPO / "shepherd_base" / "deps.py"), "arax-api"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    assert extra == project["optional-dependencies"]["arax-api"]
