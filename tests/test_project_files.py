"""Tests for project setup files: constraints.txt pins match pyproject.toml."""

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from weather_forecast.utils.helpers import get_project_root

try:
    import tomllib
except ImportError:  # Python 3.10
    import tomli as tomllib

ROOT = get_project_root()


def _declared_requirements() -> list[Requirement]:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    specs = [*project["dependencies"], *project["optional-dependencies"]["dev"]]
    return [Requirement(spec) for spec in specs]


def _constraints() -> list[Requirement]:
    lines = (ROOT / "constraints.txt").read_text().splitlines()
    return [Requirement(line) for line in lines if line.strip() and not line.startswith("#")]


def _pins() -> dict[str, str]:
    """Pinned version per package for the running Python (markers evaluated)."""
    pins = {}
    for req in _constraints():
        if req.marker is None or req.marker.evaluate():
            name = canonicalize_name(req.name)
            assert name not in pins, f"{req.name} pinned twice for this Python"
            pins[name] = next(iter(req.specifier)).version
    return pins


def test_constraints_are_exact_pins():
    constraints = _constraints()

    assert constraints
    assert [str(r) for r in constraints if [s.operator for s in r.specifier] != ["=="]] == []


@pytest.mark.parametrize("requirement", _declared_requirements(), ids=str)
def test_every_dependency_is_pinned_within_its_bounds(requirement):
    # A dependency added to pyproject.toml must also get a tested version in constraints.txt
    version = _pins().get(canonicalize_name(requirement.name))

    assert version is not None, f"{requirement.name} missing from constraints.txt"
    assert requirement.specifier.contains(version), f"{requirement.name}=={version}"
