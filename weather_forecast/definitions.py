"""Dagster definitions entry point.

This module wires together all assets, resources, schedules, and asset checks
for the weather forecast pipeline. Dagster discovers this module via the
[tool.dagster] section in pyproject.toml.
"""

import dagster as dg


defs = dg.Definitions(
    assets=[],
    asset_checks=[],
    schedules=[],
    resources={},
)
