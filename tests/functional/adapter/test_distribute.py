"""Functional tests for the `dist` model config (distribution keys)."""
from dbt.tests.util import get_manifest
import pytest

from tests.functional.utils import run_dbt

dist_random_sql = """
{{ config(materialized='table', dist='random') }}

select 1 as id, 'a' as name
"""

dist_column_sql = """
{{ config(materialized='table', dist='id') }}

select 1 as id, 'a' as name
"""

dist_list_sql = """
{{ config(materialized='table', dist=['id', 'name']) }}

select 1 as id, 'a' as name
"""


@pytest.fixture(scope="class", autouse=True)
def setup_policy(quote_policy_override):
    """Use parametrized quote policy."""
    pass


@pytest.fixture(scope="class")
def models():
    return {
        "dist_random.sql": dist_random_sql,
        "dist_column.sql": dist_column_sql,
        "dist_list.sql": dist_list_sql,
    }


def test_dist_clause_runs_against_netezza(project_root, project):
    # A syntax error in the generated DISTRIBUTE ON clause would make this
    # fail against the live Netezza test instance, so a successful run is the
    # primary end-to-end assertion for this feature.
    results = run_dbt(["run"])
    assert len(results) == 3

    manifest = get_manifest(project_root)
    for name in ("dist_random", "dist_column", "dist_list"):
        assert f"model.test.{name}" in manifest.nodes

    # Running again (full-refresh of existing tables) also exercises the
    # distribute/drop/recreate path.
    run_dbt(["run"])
