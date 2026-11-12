"""Functional tests for the `organize` model config (clustered base tables)."""
from dbt.tests.util import get_manifest
import pytest

from tests.functional.utils import run_dbt

organize_single_sql = """
{{ config(materialized='table', dist='id', organize='id') }}

select 1 as id, 'a' as name
"""

organize_list_sql = """
{{ config(materialized='table', dist=['id'], organize=['id', 'name']) }}

select 1 as id, 'a' as name
"""

organize_none_sql = """
{{ config(materialized='table', dist='random', organize='none') }}

select 1 as id, 'a' as name
"""


@pytest.fixture(scope="class", autouse=True)
def setup_policy(quote_policy_override):
    """Use parametrized quote policy."""
    pass


@pytest.fixture(scope="class")
def models():
    return {
        "organize_single.sql": organize_single_sql,
        "organize_list.sql": organize_list_sql,
        "organize_none.sql": organize_none_sql,
    }


def test_organize_clause_runs_against_netezza(project_root, project):
    # A syntax error in the generated ORGANIZE ON clause would make this fail
    # against the live Netezza test instance, so a successful run is the
    # primary end-to-end assertion for this feature.
    results = run_dbt(["run"])
    assert len(results) == 3

    manifest = get_manifest(project_root)
    for name in ("organize_single", "organize_list", "organize_none"):
        assert f"model.test.{name}" in manifest.nodes

    # Running again (full-refresh of existing tables) also exercises the
    # organize/drop/recreate path.
    run_dbt(["run"])
