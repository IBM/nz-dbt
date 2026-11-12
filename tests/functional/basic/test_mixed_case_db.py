import os

from dbt.tests.util import get_manifest
import pytest

from tests.functional.utils import run_dbt


model_sql = """
  select 1 as id
"""


@pytest.fixture(scope="class")
def models():
    return {"model.sql": model_sql}


def test_basic(project_root, project):
    # Verify database name matches the configured value (mixed-case handling)
    assert project.database == os.getenv("NZ_TEST_DATABASE", "TESTDBTINTEGRATION")

    # Tests that a project with a single model works
    results = run_dbt(["run"])
    assert len(results) == 1
    manifest = get_manifest(project_root)
    assert "model.test.model" in manifest.nodes
    # Running a second time works
    run_dbt(["run"])
