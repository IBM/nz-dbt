import os

from dbt.tests.util import check_relations_equal
import pytest

from tests.functional.utils import run_dbt


incremental_sql = """
{{
  config(
    materialized = "incremental"
  )
}}

select * from {{ adapter.quote(this.schema) }}.{{ adapter.quote('seed') }}

{% if is_incremental() %}

    where {{ adapter.quote('id') }} > (select max({{ adapter.quote('id') }}) from {{this}})

{% endif %}
"""

materialized_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ adapter.quote(this.schema) }}.{{ adapter.quote('seed') }}
"""


@pytest.fixture(scope="class")
def models():
    return {"incremental.sql": incremental_sql, "materialized.sql": materialized_sql}


def test_varchar_widening(project):
    path = os.path.join(project.test_data_dir, "varchar10_seed.sql")
    project.run_sql_file(path)

    results = run_dbt(["run"])
    assert len(results) == 2

    check_relations_equal(project.adapter, ["seed", "incremental"])
    check_relations_equal(project.adapter, ["seed", "materialized"])

    path = os.path.join(project.test_data_dir, "varchar300_seed.sql")
    project.run_sql_file(path)

    results = run_dbt(["run"])
    assert len(results) == 2

    check_relations_equal(project.adapter, ["seed", "incremental"])
    check_relations_equal(project.adapter, ["seed", "materialized"])
