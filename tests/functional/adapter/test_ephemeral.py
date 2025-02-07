import pytest
import os
import re

from dbt.tests.adapter.ephemeral.test_ephemeral import (
    TestEphemeralErrorHandling as EphemeralErrorHandling,
    TestEphemeralMulti as EphemeralMulti,
    TestEphemeralNested as EphemeralNested,
)
from dbt.tests.adapter.ephemeral.test_ephemeral import ephemeral_errors__dependent_sql, ephemeral_errors__base__base_copy_sql
from dbt.adapters.netezza.util import run_dbt, run_dbt_and_capture
from dbt.tests.util import check_relations_equal

class TestEphemeralErrorHandlingNetezza(EphemeralErrorHandling):
    ephemeral_errors__base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ this.schema }}.seed

"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": ephemeral_errors__dependent_sql,
            "base": {
                "base.sql": self.ephemeral_errors__base__base_sql,
                "base_copy.sql": ephemeral_errors__base__base_copy_sql,
            },
        }

    def test_ephemeral_error_handling(self, project):
        results = run_dbt(["run"], expect_pass=False)
        results,output=run_dbt_and_capture(["run"], expect_pass=False)
        assert "Compilation Error in model base_copy" in output


class TestEphemeralMultiNetezza(EphemeralMulti):
    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["SEED", "dependent"])
        check_relations_equal(project.adapter, ["SEED", "double_dependent"])
        check_relations_equal(project.adapter, ["SEED", "super_dependent"])
        assert os.path.exists("./target/run/test/models/double_dependent.sql")
        with open("./target/run/test/models/double_dependent.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            f'create view "{project.database}"."{project.test_schema}"."double_dependent__dbt_tmp" as ('
            "with dbt__cte__base as ("
            f"select * from {project.test_schema}.seed"
            "),  dbt__cte__base_copy as ("
            "select * from dbt__cte__base"
            ")-- base_copy just pulls from base. Make sure the listed"
            "-- graph of CTEs all share the same dbt_cte__base cte"
            "select * from dbt__cte__base where gender = 'Male'"
            "union all"
            "select * from dbt__cte__base_copy where gender = 'Female'"
            ");"
        )
        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralNestedNetezza(EphemeralNested):
    def test_ephemeral_nested(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert os.path.exists("./target/run/test/models/root_view.sql")
        with open("./target/run/test/models/root_view.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            f'create view "{project.database}"."{project.test_schema}"."root_view__dbt_tmp" as ('
            "with dbt__cte__ephemeral_level_two as ("
            f'select * from "{project.database}"."{project.test_schema}"."source_table"'
            "),  dbt__cte__ephemeral as ("
            "select * from dbt__cte__ephemeral_level_two"
            ")select * from dbt__cte__ephemeral"
            ");"
        )

        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql
