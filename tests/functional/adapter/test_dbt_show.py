import pytest
from dbt.tests.adapter.dbt_show.test_dbt_show import BaseShowLimit, BaseShowSqlHeader
from dbt.adapters.netezza.util import run_dbt
from dbt.tests.adapter.dbt_show import fixtures

class TestShowSqlHeaderNetezza(BaseShowSqlHeader):
    pass


class TestShowLimitNetezza(BaseShowLimit):
    models__sample_model = """
select * from {{ ref('SAMPLE_SEED') }}
"""
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": self.models__sample_model,
            "ephemeral_model.sql": fixtures.models__ephemeral_model,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE"}}

    @pytest.mark.parametrize(
        "args,expected",
        [
            ([], 5),  # default limit
            (["--limit", 3], 3),  # fetch 3 rows
            (["--limit", -1], 7),  # fetch all rows
        ],
    )
    def test_limit(self, project, args, expected):
        print(f"trying to run build")
        run_dbt(["build"])
        dbt_args = ["show", "--inline", fixtures.models__second_ephemeral_model, *args]
        results = run_dbt(dbt_args)
        assert len(results.results[0].agate_table) == expected
        # ensure limit was injected in compiled_code when limit specified in command args
        limit = results.args.get("limit")
        if limit > 0:
            assert f"limit {limit}" in results.results[0].node.compiled_code
