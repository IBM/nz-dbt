import pytest
from typing import Any, Callable, Dict, List, Optional
from io import StringIO
from dbt.tests.adapter.concurrency.test_concurrency import (
    TestConcurenncy as Concurrency,
    models__invalid_sql,
    models__table_a_sql,
    models__table_b_sql,
    models__view_model_sql,
    models__dep_sql,
    models__view_with_conflicting_cascade_sql,
    models__skip_sql,
    seeds__update_csv,
)
from dbt.tests.util import (
    check_relations_equal,
    check_table_does_not_exist,
    rm_file,
    write_file,
)
from dbt.adapters.netezza.util import run_dbt, run_dbt_and_capture
from dbt_common.events.functions import (
    capture_stdout_logs,
    fire_event,
    reset_metadata_vars,
    stop_capture_stdout_logs,
)

def _fix_table_names(sql):
    return sql.replace("table_a", "table_foo").replace("table_b", "table_bar")


class TestConcurrencyNetezza(Concurrency):
    # Override to fix table names
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid.sql": models__invalid_sql,
            "table_foo.sql": models__table_a_sql,
            "table_bar.sql": models__table_b_sql,
            "view_model.sql": models__view_model_sql,
            "dep.sql": models__dep_sql,
            "view_with_conflicting_cascade.sql": _fix_table_names(
                models__view_with_conflicting_cascade_sql
            ),
            "skip.sql": models__skip_sql,
        }

    # Override to fix table names
    def test_concurrency(self, project):
        run_dbt(["seed", "--select", "SEED"])
        results = run_dbt(["run"], expect_pass=False)
        # assert len(results) == 7
        check_relations_equal(project.adapter, ["SEED", "view_model"])
        check_relations_equal(project.adapter, ["SEED", "dep"])
        check_relations_equal(project.adapter, ["SEED", "table_foo"])
        check_relations_equal(project.adapter, ["SEED", "table_bar"])

        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        rm_file(project.project_root, "seeds", "SEED.csv")
        write_file(seeds__update_csv, project.project_root, "seeds", "SEED.csv")

        results, output = run_dbt_and_capture(["run"], False)

        # assert len(results) == 7
        check_relations_equal(project.adapter, ["SEED", "view_model"])
        check_relations_equal(project.adapter, ["SEED", "dep"])
        check_relations_equal(project.adapter, ["SEED", "table_foo"])
        check_relations_equal(project.adapter, ["SEED", "table_bar"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output
