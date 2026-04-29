import os
import platform
import re
import tempfile

import pytest
import yaml

from dbt.tests.adapter.simple_seed.test_seed import (
    BaseBasicSeedTests,
    BaseSeedConfigFullRefreshOn,
)
from dbt.tests.adapter.simple_seed import seeds
from dbt.tests.util import run_dbt
from dbt.adapters.netezza.et_options_parser import ETOptions, etoptions_representer


# Netezza does not support TEXT or TIMESTAMP WITHOUT TIME ZONE.
# Also, Netezza external table loads empty CSV fields as '' not NULL,
# so replace SQL NULL with '' to match.
_netezza_expected_sql = (
    seeds.seeds__expected_sql
    .replace("TEXT", "VARCHAR(256)")
    .replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP")
    .replace(",NULL,", ",'',")
)


def _split_multi_row_insert(sql):
    """Split multi-row INSERT VALUES into individual statements for Netezza.

    Netezza does not support INSERT ... VALUES (row1), (row2), ...
    Also removes double-quoted column names since Netezza stores unquoted
    identifiers as uppercase.
    """
    statements = []
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        # Match INSERT INTO ... (cols) VALUES (row1), (row2), ...
        m = re.match(
            r"(INSERT\s+INTO\s+\S+\s*\(([^)]+)\)\s*VALUES\s*)(.*)",
            stmt,
            re.DOTALL | re.IGNORECASE,
        )
        if m:
            table_and_cols = m.group(1)
            # Remove double quotes around column names
            table_and_cols = table_and_cols.replace('"', '')
            rows_str = m.group(3)
            # Split on "),\n" pattern to get individual rows
            rows = re.findall(r"\([^)]+\)", rows_str)
            for row in rows:
                statements.append(f"{table_and_cols}{row}")
        else:
            statements.append(stmt)
    return statements


class NetezzaSeedTestBase:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        for stmt in _split_multi_row_insert(_netezza_expected_sql):
            project.run_sql(stmt)

    @pytest.fixture(autouse=True, scope="class")
    def create_et_options_file(self, project):
        """Override et_options for seed CSVs that use space-separated datetimes."""
        yaml.add_representer(ETOptions, etoptions_representer)
        options = {
            "SkipRows": "1",
            "Delimiter": "','",
            "DateDelim": "'-'",
            "DateStyle": "YMD",
            "TimeStyle": "24HOUR",
            "TimeDelim": "':'",
            "DateTimeDelim": "' '",
            "BoolStyle": "TRUE_FALSE",
            "QuotedValue": "Double",
            "MaxErrors": "1",
        }
        if platform.system() == "Windows":
            logdir = os.path.join(tempfile.gettempdir(), "DBT")
            os.makedirs(logdir, exist_ok=True)
            options["LogDir"] = f"'{logdir}'"
        et_options = ETOptions(options=options)
        with open(os.path.join(project.project_root, "et_options.yml"), "w") as f:
            yaml.dump([et_options], f, default_flow_style=False)


class TestBasicSeedTests(NetezzaSeedTestBase, BaseBasicSeedTests):
    def test_simple_seed_full_refresh_flag(self, project):
        """Netezza does not support CASCADE on DROP TABLE, so the downstream
        view survives a full-refresh seed.  Override to expect exists=True."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=True
        )


class TestSeedConfigFullRefreshOn(NetezzaSeedTestBase, BaseSeedConfigFullRefreshOn):
    def test_simple_seed_full_refresh_config(self, project):
        """Netezza does not support CASCADE on DROP TABLE, so the downstream
        view survives a full-refresh seed.  Override to expect exists=True."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)
