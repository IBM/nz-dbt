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


def _split_multi_row_insert(sql, quote_identifiers=True):
    """Split multi-row INSERT VALUES into individual statements for Netezza.

    Netezza does not support INSERT ... VALUES (row1), (row2), ...
    With the new quoting policy (identifier: True), schema names come pre-quoted
    from setUp, and we add quotes around table names and remove quotes from column names.
    """
    statements = []
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        # Match INSERT INTO ... (cols) VALUES (row1), (row2), ...
        m = re.match(
            r"(INSERT\s+INTO\s+)(\S+)(\s*\(([^)]+)\)\s*VALUES\s*)(.*)",
            stmt,
            re.DOTALL | re.IGNORECASE,
        )
        if m:
            insert_prefix = m.group(1)  # "INSERT INTO "
            table_name = m.group(2)      # '"{schema}".table'  
            cols_prefix = m.group(3)     # " (...) VALUES "
            column_list = m.group(4)     # column names
            rows_str = m.group(5)        # row data
            
            # If table name has a dot and the second part isn't quoted, quote it
            if quote_identifiers and '.' in table_name:
                parts = table_name.rsplit('.', 1)
                if not parts[1].startswith('"'):
                    table_name = f'{parts[0]}."{parts[1]}"'
            
            # Remove quotes from column names only
            column_list = column_list.replace('"', '')
            
            # Reconstruct statement
            table_and_cols = f"{insert_prefix}{table_name} ({column_list}) VALUES "
            
            # Split rows
            rows = re.findall(r"\([^)]+\)", rows_str)
            for row in rows:
                statements.append(f"{table_and_cols}{row}")
        else:
            # For CREATE TABLE statements, optionally add quotes around table name
            # Match CREATE TABLE "{schema}".table and add quotes to table
            if quote_identifiers:
                stmt = re.sub(
                    r'(CREATE\s+TABLE\s+"?\{schema\}"?\.?)([a-zA-Z0-9_]+)',
                    r'\1"\2"',
                    stmt,
                    flags=re.IGNORECASE
                )
            statements.append(stmt)
    return statements


class NetezzaSeedTestBase:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project, quote_policy_override):
        schema_quoted = quote_policy_override.get("schema", True)
        identifier_quoted = quote_policy_override.get("identifier", True)
        if schema_quoted:
            sql = _netezza_expected_sql.replace('{schema}.', '"{schema}".')
        else:
            sql = _netezza_expected_sql
        for stmt in _split_multi_row_insert(sql, quote_identifiers=identifier_quoted):
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
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
    
    def test_simple_seed_full_refresh_flag(self, project):
        """Netezza does not support CASCADE on DROP TABLE, so the downstream
        view survives a full-refresh seed.  Override to expect exists=True."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=True
        )


class TestSeedConfigFullRefreshOn(NetezzaSeedTestBase, BaseSeedConfigFullRefreshOn):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
    
    def test_simple_seed_full_refresh_config(self, project):
        """Netezza does not support CASCADE on DROP TABLE, so the downstream
        view survives a full-refresh seed.  Override to expect exists=True."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)
