"""
Unit tests for the quoting-aware matching logic.

Covers:
  - NetezzaRelation._is_exactish_match: case-sensitive when quoted, case-insensitive when not
  - NetezzaPath.get_part: consistent quote-stripping across all components
  - list_relations_without_caching: identifier normalisation per quoting config
"""
from multiprocessing import get_context
from unittest import mock

import agate
import pytest

from dbt.adapters.contracts.relation import ComponentName, RelationType
from dbt.adapters.netezza import Plugin as NetezzaPlugin, NetezzaAdapter
from dbt.adapters.netezza.relation import NetezzaRelation, NetezzaQuotePolicy

from tests.unit.utils import config_from_parts_or_dicts, inject_adapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_relation(schema, identifier, schema_quoted=True, identifier_quoted=True, database_quoted=True):
    """Build a NetezzaRelation with an explicit per-component quote policy."""
    return NetezzaRelation.create(
        database="testdbt",
        schema=schema,
        identifier=identifier,
        quote_policy={
            "database": database_quoted,
            "schema": schema_quoted,
            "identifier": identifier_quoted,
        },
        type=RelationType.Table,
    )


def _adapter_with_quoting(schema_quoted, identifier_quoted, database_quoted=True):
    """Return a NetezzaAdapter whose config.quoting matches the given flags."""
    project_cfg = {
        "name": "X",
        "version": "0.1",
        "profile": "test",
        "project-root": "/tmp/dbt/does-not-exist",
        "config-version": 2,
        "quoting": {
            "database": database_quoted,
            "schema": schema_quoted,
            "identifier": identifier_quoted,
        },
    }
    profile_cfg = {
        "outputs": {
            "test": {
                "type": "netezza",
                "dbname": "testdbt",
                "user": "root",
                "host": "thishostshouldnotexist",
                "pass": "password",
                "port": 5480,
                "schema": "admin",
            }
        },
        "target": "test",
    }
    config = config_from_parts_or_dicts(project_cfg, profile_cfg)
    adapter = NetezzaAdapter(config, get_context("spawn"))
    inject_adapter(adapter, NetezzaPlugin)
    return adapter


# ---------------------------------------------------------------------------
# NetezzaPath.get_part — consistent quote stripping
# ---------------------------------------------------------------------------

class TestNetezzaPathGetPart:
    def test_database_strips_quotes(self):
        rel = _make_relation("s", "t")
        path = rel.path.__class__(database='"MYDB"', schema="s", identifier="t")
        assert path.get_part(ComponentName.Database) == "MYDB"

    def test_schema_strips_quotes(self):
        rel = _make_relation('"myschema"', "t")
        assert rel.path.get_part(ComponentName.Schema) == "myschema"

    def test_identifier_strips_quotes(self):
        rel = _make_relation("s", '"myTable"')
        assert rel.path.get_part(ComponentName.Identifier) == "myTable"

    def test_none_values_returned_as_none(self):
        path = _make_relation("s", "t").path.__class__(database=None, schema=None, identifier=None)
        assert path.get_part(ComponentName.Database) is None
        assert path.get_part(ComponentName.Schema) is None
        assert path.get_part(ComponentName.Identifier) is None


# ---------------------------------------------------------------------------
# NetezzaRelation._is_exactish_match
# ---------------------------------------------------------------------------

class TestIsExactishMatch:

    # --- quoted: case-sensitive exact match ---

    def test_quoted_schema_exact_match(self):
        rel = _make_relation("myschema", "mytable", schema_quoted=True)
        assert rel._is_exactish_match(ComponentName.Schema, "myschema")

    def test_quoted_schema_rejects_different_case(self):
        rel = _make_relation("myschema", "mytable", schema_quoted=True)
        assert not rel._is_exactish_match(ComponentName.Schema, "MYSCHEMA")

    def test_quoted_identifier_exact_match(self):
        rel = _make_relation("s", "myTable", identifier_quoted=True)
        assert rel._is_exactish_match(ComponentName.Identifier, "myTable")

    def test_quoted_identifier_rejects_different_case(self):
        rel = _make_relation("s", "myTable", identifier_quoted=True)
        assert not rel._is_exactish_match(ComponentName.Identifier, "mytable")

    def test_quoted_strips_surrounding_quotes_from_value(self):
        """Values arriving with surrounding quotes (e.g. from dbt internals) must still match."""
        rel = _make_relation("myschema", "mytable", schema_quoted=True)
        assert rel._is_exactish_match(ComponentName.Schema, '"myschema"')

    # --- unquoted: case-insensitive match ---

    def test_unquoted_schema_matches_uppercase(self):
        """Netezza returns unquoted schema names as UPPERCASE from system views."""
        rel = _make_relation("myschema", "mytable", schema_quoted=False)
        assert rel._is_exactish_match(ComponentName.Schema, "MYSCHEMA")

    def test_unquoted_schema_matches_lowercase(self):
        rel = _make_relation("myschema", "mytable", schema_quoted=False)
        assert rel._is_exactish_match(ComponentName.Schema, "myschema")

    def test_unquoted_identifier_matches_uppercase(self):
        rel = _make_relation("s", "my_table", identifier_quoted=False)
        assert rel._is_exactish_match(ComponentName.Identifier, "MY_TABLE")

    # --- mixed per-component policy ---

    def test_mixed_schema_quoted_identifier_unquoted(self):
        rel = _make_relation("myschema", "my_table", schema_quoted=True, identifier_quoted=False)
        # schema: case-sensitive
        assert rel._is_exactish_match(ComponentName.Schema, "myschema")
        assert not rel._is_exactish_match(ComponentName.Schema, "MYSCHEMA")
        # identifier: case-insensitive
        assert rel._is_exactish_match(ComponentName.Identifier, "MY_TABLE")
        assert rel._is_exactish_match(ComponentName.Identifier, "my_table")


# ---------------------------------------------------------------------------
# list_relations_without_caching — identifier normalisation
# ---------------------------------------------------------------------------

class TestListRelationsNormalisation:
    """Verify that identifiers returned by the DB are normalised based on quoting config."""

    def _mock_result(self, database, schema, identifier, type_="TABLE"):
        """Build an agate.Table that mimics what the LIST_RELATIONS macro returns."""
        return agate.Table(
            rows=[(database, schema, identifier, type_)],
            column_names=["DATABASE", "SCHEMA", "NAME", "TYPE"],
        )

    @mock.patch("dbt.adapters.netezza.connections.nzpy")
    def test_unquoted_identifiers_lowercased(self, _nzpy):
        adapter = _adapter_with_quoting(schema_quoted=False, identifier_quoted=False)
        mock_result = self._mock_result("TESTDBT", "MY_SCHEMA", "MY_TABLE")

        with mock.patch.object(adapter, "execute_macro", return_value=mock_result):
            schema_rel = NetezzaRelation.create(database="TESTDBT", schema="MY_SCHEMA")
            relations = adapter.list_relations_without_caching(schema_rel)

        assert len(relations) == 1
        rel = relations[0]
        assert rel.schema == "my_schema", f"expected 'my_schema', got '{rel.schema}'"
        assert rel.identifier == "my_table", f"expected 'my_table', got '{rel.identifier}'"

    @mock.patch("dbt.adapters.netezza.connections.nzpy")
    def test_quoted_identifiers_preserved(self, _nzpy):
        adapter = _adapter_with_quoting(schema_quoted=True, identifier_quoted=True)
        # Quoted objects are stored exactly as created — mixed case is valid
        mock_result = self._mock_result("testdbt", "mySchema", "myTable")

        with mock.patch.object(adapter, "execute_macro", return_value=mock_result):
            schema_rel = NetezzaRelation.create(database="testdbt", schema="mySchema")
            relations = adapter.list_relations_without_caching(schema_rel)

        assert len(relations) == 1
        rel = relations[0]
        assert rel.schema == "mySchema", f"expected 'mySchema', got '{rel.schema}'"
        assert rel.identifier == "myTable", f"expected 'myTable', got '{rel.identifier}'"

    @mock.patch("dbt.adapters.netezza.connections.nzpy")
    def test_mixed_schema_quoted_identifier_unquoted(self, _nzpy):
        adapter = _adapter_with_quoting(schema_quoted=True, identifier_quoted=False)
        mock_result = self._mock_result("testdbt", "mySchema", "MY_TABLE")

        with mock.patch.object(adapter, "execute_macro", return_value=mock_result):
            schema_rel = NetezzaRelation.create(database="testdbt", schema="mySchema")
            relations = adapter.list_relations_without_caching(schema_rel)

        assert len(relations) == 1
        rel = relations[0]
        # schema is quoted → preserved
        assert rel.schema == "mySchema", f"expected 'mySchema', got '{rel.schema}'"
        # identifier is unquoted → lowercased
        assert rel.identifier == "my_table", f"expected 'my_table', got '{rel.identifier}'"
