import json
import pytest

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsBase,
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)
from dbt.adapters.netezza.util import run_dbt


class NetezzaBasePersistDocsBase(BasePersistDocsBase):

    _DOCS__MY_FUN_DOCS = """
{% docs my_fun_doc %}
name Column description "with double quotes"
and with 'single  quotes' as welll as other;
'''abc123'''
reserved -- characters
80% of statistics are made up on the spot
--
/* comment */
Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting

{% enddocs %}
"""

    _PROPERTIES__SCHEMA_YML = """
version: 2

models:
  - name: table_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
  - name: view_model
    description: |
      View model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting

seeds:
  - name: SEED
    description: |
      Seed model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
"""

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])
        run_dbt()

    @pytest.fixture(scope="class")
    def properties(self):
        return {

            "my_fun_docs.md": self._DOCS__MY_FUN_DOCS,
            "schema.yml": self._PROPERTIES__SCHEMA_YML,
        }

    # Override to change the column names to uppercase
    def _assert_has_table_comments(self, table_node):
        print(f'The value of table_node : {table_node}')
        table_node["columns"]["id"] = table_node["columns"]["ID"]
        table_node["columns"]["name"] = table_node["columns"]["NAME"]
        super()._assert_has_table_comments(table_node)

    # Override to change the column names to uppercase
    def _assert_has_view_comments(
        self, view_node, has_node_comments=True, has_column_comments=True
    ):
        view_node["columns"]["id"] = view_node["columns"]["ID"]
        view_node["columns"]["name"] = view_node["columns"]["NAME"]
        super()._assert_has_view_comments(
            view_node, has_node_comments, has_column_comments
        )

class NetezzaBasePersistDocs(NetezzaBasePersistDocsBase):
    # @pytest.fixture(scope="class", autouse=True)
    # def setUp(self, project):
    #     run_dbt(["seed"])
    #     run_dbt()
    def test_has_comments_pglike(self, project):
        print("The test_has_comments_pglike function is called from netezza directory!!")
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data
        assert len(catalog_data["nodes"]) == 4
        table_node = catalog_data["nodes"]["model.test.table_model"]
        view_node = self._assert_has_table_comments(table_node)

        view_node = catalog_data["nodes"]["model.test.view_model"]
        self._assert_has_view_comments(view_node)

        no_docs_node = catalog_data["nodes"]["model.test.no_docs_model"]
        self._assert_has_view_comments(no_docs_node, False, False)

class TestPersistDocsNetezza(NetezzaBasePersistDocs):
    pass



# class NetezzaBasePersistDocsColumnMissing(NetezzaBasePersistDocs, BasePersistDocsColumnMissing):
#     pass


# class TestPersistDocsColumnMissingNetezza(NetezzaBasePersistDocsColumnMissing):
#     # Override to change the column name to uppercase
#     def test_missing_column(self, project):
#         run_dbt(["docs", "generate"])
#         with open("target/catalog.json") as fp:
#             catalog_data = json.load(fp)
#         assert "nodes" in catalog_data

#         table_node = catalog_data["nodes"]["model.test.missing_column"]
#         table_id_comment = table_node["columns"]["ID"]["comment"]
#         assert table_id_comment.startswith("test id column description")


# class TestPersistDocsCommentOnQuotedColumnNetezza(BasePersistDocsCommentOnQuotedColumn):
#     pass
