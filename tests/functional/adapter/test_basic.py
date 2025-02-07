import os
import pytest

from datetime import datetime

from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols, check_relation_rows
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    BaseGenerateProject,
    seed__seed_csv,
    write_project_files,
    models__readme_md,
    models__model_sql,
    ref_models__schema_yml,
    ref_models__view_summary_sql,
    ref_models__ephemeral_summary_sql,
    ref_models__ephemeral_copy_sql,
    ref_models__docs_md,
    verify_catalog,
)
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


from dbt.tests.util import (
    check_relations_equal,
    relation_from_name,
    rm_file
)
from dbt.tests.adapter.basic.files import (
    schema_base_yml,
)
from dbt.adapters.netezza.util import run_dbt
from dbt.tests.util import check_result_nodes_by_name, get_manifest, update_rows


# @pytest.mark.skip("Fails to rename view to a table due to 'alter table' SQL")
# # TODO Implement get_rename_view_sql in dbt-1.7 and enable test
# class TestSimpleMaterializationsNetezza(BaseSimpleMaterializations):
#     pass


# class TestTableMaterializationNetezza(BaseTableMaterialization):
#     def test_table_materialization_sort_dist_no_op(self, project):
#         # basic table materialization test, sort and dist is not supported by postgres so the result table would still be same as input

#         # check seed
#         results = run_dbt(["seed"])
#         print(f"The value of seed res : {len(results)}")
#         assert len(results) == 1

#         # check run
#         results = run_dbt(["run"])
#         print(f"The value of seed runs : {len(results)}")
#         assert len(results) == 1

#         check_relations_equal(project.adapter, ["SEED", "materialized"])


# # Expected compilation error
# # TODO
# class TestSingularTestsNetezza(BaseSingularTests):
#     @pytest.fixture(autouse=True)
#     def clean_up(self, project):
#         pass


# class TestSingularTestsEphemeralNetezza(BaseSingularTestsEphemeral):
#     @pytest.fixture(scope="class")
#     def seeds(self):
#         return {
#             "base.csv": files.seeds_base_csv,
#         }

#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "ephemeral.sql": files.ephemeral_with_cte_sql,
#             "passing_model.sql": files.test_ephemeral_passing_sql,
#             "failing_model.sql": files.test_ephemeral_failing_sql,
#             "schema.yml": files.schema_base_yml,
#         }

#     @pytest.fixture(scope="class")
#     def tests(self):
#         return {
#             "passing.sql": files.test_ephemeral_passing_sql,
#             "failing.sql": files.test_ephemeral_failing_sql,
#         }

#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "name": "singular_tests_ephemeral",
#         }

#     @pytest.fixture(autouse=True)
#     def clean_up(self, project):
#         pass
#         # yield
#         # with project.adapter.connection_named("__test"):
#         #     relation = project.adapter.Relation.create(
#         #         database=project.database, schema=project.test_schema
#         #     )
#         #     project.adapter.drop_schema(relation)

#     pass

#     def test_singular_tests_ephemeral(self, project):
#         # check results from seed command
#         results = run_dbt(["seed"])
#         assert len(results) == 1
#         check_result_nodes_by_name(results, ["BASE"])

#         # check results from run command
#         results = run_dbt()
#         assert len(results) == 2
#         check_result_nodes_by_name(results, ["failing_model", "passing_model"])

#         # # Check results from test command
#         # TODO : Check why the test cases for failing are giving error
#         # print("The test is being run")
#         # results = run_dbt(["test"], expect_pass=False)
#         # assert len(results) == 2
#         # check_result_nodes_by_name(results, ["passing", "failing"])

#         # Check result status
#         for result in results:
#             if result.node.name == "passing":
#                 assert result.status == "pass"
#             elif result.node.name == "failing":
#                 assert result.status == "fail"


# class TestEmptyNetezza(BaseEmpty):
#     pass


# class TestEphemeralNetezza(BaseEphemeral):
#     def test_ephemeral(self, project):
#         # seed command
#         results = run_dbt(["seed"])
#         assert len(results) == 1
#         check_result_nodes_by_name(results, ["BASE"])

#         # run command
#         results = run_dbt(["run"])
#         assert len(results) == 2
#         check_result_nodes_by_name(results, ["view_model", "table_model"])

#         # base table rowcount
#         relation = relation_from_name(project.adapter, "BASE")
#         result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
#         assert result[0] == 10

#         # relations equal
#         check_relations_equal(project.adapter, ["BASE", "view_model", "table_model"])

#         # catalog node count
#         catalog = run_dbt(["docs", "generate"])
#         catalog_path = os.path.join(project.project_root, "target", "catalog.json")
#         assert os.path.exists(catalog_path)
#         assert len(catalog.nodes) == 3
#         assert len(catalog.sources) == 1

#         # manifest (not in original)
#         manifest = get_manifest(project.project_root)
#         assert len(manifest.nodes) == 4
#         assert len(manifest.sources) == 1


# class TestIncrementalNetezza(BaseIncremental):
#     incremental_sql = """
#     {{ config(materialized="incremental", unique_key="id") }}
#     select
#         id,
#         name::varchar(255) as name,
#         some_date
#     from
#         {{ source('raw', 'seed') }}
#     """.strip()

#     @pytest.fixture(scope="class")
#     def models(self):
#         return {"incremental.sql": self.incremental_sql, "schema.yml": schema_base_yml}

#     @pytest.fixture(autouse=True)
#     def clean_up(self, project):
#         pass
#         # yield
#         # with project.adapter.connection_named("__test"):
#         #     relation = project.adapter.Relation.create(
#         #         database=project.database, schema=project.test_schema
#         #     )
#         #     project.adapter.drop_schema(relation)

#     def test_incremental(self, project):
#         # seed command
#         results = run_dbt(["seed"])
#         assert len(results) == 2

#         # base table rowcount
#         relation = relation_from_name(project.adapter, "BASE")
#         result = project.run_sql(
#             f"select count(*) as num_rows from {relation}", fetch="one"
#         )
#         assert result[0] == 10

#         # added table rowcount
#         relation = relation_from_name(project.adapter, "ADDED")
#         result = project.run_sql(
#             f"select count(*) as num_rows from {relation}", fetch="one"
#         )
#         assert result[0] == 20

#         # run command
#         # the "seed_name" var changes the seed identifier in the schema file
#         results = run_dbt(["run", "--vars", "seed_name: BASE"])
#         assert len(results) == 1

#         # check relations equal
#         check_relations_equal(project.adapter, ["BASE", "incremental"])

#         # change seed_name var
#         # the "seed_name" var changes the seed identifier in the schema file
#         results = run_dbt(["-d", "run", "--vars", "seed_name: ADDED"])
#         assert len(results) == 1

#         # check relations equal
#         check_relations_equal(project.adapter, ["ADDED", "incremental"])


# class TestIncrementalNotSchemaChangeNetezza(BaseIncrementalNotSchemaChange):
#     pass


# class TestGenericTestsNetezza(BaseGenericTests):
#     @pytest.fixture(autouse=True)
#     def clean_up(self, project):
#         # yield
#         # with project.adapter.connection_named("__test"):
#         #     relation = project.adapter.Relation.create(
#         #         database=project.database, schema=project.test_schema
#         #     )
#         #     project.adapter.drop_schema(relation)
#         pass

#     def test_generic_tests(self, project):
#         # seed command
#         results = run_dbt(["seed"])

#         # test command selecting base model
#         results = run_dbt(["test", "-m", "BASE"])
#         assert len(results) == 1

#         # run command
#         results = run_dbt(["run"])
#         assert len(results) == 2

#         # test command, all tests
#         results = run_dbt(["test"])
#         assert len(results) == 3


# class NetezzaSnapshotSeedConfig:
#     # Override to set varchar widths to allow snapshot to store longer values
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "seeds": {
#                 "test": {
#                     "BASE": {"+column_types": {"name": "varchar(100)"}},
#                     "ADDED": {"+column_types": {"name": "varchar(100)"}},
#                 }
#             }
#         }


# class TestSnapshotCheckColsNetezza(NetezzaSnapshotSeedConfig, BaseSnapshotCheckCols):
#     @pytest.fixture(autouse=True)
#     def clean_up(self, project):
#         # yield
#         # with project.adapter.connection_named("__test"):
#         #     relation = project.adapter.Relation.create(
#         #         database=project.database, schema=project.test_schema
#         #     )
#         #     project.adapter.drop_schema(relation)
#         pass

#     def test_snapshot_check_cols(self, project):
#         # seed command
#         results = run_dbt(["seed"])
#         assert len(results) == 2

#         # snapshot command
#         results = run_dbt(["snapshot"])
#         for result in results:
#             assert result.status == "success"

#         # check rowcounts for all snapshots
#         check_relation_rows(project, "cc_all_snapshot", 10)
#         check_relation_rows(project, "cc_name_snapshot", 10)
#         check_relation_rows(project, "cc_date_snapshot", 10)

#         relation = relation_from_name(project.adapter, "cc_all_snapshot")
#         result = project.run_sql(f"select * from {relation}", fetch="all")

#         # point at the "added" seed so the snapshot sees 10 new rows
#         results = run_dbt(["--no-partial-parse", "snapshot", "--vars", "seed_name: ADDED"])
#         for result in results:
#             assert result.status == "success"

#         # check rowcounts for all snapshots
#         check_relation_rows(project, "cc_all_snapshot", 20)
#         check_relation_rows(project, "cc_name_snapshot", 20)
#         check_relation_rows(project, "cc_date_snapshot", 20)

#         # update some timestamps in the "added" seed so the snapshot sees 10 more new rows
#         update_rows_config = {
#             "name": "ADDED",
#             "dst_col": "some_date",
#             "clause": {"src_col": "some_date", "type": "add_timestamp"},
#             "where": "id > 10 and id < 21",
#         }
#         update_rows(project.adapter, update_rows_config)

#         # re-run snapshots, using "added'
#         results = run_dbt(["snapshot", "--vars", "seed_name: ADDED"])
#         for result in results:
#             assert result.status == "success"

#         # check rowcounts for all snapshots
#         check_relation_rows(project, "cc_all_snapshot", 30)
#         check_relation_rows(project, "cc_date_snapshot", 30)
#         # unchanged: only the timestamp changed
#         check_relation_rows(project, "cc_name_snapshot", 20)

#         # Update the name column
#         update_rows_config = {
#             "name": "ADDED",
#             "dst_col": "name",
#             "clause": {
#                 "src_col": "name",
#                 "type": "add_string",
#                 "value": "_updated",
#             },
#             "where": "id < 11",
#         }
#         update_rows(project.adapter, update_rows_config)

#         # re-run snapshots, using "added'
#         results = run_dbt(["snapshot", "--vars", "seed_name: ADDED"])
#         for result in results:
#             assert result.status == "success"

#         # check rowcounts for all snapshots
#         check_relation_rows(project, "cc_all_snapshot", 40)
#         check_relation_rows(project, "cc_name_snapshot", 30)
#         # does not see name updates
#         check_relation_rows(project, "cc_date_snapshot", 30)


# class TestSnapshotTimestampNetezza(NetezzaSnapshotSeedConfig, BaseSnapshotTimestamp):
#     pass


# class TestBaseAdapterMethodNetezza(BaseAdapterMethod):
#     def test_adapter_methods(self, project, equal_tables):
#         with pytest.raises(RuntimeError, match="does not support"):
#             super().test_adapter_methods(project, equal_tables)

#     @pytest.fixture(autouse=True)
#     def clean_up(self, project):
#         # yield
#         # with project.adapter.connection_named("__test"):
#         #     relation = project.adapter.Relation.create(
#         #         database=project.database, schema=project.test_schema
#         #     )
#         #     project.adapter.drop_schema(relation)
#         pass



class NetezzaGenerateProject(BaseGenerateProject):
    seed__schema_yml = """
version: 2
seeds:
  - name: seed
    description: "The test seed"
    columns:
      - name: id
        description: The user ID number
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated
"""

    snapshot__snapshot_seed_sql = """
    {% snapshot snapshot_seed %}
    {{
        config(
        unique_key='id',
        strategy='check',
        check_cols='all',
        target_schema=var('alternate_schema')
        )
    }}
    select * from {{ ref('SEED') }}
    {% endsnapshot %}
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"schema.yml": self.seed__schema_yml, "seed.csv": seed__seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_seed.sql": self.snapshot__snapshot_seed_sql}

    class AnyCharacterVarying:
        def __eq__(self, other):
            return other.startswith("CHARACTER VARYING")

    # Override to remove test schema creation
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        os.environ["DBT_ENV_CUSTOM_ENV_env_key"] = "env_value"
        assets = {"lorem-ipsum.txt": "Lorem ipsum dolor sit amet"}
        write_project_files(project.project_root, "assets", assets)
        run_dbt(["seed"])
        yield
        del os.environ["DBT_ENV_CUSTOM_ENV_env_key"]

    # Override to use the same schema for both schema vars
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "asset-paths": ["assets", "invalid-asset-paths"],
            "vars": {
                "test_schema": unique_schema,
                "alternate_schema": unique_schema,
            },
            "seeds": {"quote_columns": False, "datetimedelim": " "},
        }


# class TestDocsGenerateNetezza(NetezzaGenerateProject, BaseDocsGenerate):
#     models__schema_yml = """
# version: 2

# models:
#   - name: model
#     description: "The test model"
#     docs:
#       show: false
#     columns:
#       - name: id
#         description: The user ID number
#         data_tests:
#           - unique
#           - not_null
#       - name: first_name
#         description: The user's first name
#       - name: email
#         description: The user's email
#       - name: ip_address
#         description: The user's IP address
#       - name: updated_at
#         description: The last time this user's email was updated
#     data_tests:
#       - test.nothing

#   - name: second_model
#     description: "The second test model"
#     docs:
#       show: false
#     columns:
#       - name: id
#         description: The user ID number
#       - name: first_name
#         description: The user's first name
#       - name: email
#         description: The user's email
#       - name: ip_address
#         description: The user's IP address
#       - name: updated_at
#         description: The last time this user's email was updated


# sources:
#   - name: my_source
#     description: "My source"
#     loader: a_loader
#     schema: "{{ var('test_schema') }}"
#     tables:
#       - name: my_table
#         description: "My table"
#         identifier: seed
#         columns:
#           - name: id
#             description: "An ID field"


# exposures:
#   - name: simple_exposure
#     type: dashboard
#     depends_on:
#       - ref('model')
#       - source('my_source', 'my_table')
#     owner:
#       email: something@example.com
#   - name: notebook_exposure
#     type: notebook
#     depends_on:
#       - ref('model')
#       - ref('second_model')
#     owner:
#       email: something@example.com
#       name: Some name
#     description: >
#       A description of the complex exposure
#     maturity: medium
#     meta:
#       tool: 'my_tool'
#       languages:
#         - python
#     tags: ['my_department']
#     url: http://example.com/notebook/1
# """

#     models__second_model_sql = """
#     {{
#         config(
#             materialized='view',
#         )
#     }}

#     select * from {{ ref('SEED') }}
#     """
#     models__model_sql = """
#     {{
#         config(
#             materialized='view',
#         )
#     }}

#     select * from {{ ref('SEED') }}
#     """

#     @pytest.fixture(autouse=True)
#     def clean_up(self, project):
#         # yield
#         # with project.adapter.connection_named("__test"):
#         #     alternate_schema = f"{project.test_schema}_test"
#         #     relation = project.adapter.Relation.create(
#         #         database=project.database, schema=alternate_schema
#         #     )
#         #     project.adapter.drop_schema(relation)
#       pass

#     # Override to remove schema declaration in second_model
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "schema.yml": self.models__schema_yml,
#             "second_model.sql": self.models__second_model_sql,
#             "readme.md": models__readme_md,
#             "model.sql": self.models__model_sql,
#         }

#     # Override to modify casing and types
#     @pytest.fixture(scope="class")
#     def expected_catalog(self, project, profile_user, unique_schema):
#         print("inside the expected base catalog :")
#         print("The value of project is {project :}")
#         expected = base_expected_catalog(
#             project,
#             role=profile_user.upper(),
#             id_type="INTEGER",
#             text_type=self.AnyCharacterVarying(),
#             time_type="TIMESTAMP",
#             view_type="VIEW",
#             table_type="TABLE",
#             model_stats=no_stats(),
#             case=None,
#             case_columns=str.upper,
#         )
#         print(f"The value of exxpected : {expected}")
#         expected["nodes"]["model.test.second_model"]["metadata"][
#             "schema"
#         ] = unique_schema
#         expected["nodes"]["model.test.second_model"]["metadata"][
#             "name"
#         ] = expected["nodes"]["model.test.second_model"]["metadata"]["name"].lower()

#         print(f"Inside the expected catalog function : the name to be expected : {expected['nodes']['model.test.model']}")
#         expected['nodes']['model.test.model']['metadata']['name'] = expected['nodes']['model.test.model']['metadata']['name'].lower()

#         expected['sources']['source.test.my_source.my_table']['metadata']['name'] = expected['sources']['source.test.my_source.my_table']['metadata']['name'].upper()
#         # print("Ayush mehrotra Testing : ",expected['nodes']['seed.test.SEED'])
#         # print("Ayush mehrotra Testing : ",expected['sources']['source.test.my_source.my_table']['metadata'])
#         # print(f"Ayush mehrotra Testing Before : {expected['nodes']['seed.test.seed']}")
#         seed_update = expected['nodes'].pop('seed.test.seed')
#         expected['nodes']['seed.test.SEED'] = {
#           'unique_id': 'seed.test.SEED',
#           'metadata': seed_update['metadata'],
#           'stats': seed_update['stats'],
#           'columns': seed_update['columns']
#         }
#         # print(f"Ayush mehrotra Testing After : {expected['nodes']['seed.test.SEED']}")

#         expected['nodes']['seed.test.SEED']['metadata']['name'] = expected['nodes']['seed.test.SEED']['metadata']['name'].upper()

#         # print(f"The value of expected['nodes']['source.test.my_source.my_table']['metadata']['name'] : {expected['nodes']['source.test.my_source.my_table']['metadata']['name']}")
#         # expected['nodes']['source.test.my_source.my_table']['metadata']['name'] = expected['nodes']['model.test.model']['metadata']['name'].upper()

#         # expected['nodes']['model.test.model']['metadata']['columns']
#     #     print(f"The value of ['nodes']['model.test.model']['columns'] : {expected['nodes']['model.test.model']['columns']}")
#     #     # print(f"After update Inside the expected catalog function : the name to be expected : {expected['nodes']['model.test.model']['metadata']['name']}")
#         d_keys=list(expected['nodes']['model.test.model']['columns'].keys())
#         # d_keys=list(l.keys())
#         for i in d_keys:
#             j=expected['nodes']['model.test.model']['columns'].pop(i)
#             # print(j)
#             expected['nodes']['model.test.model']['columns'][j['name'].upper()] = {
#             'name': j['name'].upper(),
#             'index': j['index'],
#             'type': j['type'],
#             'comment': j['comment']
#             }
#         return expected


class TestDocsGenReferencesNetezza(NetezzaGenerateProject, BaseDocsGenReferences):
    # Override to remove invalid 'order by' for view
    ref_models__view_summary_sql = """
    {{
    config(
        materialized = "view"
    )
    }}

    select first_name, ct from {{ref('ephemeral_summary')}}
    """

    ref_sources__schema_yml = """
version: 2
sources:
  - name: my_source
    description: "{{ doc('source_info') }}"
    loader: a_loader
    schema: "{{ var('test_schema') }}"
    tables:
      - name: my_table
        description: "{{ doc('table_info') }}"
        identifier: SEED
        columns:
          - name: id
            description: "{{ doc('column_info') }}"
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": ref_models__schema_yml,
            "sources.yml": self.ref_sources__schema_yml,
            "view_summary.sql": self.ref_models__view_summary_sql,
            "ephemeral_summary.sql": ref_models__ephemeral_summary_sql,
            "ephemeral_copy.sql": ref_models__ephemeral_copy_sql,
            "docs.md": ref_models__docs_md,
        }

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        expected = expected_references_catalog(
            project,
            role=profile_user.upper(),
            id_type="INTEGER",
            text_type=self.AnyCharacterVarying(),
            time_type="TIMESTAMP",
            bigint_type="BIGINT",
            view_type="VIEW",
            table_type="TABLE",
            model_stats=no_stats(),
            case=str.upper,
            case_columns=str.upper,
        )

        d_keys=list(expected['nodes']['seed.test.SEED']['columns'].keys())
        for i in d_keys:
            j=expected['nodes']['seed.test.SEED']['columns'].pop(i)
            expected['nodes']['seed.test.SEED']['columns'][j['name'].upper()] = {
            'name': j['name'].upper(),
            'index': j['index'],
            'type': j['type'],
            'comment': j['comment']
            }

        d_keys=list(expected['nodes']['model.test.ephemeral_summary']['columns'].keys())
        for i in d_keys:
            j=expected['nodes']['model.test.ephemeral_summary']['columns'].pop(i)
            expected['nodes']['model.test.ephemeral_summary']['columns'][j['name'].upper()] = {
            'name': j['name'].upper(),
            'index': j['index'],
            'type': j['type'],
            'comment': j['comment']
            }

        expected['nodes']['model.test.ephemeral_summary']['metadata']['name'] = expected['nodes']['model.test.ephemeral_summary']['metadata']['name'].lower()
        expected['nodes']['model.test.view_summary']['metadata']['name'] = expected['nodes']['model.test.view_summary']['metadata']['name'].lower()

        return expected

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
    #     yield
    #     with project.adapter.connection_named("__test"):
    #         alternate_schema = f"{project.test_schema}_test"
    #         relation = project.adapter.Relation.create(
    #             database=project.database, schema=alternate_schema
    #         )
    #         project.adapter.drop_schema(relation)
      pass


class TestValidateConnectionNetezza(BaseValidateConnection):
    pass
