"""
Tests for GRANT/REVOKE functionality

Requires the additions of three environment vars to test.env (username can be
any valid username in target db):

DBT_TEST_USER_1=<username>
DBT_TEST_USER_2=<username>
DBT_TEST_USER_3=<username>

NOTE: None of these test usernames should be the same as session_user when the
tests run, or the tests will find no privileges and fail
"""

from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants
from dbt.tests.util import (
    get_manifest,
    write_file,
)
from dbt.adapters.netezza.util import run_dbt, run_dbt_and_capture
from dbt.tests.adapter.grants.test_model_grants import user2_model_schema_yml, table_model_schema_yml, user2_table_model_schema_yml, multiple_users_table_model_schema_yml, multiple_privileges_table_model_schema_yml

from dbt.tests.adapter.grants.test_invalid_grants import invalid_user_table_model_schema_yml, invalid_privilege_table_model_schema_yml

my_incremental_model_sql = """
  select 1 as fun
"""

incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


# class TestInvalidGrantsNetezza(BaseInvalidGrants):
#     def grantee_does_not_exist_error(self):
#         return "ProcessObjectPrivileges: group/user"

#     def privilege_does_not_exist_error(self):
#         return r"expecting `ALL' or `ALTER' or `CREATE' or `DELETE' or `DROP'"

#     def test_invalid_grants(self, project, get_test_users, logs_dir):
#         # failure when grant to a user/role that doesn't exist
#         yaml_file = self.interpolate_name_overrides(invalid_user_table_model_schema_yml)
#         write_file(yaml_file, project.project_root, "models", "schema.yml")
#         (results, log_output) = run_dbt_and_capture(["--debug", "run"], expect_pass=False)

#         assert self.grantee_does_not_exist_error() in log_output

#         # failure when grant to a privilege that doesn't exist
#         yaml_file = self.interpolate_name_overrides(invalid_privilege_table_model_schema_yml)
#         write_file(yaml_file, project.project_root, "models", "schema.yml")
#         (results, log_output) = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
#         # print(f"The captured output is : {log_output} and len : {len(log_output)}")
#         # if self.grantee_does_not_exist_error() in log_output:
#         #     print("*"*70)
#         assert self.privilege_does_not_exist_error() in log_output



class TestModelGrantsNetezza(BaseModelGrants):
    def test_view_table_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        insert_privilege_name = self.privilege_grantee_name_overrides()["insert"]
        assert len(test_users) == 3

        # View materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        expected = {select_privilege_name: [test_users[0]]}
        assert model.config.grants == expected
        assert model.config.materialized == "view"
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # View materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1

        expected = {select_privilege_name: [get_test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: [test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, multiple grantees
        updated_yaml = self.interpolate_name_overrides(multiple_users_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: [test_users[0], test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, multiple privileges
        updated_yaml = self.interpolate_name_overrides(multiple_privileges_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: [test_users[0]], insert_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)


# class TestIncrementalGrantsNetezza(BaseIncrementalGrants):
#     # Override due to DROP SCHEMA statement, which is not supported by Netezza
#     def test_incremental_grants(self, project, get_test_users):
#         # we want the test to fail, not silently skip
#         test_users = get_test_users
#         select_privilege_name = self.privilege_grantee_name_overrides()["select"]
#         assert len(test_users) == 3

#         # Incremental materialization, single select grant
#         (results, log_output) = run_dbt_and_capture(["--debug", "run"])
#         assert len(results) == 1
#         manifest = get_manifest(project.project_root)
#         model_id = "model.test.my_incremental_model"
#         model = manifest.nodes[model_id]
#         assert model.config.materialized == "incremental"
#         expected = {select_privilege_name: [test_users[0]]}
#         self.assert_expected_grants_match_actual(
#             project, "my_incremental_model", expected
#         )

#         # Incremental materialization, run again without changes
#         (results, log_output) = run_dbt_and_capture(["--debug", "run"])
#         assert len(results) == 1
#         assert "revoke " not in log_output
#         assert (
#             "grant " not in log_output
#         )  # with space to disambiguate from 'show grants'
#         self.assert_expected_grants_match_actual(
#             project, "my_incremental_model", expected
#         )

#         # Incremental materialization, change select grant user
#         updated_yaml = self.interpolate_name_overrides(
#             user2_incremental_model_schema_yml
#         )
#         write_file(updated_yaml, project.project_root, "models", "schema.yml")
#         (results, log_output) = run_dbt_and_capture(["--debug", "run"])
#         assert len(results) == 1
#         assert "revoke " in log_output
#         manifest = get_manifest(project.project_root)
#         model = manifest.nodes[model_id]
#         assert model.config.materialized == "incremental"
#         expected = {select_privilege_name: [test_users[1]]}
#         self.assert_expected_grants_match_actual(
#             project, "my_incremental_model", expected
#         )

#         # Incremental materialization, same config, now with --full-refresh
#         run_dbt(["--debug", "run", "--full-refresh"])
#         assert len(results) == 1
#         # whether grants or revokes happened will vary by adapter
#         self.assert_expected_grants_match_actual(
#             project, "my_incremental_model", expected
#         )

#         # NOTE: The last component of the test assumes that state has
#         # changed due to a DROP SCHEMA statement; since Netezza does not
#         # support that statement and it therefore never runs, state is identical
#         # and subsequent test will not actually test anything


# class TestSeedGrantsNetezza(BaseSeedGrants):
#     pass


# class TestSnapshotGrantsNetezza(BaseSnapshotGrants):
#     pass
