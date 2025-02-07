import pytest
from pathlib import Path
import nzpy
from dbt.tests.util import check_relations_equal

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from dbt.adapters.netezza.util import run_dbt, run_dbt_and_capture
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.adapter.incremental.test_incremental_predicates import ResultHolder

from dbt.tests.adapter.incremental.test_incremental_unique_id import models__expected__one_str__overwrite_sql, models__expected__unique_key_list__inplace_overwrite_sql

# Overwrite to explicitly cast to string types in model
models__delete_insert_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

{% if not is_incremental() %}

select 1 as id, 'hello'::{{type_string()}} as msg, 'blue'::{{type_string()}} as color
union all
select 2 as id, 'goodbye'::{{type_string()}} as msg, 'red'::{{type_string()}} as color

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey'::{{type_string()}} as msg, 'blue'::{{type_string()}} as color
union all
select 2 as id, 'yo'::{{type_string()}} as msg, 'green'::{{type_string()}} as color
union all
select 3 as id, 'anyway'::{{type_string()}} as msg, 'purple'::{{type_string()}} as color

{% endif %}
"""


class TestIncrementalOnSchemaChangeNetezza(BaseIncrementalOnSchemaChange):
    def run_twice_and_assert(self, include, compare_source, compare_target, project):
        # dbt run (twice)
        run_args = ["run"]
        if include:
            run_args.extend(("--select", include))
        results_one = run_dbt(run_args)
        assert len(results_one) == 3

        results_two = run_dbt(run_args)
        assert len(results_two) == 3

        check_relations_equal(project.adapter, [compare_source, compare_target])

    def test_run_incremental_ignore(self, project):
        select = "model_a incremental_ignore incremental_ignore_target"
        compare_source = "incremental_ignore"
        compare_target = "incremental_ignore_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)


# # override to drop test models when tests complete, use new models__delete_insert_incremental_predicates_sql
# class BaseIncrementalPredicatesNetezza(BaseIncrementalPredicates):
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "delete_insert_incremental_predicates.sql": models__delete_insert_incremental_predicates_sql
#         }

#     def update_incremental_model(self, incremental_model):
#         """update incremental model after the seed table has been updated"""
#         model_result_set = run_dbt(["run", "--select", incremental_model])
#         return len(model_result_set)

#     def get_test_fields(
#         self, project, seed, incremental_model, update_sql_file, opt_model_count=None
#     ):
#         seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

#         model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
#         # pass on kwarg
#         relation = incremental_model
#         # update seed in anticipation of incremental model update
#         row_count_query = "select * from {}.{}".format(project.test_schema, seed)
#         # project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
#         seed_rows = len(project.run_sql(row_count_query, fetch="all"))

#         # propagate seed state to incremental model according to unique keys
#         inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

#         return ResultHolder(
#             seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
#         )

#     def test__incremental_predicates(self, project):
#         """seed should match model after two incremental runs"""

#         expected_fields = self.get_expected_fields(
#             relation="EXPECTED_DELETE_INSERT_INCREMENTAL_PREDICATES", seed_rows=4
#         )
#         test_case_fields = self.get_test_fields(
#             project,
#             seed="EXPECTED_DELETE_INSERT_INCREMENTAL_PREDICATES",
#             incremental_model="delete_insert_incremental_predicates",
#             update_sql_file=None,
#         )
#         self.check_scenario_correctness(expected_fields, test_case_fields, project)


# class TestIncrementalPredicatesDeleteInsertNetezza(BaseIncrementalPredicatesNetezza):
#     pass


# class TestIncrementalPredicatesMergeNetezza(BaseIncrementalPredicatesNetezza):
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "models": {
#                 "+incremental_predicates": ["DBT_INTERNAL_SOURCE.id != 2"],
#                 "+incremental_strategy": "merge",
#             }
#         }


# class TestPredicatesDeleteInsertNetezza(BaseIncrementalPredicatesNetezza):
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "models": {
#                 "+predicates": ["id != 2"],
#                 "+incremental_strategy": "delete+insert",
#             }
#         }


# class TestPredicatesMergeNetezza(BaseIncrementalPredicatesNetezza):
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "models": {
#                 "+predicates": ["DBT_INTERNAL_SOURCE.id != 2"],
#                 "+incremental_strategy": "merge",
#             }
#         }


class TestIncrementalUniqueKeyNetezza(BaseIncrementalUniqueKey):
    models__trinary_unique_key_list_sql = """
    -- a multi-argument unique key list should see overwriting on rows in the model
    --   where all unique key fields apply

    {{
        config(
            materialized='incremental',
            unique_key=['state', 'county', 'city']
        )
    }}

    select
        state as state,
        county as county,
        city as city,
        last_visit_date as last_visit_date
    from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__nontyped_trinary_unique_key_list_sql = """
    -- a multi-argument unique key list should see overwriting on rows in the model
    --   where all unique key fields apply
    --   N.B. needed for direct comparison with seed

    {{
        config(
            materialized='incremental',
            unique_key=['state', 'county', 'city']
        )
    }}

    select
        state as state,
        county as county,
        city as city,
        last_visit_date as last_visit_date
    from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__unary_unique_key_list_sql = """
    -- a one argument unique key list should result in overwritting semantics for
    --   that one matching field

    {{
        config(
            materialized='incremental',
            unique_key=['state']
        )
    }}

    select
        state as state,
        county as county,
        city as city,
        last_visit_date as last_visit_date
    from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__not_found_unique_key_sql = """
    -- a model with a unique key not found in the table itself will error out

    {{
        config(
            materialized='incremental',
            unique_key='thisisnotacolumn'
        )
    }}

    select * from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__empty_unique_key_list_sql = """
    -- model with empty list unique key should build normally

    {{
        config(
            materialized='incremental',
            unique_key=[]
        )
    }}

    select * from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__no_unique_key_sql = """
    -- no specified unique key should cause no special build behavior

    {{
        config(
            materialized='incremental'
        )
    }}

    select * from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__empty_str_unique_key_sql = """
    -- ensure model with empty string unique key should build normally

    {{
        config(
            materialized='incremental',
            unique_key=''
        )
    }}

    select * from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__str_unique_key_sql = """
    -- a unique key with a string should trigger to overwrite behavior when
    --   the source has entries in conflict (i.e. more than one row per unique key
    --   combination)

    {{
        config(
            materialized='incremental',
            unique_key='state'
        )
    }}

    select
        state as state,
        county as county,
        city as city,
        last_visit_date as last_visit_date
    from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__duplicated_unary_unique_key_list_sql = """
    {{
        config(
            materialized='incremental',
            unique_key=['state', 'state']
        )
    }}

    select
        state as state,
        county as county,
        city as city,
        last_visit_date as last_visit_date
    from {{ ref('SEED') }}

    {% if is_incremental() %}
        where last_visit_date > (select max(last_visit_date) from {{ this }})
    {% endif %}

    """

    models__not_found_unique_key_list_sql = """
    -- a unique key list with any element not in the model itself should error out

    {{
        config(
            materialized='incremental',
            unique_key=['state', 'thisisnotacolumn']
        )
    }}

    select * from {{ ref('SEED') }}

    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": self.models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": self.models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": self.models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": self.models__not_found_unique_key_sql,
            "empty_unique_key_list.sql":self.models__empty_unique_key_list_sql,
            "no_unique_key.sql": self.models__no_unique_key_sql,
            "empty_str_unique_key.sql": self.models__empty_str_unique_key_sql,
            "str_unique_key.sql": self.models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": self.models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": self.models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": models__expected__one_str__overwrite_sql,
                "unique_key_list__inplace_overwrite.sql": models__expected__unique_key_list__inplace_overwrite_sql,
            },
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        pass
        # yield
        # with project.adapter.connection_named("__test"):
        #     relation = project.adapter.Relation.create(
        #         database=project.database, schema=project.test_schema
        #     )
        #     project.adapter.drop_schema(relation)

    def get_test_fields(
        self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):
        """build a test case and return values for assertions
        [INFO] Models must be in place to test incremental model
        construction and merge behavior. Database touches are side
        effects to extract counts (which speak to health of unique keys)."""
        # idempotently create some number of seeds and incremental models'''
        print("Ye walal hua call !")
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        conn = nzpy.connect(user="admin", password="password",host='am9001.fyre.ibm.com', port=5480, database="dbttestdb", securityLevel=1,logLevel=0)

        with conn.cursor() as cursor:
            try:
                cursor.execute("select * from seed;")
                print(f"The results is : {cursor.fetchall()}")
                print("Table customerAddress created successfully")
            except Exception as e:
                print(str(e))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = "select * from {}.{}".format(project.test_schema, seed)
        project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
        )

    def update_incremental_model(self, incremental_model):
        """update incremental model after the seed table has been updated"""
        model_result_set = run_dbt(["run", "--select", incremental_model])
        return len(model_result_set)

    def fail_to_build_inc_missing_unique_key_column(self, incremental_model_name):
        """should pass back error state when trying build an incremental
        model whose unique key or keylist includes a column missing
        from the incremental model"""
        seed_count = len(run_dbt(["seed", "--select", "SEED", "--full-refresh"]))  # noqa:F841
        # unique keys are not applied on first run, so two are needed
        run_dbt(
            ["run", "--select", incremental_model_name, "--full-refresh"],
            expect_pass=True,
        )
        result, output = run_dbt_and_capture(
            ["run", "--select", incremental_model_name], expect_pass=False
        )
        # run_result = run_dbt(
        #     ["run", "--select", incremental_model_name], expect_pass=False
        # ).results[0]

        return result, output
    # # no unique_key test
    # def test__no_unique_keys(self, project):
    #     """with no unique keys, seed and model should match"""

    #     expected_fields = self.get_expected_fields(relation="SEED", seed_rows=8)
    #     test_case_fields = self.get_test_fields(
    #         project, seed="SEED", incremental_model="no_unique_key", update_sql_file="add_new_rows"
    #     )
    #     self.check_scenario_correctness(expected_fields, test_case_fields, project)

    # # unique_key as str tests
    # def test__empty_str_unique_key(self, project):
    #     """with empty string for unique key, seed and model should match"""

    #     expected_fields = self.get_expected_fields(relation="SEED", seed_rows=8)
    #     test_case_fields = self.get_test_fields(
    #         project,
    #         seed="SEED",
    #         incremental_model="empty_str_unique_key",
    #         update_sql_file="add_new_rows",
    #     )
    #     self.check_scenario_correctness(expected_fields, test_case_fields, project)

    # def test__one_unique_key(self, project):
    #     """with one unique key, model will overwrite existing row"""
    #     print("Trying to run the test__one_unique_key !!")
    #     expected_fields = self.get_expected_fields(
    #         relation="one_str__overwrite", seed_rows=7, opt_model_count=1
    #     )
    #     print(f"expected_fields : {expected_fields}")
    #     test_case_fields = self.get_test_fields(
    #         project,
    #         seed="SEED",
    #         incremental_model="str_unique_key",
    #         update_sql_file="duplicate_insert",
    #         opt_model_count=self.update_incremental_model("one_str__overwrite"),
    #     )
    #     print("The testcase fields are : ",test_case_fields)
    #     self.check_scenario_correctness(expected_fields, test_case_fields, project)

    # def test__bad_unique_key(self, project):
    #     """expect compilation error from unique key not being a column"""

    #     res, exc = self.fail_to_build_inc_missing_unique_key_column(
    #         incremental_model_name="not_found_unique_key"
    #     )
    #     print(f'the value of result recieved is : {res}')
    #     print(f'the value of exc recieved is : {exc}')

    #     # assert status == RunStatus.Error
    #     assert "thisisnotacolumn" in exc.lower()

    # # test unique_key as list
    # def test__empty_unique_key_list(self, project):
    #     """with no unique keys, seed and model should match"""

    #     expected_fields = self.get_expected_fields(relation="SEED", seed_rows=8)

    #     test_case_fields = self.get_test_fields(
    #         project,
    #         seed="SEED",
    #         incremental_model="empty_unique_key_list",
    #         update_sql_file="add_new_rows",
    #     )
    #     self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__unary_unique_key_list(self, project):
        """with one unique key, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="unique_key_list__inplace_overwrite", seed_rows=7, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="SEED",
            incremental_model="unary_unique_key_list",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("unique_key_list__inplace_overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)


    # def get_test_fields(
    #     self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    # ):
    #     """build a test case and return values for assertions
    #     [INFO] Models must be in place to test incremental model
    #     construction and merge behavior. Database touches are side
    #     effects to extract counts (which speak to health of unique keys)."""
    #     # idempotently create some number of seeds and incremental models'''

    #     seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

    #     model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
    #     # pass on kwarg
    #     relation = incremental_model
    #     # update seed in anticipation of incremental model update
    #     row_count_query = "select * from {}.{}".format(project.test_schema, seed)
    #     project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
    #     seed_rows = len(project.run_sql(row_count_query, fetch="all"))

    #     # propagate seed state to incremental model according to unique keys
    #     inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

    #     return ResultHolder(
    #         seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
    #     )
